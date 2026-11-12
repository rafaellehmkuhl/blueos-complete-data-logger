#!/usr/bin/env python3
"""Single File Logger.

Records MAVLink, BlueOS service state, kernel messages, BlueOS service logs and container logs into one MCAP file
per run. Every message is wrapped as:
  mono_ns                 CLOCK_MONOTONIC when the message arrived. Never jumps; resets on reboot.
  wall_ns                 CLOCK_REALTIME when the message arrived, i.e. what Linux believed the time was.
  corrected_ns            mono_ns + shift, the best UTC estimate (like ArduPilot's TimeUS + rtc_shift).
  time_synced_externally  whether that estimate comes from an external source (NTP or the autopilot's clock).
  time_source             ntp, autopilot or linux_unsynced.
The file on disk uses mono_ns + the shift at start as MCAP log_time, so it never jumps and never reorders. Downloading
it re-stamps every message with the best reference of the whole run, which also fixes what came before a sync.
"""

import asyncio
import ctypes
import errno
import http.client
import io
import json
import os
import re
import shutil
import signal
import time
import zipfile
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import BoundedSemaphore, RLock, Thread
from urllib.parse import parse_qs, unquote, urlparse

# Every source is on localhost, so retry often: the library's default backoff grows to 90 s, which after a BlueOS core
# restart would leave MAVLink unrecorded for up to a minute after it is back.
for _name, _value in (("INITIAL", "1"), ("MIN", "1"), ("MAX", "2")):
    os.environ.setdefault(f"WEBSOCKETS_BACKOFF_{_name}_DELAY", _value)

import websockets  # noqa: E402
from mcap.exceptions import EndOfFile  # noqa: E402
from mcap.reader import make_reader  # noqa: E402
from mcap.records import Channel, Message, Metadata  # noqa: E402
from mcap.stream_reader import StreamReader  # noqa: E402
from mcap.writer import IndexType, Writer  # noqa: E402

VERSION = os.environ.get("VERSION", "0.0.0")
HOST = os.environ.get("BLUEOS_HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "9650"))
INGEST_PORT = int(os.environ.get("INGEST_PORT", "9651"))
LOG_DIR = Path(os.environ.get("LOG_DIR", "/app/logs"))
SERVICE_LOGS = Path(os.environ.get("SERVICE_LOGS", "/var/logs/blueos/services"))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_MB", "1024")) * 2**20
MAX_DISK_USE = float(os.environ.get("MAX_DISK_USE_PERCENT", "95")) / 100  # stop before the system runs out of space
# What a power cut can lose: the chunk in memory is written and synced to the card at least this often.
FLUSH_S = 5
# MCAP channel ids are 16 bits. A client inventing topics (an id in the name) must never get near that: past this, new
# topics go to /logger/overflow, still recorded, instead of breaking the file.
MAX_CHANNELS = 10_000
# Per-message indexes cost ~12 KB of memory per chunk until the file closes, which over days of uptime is too much for
# a Pi. Readers find messages through the chunk index just the same.
INDEXES = IndexType.ALL & ~IndexType.MESSAGE

# SYSTEM_TIME arrives over a websocket, so each sample is late by its delivery delay. The shift is the least delayed
# sample of the last minute, re-recorded when it moves more than this.
AUTOPILOT_RESYNC_NS = 20_000_000
AUTOPILOT_STEP_NS = 1_000_000_000  # a forward jump this big is the autopilot's clock being set, not a faster delivery
# Same idea as ArduPilot's oldest_acceptable_date: anything earlier is an unset clock.
OLDEST_VALID_NS = 1_704_067_200 * 10**9  # 2024-01-01

# (path behind the BlueOS nginx, period in seconds, record every poll). When the last field is False the answer is
# only recorded when it changes, which is what keeps big, mostly static structures (bag of holding, v4l) cheap.
POLLS = [
    ("/system-information/system/cpu", 1, True),
    ("/system-information/system/memory", 1, True),
    ("/system-information/system/temperature", 1, True),
    ("/system-information/system/network", 1, True),
    ("/system-information/system/disk", 5, True),
    ("/kraken/v1.0/stats", 10, True),  # docker stats of every container takes ~2 s of work
    ("/system-information/system/process", 10, True),
    ("/system-information/platform", 5, False),
    ("/system-information/system/info", 30, False),
    ("/system-information/model", 60, False),
    ("/system-information/serial?udev=true", 10, False),
    ("/system-information/netstat", 10, False),
    ("/kraken/v1.0/installed_extensions", 10, False),
    ("/kraken/v1.0/list_containers", 10, False),
    ("/bag/v1.0/get/*", 30, False),
    ("/ardupilot-manager/v1.0/firmware_info", 30, False),
    ("/ardupilot-manager/v1.0/vehicle_type", 30, False),
    ("/ardupilot-manager/v1.0/firmware_vehicle_type", 30, False),
    ("/ardupilot-manager/v1.0/board", 10, False),
    ("/ardupilot-manager/v1.0/serials", 30, False),
    ("/ardupilot-manager/v1.0/endpoints/", 10, False),
    ("/ardupilot-manager/v1.0/preferred_router", 30, False),
    ("/beacon/v1.0/services", 30, False),
    ("/beacon/v1.0/ip", 10, False),
    ("/beacon/v1.0/hostname", 60, False),
    ("/beacon/v1.0/vehicle_name", 60, False),
    ("/bridget/v1.0/bridges", 10, False),
    ("/cable-guy/v1.0/ethernet", 10, False),
    ("/cable-guy/v1.0/interfaces", 10, False),
    ("/cable-guy/v1.0/host_dns", 60, False),
    ("/commander/v1.0/environment_variables", 60, False),
    ("/helper/latest/web_services", 30, False),
    ("/mavlink-camera-manager/streams", 5, False),
    ("/mavlink-camera-manager/info", 60, False),
    ("/mavlink-camera-manager/v4l", 30, False),
    ("/nmea-injector/v1.0/socks", 30, False),
    ("/ping/v1.0/sensors", 10, False),
    ("/version-chooser/v1.0/version/current/", 60, False),
    ("/version-chooser/v1.0/bootstrap/current", 60, False),
    ("/wifi-manager/v1.0/status", 5, False),
    ("/wifi-manager/v1.0/hotspot", 30, False),
]
DEFAULT_PERIODS = {path: period for path, period, _ in POLLS}
periods = dict(DEFAULT_PERIODS)  # what the user set, in seconds
MIN_PERIOD, MAX_PERIOD = 0.1, 3600
SETTINGS = LOG_DIR / "settings.json"
auto_record = True  # record from the moment the logger starts; off, it waits for Start on the page


def valid_period(value) -> bool:
    return type(value) in (int, float) and MIN_PERIOD <= value <= MAX_PERIOD


def load_settings() -> None:
    global auto_record
    try:
        saved = json.loads(SETTINGS.read_text())
    except (OSError, ValueError):
        return
    if not isinstance(saved, dict):
        return
    if isinstance(polls := saved.get("poll_periods"), dict):
        periods.update({path: v for path, v in polls.items() if path in DEFAULT_PERIODS and valid_period(v)})
    if isinstance(saved.get("auto_record"), bool):
        auto_record = saved["auto_record"]


def save_settings(poll_periods: dict, auto: bool) -> str | None:
    """Returns an error, if any. A power cut leaves the old settings or the new ones, never half."""
    changed = {path: v for path, v in poll_periods.items() if v != DEFAULT_PERIODS[path]}
    temporary = SETTINGS.with_suffix(".tmp")
    try:
        temporary.write_text(json.dumps({"auto_record": auto, "poll_periods": changed}, indent=2))
        temporary.replace(SETTINGS)
    except OSError as error:
        return f"could not save the settings: {error}"
    return None


def set_auto_record(value) -> str | None:
    """Only for the next start of the logger: what is recording now goes on, or stays stopped."""
    global auto_record
    if not isinstance(value, bool):
        return "auto_record must be true or false"
    if error := save_settings(periods, value):
        return error
    auto_record = value
    rec.event(f"automatic recording {'enabled' if value else 'disabled'} for the next starts")
    return None


def apply_periods(new) -> str | None:
    """Sets poll periods and starts a new file, so every file has one set of rates. Returns an error, if any."""
    if not isinstance(new, dict) or not new:
        return "expected an object of poll path to period in seconds"
    for path, value in new.items():
        if path not in DEFAULT_PERIODS:
            return f"unknown poll {path!r}"
        if not valid_period(value):
            return f"the period of {path} must be a number of seconds from {MIN_PERIOD} to {MAX_PERIOD}"
    merged = {**periods, **new}
    if error := save_settings(merged, auto_record):  # nothing changes, so the current file's poll_periods_s stays true
        return error
    periods.update(new)
    changed = {path: v for path, v in merged.items() if v != DEFAULT_PERIODS[path]}
    rec.event(f"poll periods changed: {json.dumps(changed)}")
    if rec.stopped:  # the next file, when recording starts again, uses them
        return None
    try:
        rec.rotate()
    except Exception as error:
        return f"saved, but could not start a new file: {error}"
    return None

# Logs end up shared with other people, so never write anything that looks like a credential.
SECRET_KEY = re.compile(r"pass|secret|token|credential|api_?key|private_?key", re.IGNORECASE)


def redact(value):
    if isinstance(value, dict):
        return {k: "<redacted>" if SECRET_KEY.search(str(k)) else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


BOOTTIME = getattr(time, "CLOCK_BOOTTIME", time.CLOCK_MONOTONIC)


def linux_shift() -> int:
    return time.time_ns() - time.monotonic_ns()


_adjtimex = getattr(ctypes.CDLL(None), "adjtimex", None)


def ntp_synced() -> bool | None:
    """What `timedatectl` reports as "System clock synchronized"; None where the kernel can't say."""
    if _adjtimex is None:
        return None
    timex = ctypes.create_string_buffer(512)  # zeroed, so modes = 0: read only, no privilege needed
    state = _adjtimex(timex)
    return None if state == -1 else state != 5  # 5 is TIME_ERROR, set while the clock is unsynchronized


def iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).isoformat(timespec="milliseconds")


# Time references, best first. A lower one never replaces a higher one, like ArduPilot's RTC source priority.
RANK = {"ntp": 2, "autopilot": 1, "linux_unsynced": 0}
NTP_DRIFT_NS = 10_000_000  # NTP slews Linux a little all the time; only re-record its reference past this


class Clock:
    """Maps the monotonic clock to UTC: corrected = mono + shift, like ArduPilot's rtc_shift."""

    def __init__(self):
        self.start_shift = self.shift = self.linux = linux_shift()
        self.source = "ntp" if ntp_synced() else "linux_unsynced"
        self.syncs = 0

    @property
    def synced(self) -> bool:
        return self.source != "linux_unsynced"

    def reference(self) -> dict:
        return {"source": self.source, "synced": self.synced, "shift_ns": self.shift}


class Recorder:
    """The one place that writes to the file. It never raises: a failure is shown on the page and recorded, and
    recording goes on as soon as the disk lets it."""

    def __init__(self, clock: Clock, record: bool = True):
        self.clock = clock
        self.generation = 0  # bumped with every new file and resume, so sources that write on change write again
        # Reentrant, because start() writes. The container log threads write directly, so a backlog can't starve the
        # event loop, and every writer call happens under it.
        self.lock = RLock()
        self.dropped = 0  # messages not recorded: while paused for disk space or after a write error
        self.paused = False
        self.closed = False  # the file is finished and nothing is written: the logger is stopping, or stopped
        self.stopped = False  # stopped from the page, until started again from there
        self.write_error: str | None = None  # the writer failed: nothing is recorded until a new file opens
        self.errors: dict[str, str] = {}  # what is failing, shown on the page
        self.links: dict[str, str] = {}
        self.seen: dict[str, float] = {}  # monotonic time of the last message from each websocket
        if not record:  # automatic recording is off: stopped until Start, and no empty file meanwhile
            self.path, self.file, self.writer = LOG_DIR / "none", None, None
            self.closed = self.stopped = True
            self.dropped_at_stop = 0
            self._reset()
            return
        try:
            self.path, self.file, self.writer = self._new_file()
        except Exception as error:  # a read-only card, say: the page says so, and watch_disk keeps trying
            self.path, self.file, self.writer = LOG_DIR / "none", None, None
            self._failed(error)
        self._reset()

    def _new_file(self):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        # The sequence number keeps files in recording order even when the clock that named them was wrong.
        number = max((int(p.name[:4]) for p in LOG_DIR.glob("[0-9][0-9][0-9][0-9]_*.mcap")), default=0) + 1
        stamp = time.strftime("%Y-%m-%dT%H-%M-%SZ", time.gmtime())
        path = LOG_DIR / f"{number:04d}_{stamp}.mcap"
        file = open(path, "xb")  # never overwrite a recording
        try:
            writer = Writer(file, index_types=INDEXES)
            writer.start(library=f"blueos-complete-data-logger/{VERSION}")
            file.flush()  # the header on the card right away, so the new file reads as MCAP from its first second
        except BaseException:
            file.close()
            with suppress(OSError):  # no empty file per retry while the card refuses writes
                path.unlink()
            raise
        return path, file, writer

    def _reset(self) -> None:
        self.errors.pop("topics", None)
        self.channels: dict[str, int] = {}
        self.messages = 0
        # MCAP readers sort by log_time, so within a file it must never jump: mono_ns placed at the best UTC estimate
        # of the moment the file started.
        self.start_shift = self.clock.shift
        self.flushed = time.monotonic()
        self.generation += 1

    def start(self) -> None:
        """Opens the file with what a reader needs to place it in time on its own."""
        with self.lock:
            self.start_mono = time.monotonic_ns()
            self._metadata("recording", {
                "extension_version": VERSION,
                "boot_id": boot_id() or "unknown",
                "start_mono_ns": str(self.start_mono),
                "start_wall_ns": str(time.time_ns()),
            })
            self._metadata("poll_periods_s", {path: str(period) for path, period in periods.items()})
            # The reference in use, so a file started after the sync is synchronized from its first message.
            self.write_reference(file_start=True)

    def write_reference(self, **extra) -> None:
        self.write("/time/sync", {**self.clock.reference(), "previous_shift_ns": self.clock.shift, "step_ns": 0,
                                  "boottime_ns": time.clock_gettime_ns(BOOTTIME), "linux_ntp_synced": ntp_synced(),
                                  **extra})

    def rotate(self) -> Path:
        """Closes the current file and starts the next one, without losing or misplacing a message. Returns the
        finished file. Raises, and keeps recording into the current file, if the next one can't be created."""
        with self.lock:
            if self.closed:
                raise RuntimeError("recording is stopped" if self.stopped else "the logger is stopping")
            if self.write_error:
                raise RuntimeError(f"recording is stopped by a write error: {self.write_error}")
            new = self._new_file()  # first, so a failure leaves the current file recording
            finished = self.path
            self.event("closing this file, recording continues in the next one")
            if self.write_error:  # that write failed: recovery takes over, the new file is not needed
                new[1].close()
                with suppress(OSError):
                    new[0].unlink()
                raise RuntimeError(f"recording is stopped by a write error: {self.write_error}")
            self._close()
            self.path, self.file, self.writer = new
            self._reset()
            self.start()
            self.event(f"started after {finished.name}")
        return finished

    def recover(self) -> bool:
        """After a write error, tries a new file. Returns whether recording goes on."""
        with self.lock:
            if not self.write_error or self.closed:
                return not self.write_error
            try:
                new = self._new_file()
            except Exception as error:
                self.errors["recording"] = f"stopped by {self.write_error}; a new file can't be created either: " \
                                           f"{type(error).__name__}: {error}"
                return False
            failure, lost = self.write_error, self.dropped - self.dropped_at_failure
            self.write_error = None
            self.errors.pop("recording", None)
            self.path, self.file, self.writer = new
            self._reset()
            self.start()
            self.event(f"recording resumed in a new file after a write error ({failure}). Not recorded: {lost} "
                       f"messages, plus the last {FLUSH_S} s before the error")
        return True

    def stop(self) -> None:
        """The emergency stop on the page: the file is finished whole and nothing more is written, not even to a new
        file, until restart(). Not kept across restarts of the logger: those follow auto_record."""
        with self.lock:
            if self.closed:
                raise RuntimeError("not recording")
            self.event("recording stopped from the page")
            self.finish()
            self.stopped, self.dropped_at_stop = True, self.dropped

    def restart(self) -> None:
        """Recording again after stop(), in a new file. Raises, and stays stopped, if the file can't be created."""
        with self.lock:
            if not self.stopped:
                raise RuntimeError("already recording")
            self.path, self.file, self.writer = self._new_file()
            self.closed = self.stopped = False
            self.write_error = None  # any failure was in the finished file
            self.errors.pop("recording", None)
            self._reset()
            self.start()
            self.event(f"recording started from the page, after {self.dropped - self.dropped_at_stop} messages "
                       f"not recorded while stopped")

    def resume(self) -> None:
        """Recording again after a pause for disk space: what is only written on change is written again, so the file
        never shows a state from before the pause as current."""
        with self.lock:
            self.paused = False
            self.generation += 1
            self.event(f"recording resumed after dropping {self.dropped} messages")
            self.write_reference(resumed=True)

    def write(self, topic: str, data, mono: int | None = None) -> None:
        with self.lock:
            self._write(topic, data, mono)

    def _write(self, topic: str, data, mono: int | None) -> None:
        """Records `data` as happening now, or at `mono` (our CLOCK_MONOTONIC) when the sender stamped it."""
        if self.paused or self.closed or self.write_error:
            self.dropped += 1
            return
        now, wall = time.monotonic_ns(), time.time_ns()
        extra = {}
        if mono is None:
            mono = now
        else:
            wall -= now - mono  # what Linux believed when it happened
            extra["received_mono_ns"] = now
        if topic not in self.channels and len(self.channels) >= MAX_CHANNELS:
            self.errors["topics"] = f"over {MAX_CHANNELS} topics in this file: new ones go to /logger/overflow"
            data, topic = {"topic": topic, "data": data}, "/logger/overflow"
        envelope = {"mono_ns": mono, "wall_ns": wall, "corrected_ns": mono + self.clock.shift,
                    "time_synced_externally": self.clock.synced, "time_source": self.clock.source, **extra,
                    "data": data}
        try:
            payload = json.dumps(envelope, separators=(",", ":"), allow_nan=False).encode()
        except ValueError:  # NaN or Infinity: no JSON reader accepts them, so they are written as null
            envelope = json.loads(json.dumps(envelope), parse_constant=lambda _: None)
            payload = json.dumps(envelope, separators=(",", ":")).encode()
        try:
            if topic not in self.channels:
                self.channels[topic] = self.writer.register_channel(topic, message_encoding="json", schema_id=0)
            self.writer.add_message(self.channels[topic], log_time=mono + self.start_shift, data=payload,
                                    publish_time=wall)
        except Exception as error:
            self._failed(error)
            return
        self.messages += 1

    def _metadata(self, name: str, data: dict) -> None:
        if self.write_error:
            return
        try:
            self.writer.add_metadata(name, data)
        except Exception as error:
            self._failed(error)

    def _failed(self, error: Exception) -> None:
        """The card returned an error (EIO, a read-only remount...). The writer may hold half a record, so it is never
        used again: recording stops, visibly, until watch_disk gets a new file open."""
        self.write_error = f"{type(error).__name__}: {error}"
        self.errors["recording"] = f"stopped by a write error: {self.write_error}"
        self.dropped_at_failure = self.dropped
        print(f"recording stopped by a write error: {error!r}", flush=True)
        with suppress(Exception):
            self.file.close()

    def flush(self) -> int | None:
        """Writes the chunk in memory out. Returns the file descriptor to fsync, outside the lock: on a slow card
        that can take a while, and the event loop writes under this lock."""
        with self.lock:
            if self.write_error or self.file is None or self.file.closed:
                return None
            try:
                self.writer.flush()
            except Exception as error:
                self._failed(error)
                return None
            self.flushed = time.monotonic()
            return self.file.fileno()

    def event(self, text: str) -> None:
        print(text, flush=True)
        self.write("/logger/events", {"event": text})

    def sync(self, source: str, shift: int, **extra) -> bool:
        """Moves the reference to `source` unless a better one is already in use. Returns whether it moved."""
        with self.lock:
            if RANK[source] < RANK[self.clock.source]:
                return False
            previous = self.clock.shift
            self.clock.shift, self.clock.source = shift, source
            self.clock.syncs += 1
            self.write("/time/sync", {**self.clock.reference(), "previous_shift_ns": previous,
                                      "step_ns": shift - previous, "boottime_ns": time.clock_gettime_ns(BOOTTIME),
                                      "linux_ntp_synced": ntp_synced(), **extra})
        print(f"time reference {source}: corrected time is now {iso(time.monotonic_ns() + shift)}", flush=True)
        return True

    def finish(self) -> None:
        with self.lock:
            if self.closed:  # already finished by stop()
                return
            self.closed = True  # threads still running get dropped from here on
            self._close()

    def _close(self) -> None:
        """Ends the file with its index and the reference export() applies to it: corrected = mono_ns + shift_ns."""
        try:
            if not self.write_error:
                self.writer.add_metadata("time", {k: str(v) for k, v in self.clock.reference().items()})
                self.writer.finish()
                self.file.flush()
                os.fsync(self.file.fileno())
        except Exception as error:  # the file stays readable up to its last chunk
            print(f"could not finish {self.path.name}: {error!r}", flush=True)
            if not self.write_error:
                self._failed(error)
        finally:
            with suppress(Exception):
                self.file.close()


clock = Clock()
rec: Recorder  # created in main(), after the event loop exists


def fetch(path: str) -> bytes:
    with urllib.request.urlopen(f"http://{HOST}{path}", timeout=15) as response:
        return response.read()


def get_json(path: str):
    body = fetch(path)
    return json.loads(body) if body else None


async def poll(path: str, always: bool) -> None:
    topic = "/blueos" + path.split("?")[0].rstrip("/*")
    last, generation = None, rec.generation
    while True:
        started = time.monotonic()
        if generation != rec.generation:  # a new file, or a resume: write the current state even if it didn't change
            last, generation = None, rec.generation
        try:
            body = await asyncio.to_thread(fetch, path)
            # Compared as bytes, so an unchanged answer (the bag of holding is ~600 KB) costs no parsing at all.
            if body != last or always:
                rec.write(topic, redact(json.loads(body)) if body else None)
            last = body
            rec.errors.pop(path, None)
        except Exception as error:  # a service being down is data too
            message = f"{type(error).__name__}: {error}"
            rec.errors[path] = message
            if message != last:
                rec.write(topic, {"error": message})
            last = message
        # Wait out the period, but a new file (which is also how new periods are applied) cuts the wait short.
        while time.monotonic() - started < periods[path] and generation == rec.generation:
            await asyncio.sleep(min(0.5, max(0.0, periods[path] - (time.monotonic() - started))))


autopilot_shifts: deque[int] = deque(maxlen=60)
autopilot_system: int | None = None


def autopilot_time(message: dict, system: int) -> None:
    global autopilot_system
    unix_ns = message.get("time_unix_usec", 0) * 1000
    if unix_ns < OLDEST_VALID_NS:  # the autopilot has no time source yet
        return
    if autopilot_system is None:
        autopilot_system = system
    elif system != autopilot_system:  # another vehicle or a SITL on the same network: one clock only
        if f"SYSTEM_TIME {system}" not in rec.errors:
            rec.errors[f"SYSTEM_TIME {system}"] = f"ignored: the time reference follows system {autopilot_system}"
            rec.event(f"SYSTEM_TIME from system {system} ignored, the time reference follows system {autopilot_system}")
        return
    # ArduPilot never moves its clock backwards, so after a bad SYSTEM_TIME from a topside it can stay wrong while NTP
    # has already fixed Linux. That's why it ranks below NTP (see RANK).
    shift = unix_ns - time.monotonic_ns()
    if autopilot_shifts and shift > max(autopilot_shifts) + AUTOPILOT_STEP_NS:
        autopilot_shifts.clear()  # the autopilot's clock was set: what came before no longer applies
    autopilot_shifts.append(shift)
    # Every delivery delay makes a sample's shift smaller, so the largest is the least delayed: the best estimate.
    best = max(autopilot_shifts)
    if clock.source != "autopilot" or abs(best - clock.shift) > AUTOPILOT_RESYNC_NS:
        rec.sync("autopilot", best, time_unix_usec=message["time_unix_usec"], time_boot_ms=message.get("time_boot_ms"),
                 system_id=system)


async def websocket(name: str, path: str, handle) -> None:
    # Iterating connect() reconnects with backoff when the service restarts or nginx answers 5xx.
    async for ws in websockets.connect(f"ws://{HOST}{path}", max_size=None, open_timeout=10):
        rec.links[name] = "connected"
        rec.seen[name] = time.monotonic()
        rec.errors.pop(name, None)
        rec.event(f"{name} connected")
        bad = 0
        try:
            async for raw in ws:
                rec.seen[name] = time.monotonic()
                try:
                    handle(json.loads(raw))
                except (ValueError, KeyError, TypeError, IndexError, AttributeError) as error:
                    # One unexpected message must not cost the connection: it is kept as it came.
                    bad += 1
                    rec.errors[name] = f"{bad} unexpected messages, kept in /logger/unparsed/{name}: {error!r}"
                    rec.write(f"/logger/unparsed/{name}", {"error": repr(error), "raw": str(raw)[:2**16]})
        except websockets.ConnectionClosed:
            pass
        finally:
            rec.links[name] = "reconnecting"
        rec.event(f"{name} disconnected")


mavlink_sequence: dict[int, int] = {}
# Messages an autopilot numbered that never reached us. Only autopilots (component 1): other components, like the camera
# manager's 100 to 102, share one counter between them or repeat it, which would read as losses that aren't.
mavlink_gaps = 0


def handle_mavlink(packet: dict) -> None:
    global mavlink_gaps
    header, message = packet["header"], packet["message"]
    sender = (header["system_id"], header["component_id"])
    rec.write(f"/mavlink/{sender[0]}/{sender[1]}/{message['type']}", packet)
    sequence = header.get("sequence")
    if sender[1] == 1 and isinstance(sequence, int):
        gap = (sequence - mavlink_sequence.get(sender[0], sequence - 1) - 1) % 256
        if gap < 128:  # a bigger jump is a reordering or a restarted sender, not a loss
            mavlink_gaps += gap
        mavlink_sequence[sender[0]] = sequence
    if message["type"] == "SYSTEM_TIME" and header["component_id"] == 1:
        autopilot_time(message, header["system_id"])


last_kernel_sequence = -1


def handle_kernel(messages: list) -> None:
    # The service sends the whole ring buffer on every connection, so skip what was already recorded.
    global last_kernel_sequence
    for message in messages:
        if message["sequence_number"] > last_kernel_sequence:
            last_kernel_sequence = message["sequence_number"]
            rec.write("/linux/kernel", message)


def on_linux_clock_set() -> None:
    # The record nothing on BlueOS keeps today: old time, new time and monotonic time of every step.
    shift = linux_shift()
    previous, clock.linux = clock.linux, shift
    synced = ntp_synced()
    rec.write("/time/linux_clock_set", {"shift_ns": shift, "previous_shift_ns": previous, "step_ns": shift - previous,
                                        "boottime_ns": time.clock_gettime_ns(BOOTTIME), "linux_ntp_synced": synced})
    rec.sync("ntp" if synced else "linux_unsynced", shift)


async def watch_linux_clock() -> None:
    """Records every set of the Linux clock, of any size, from any caller. Slewing is not a set and is not recorded."""
    if not hasattr(os, "timerfd_create"):
        rec.event("clock step detection needs Linux and Python 3.13, not recording Linux clock steps")
        return
    loop = asyncio.get_running_loop()
    fd = os.timerfd_create(time.CLOCK_REALTIME, flags=os.TFD_NONBLOCK | os.TFD_CLOEXEC)

    def arm() -> None:
        # The kernel cancels this timer whenever CLOCK_REALTIME is set. A year ahead keeps it inside 32-bit time_t.
        os.timerfd_settime_ns(fd, flags=os.TFD_TIMER_ABSTIME | os.TFD_TIMER_CANCEL_ON_SET,
                              initial=time.time_ns() + 365 * 86400 * 10**9)

    def on_ready() -> None:
        try:
            os.read(fd, 8)  # only returns if a whole year went by without a clock set
        except OSError as error:
            if error.errno == errno.ECANCELED:
                on_linux_clock_set()
            elif error.errno != errno.EAGAIN:
                raise
        arm()

    arm()
    loop.add_reader(fd, on_ready)
    try:
        synced, generation = None, rec.generation
        while True:
            if generation != rec.generation:
                synced, generation = None, rec.generation
            if (now := ntp_synced()) != synced:
                synced = now
                rec.write("/time/linux_ntp", {"synced": synced})
            # NTP can reach sync by slewing alone, with no clock set to notice it.
            if synced and (clock.source != "ntp" or abs(linux_shift() - clock.shift) > NTP_DRIFT_NS):
                rec.sync("ntp", linux_shift())
            await asyncio.sleep(1)
    finally:
        loop.remove_reader(fd)
        os.close(fd)


def read_new_lines(path: Path, state: list) -> None:
    """state is [offset read up to, partial last line, size when first seen]: what was there before is backfill."""
    offset, partial, backfill_until = state
    if path.stat().st_size < offset:  # truncated in place
        offset, backfill_until = 0, 0
    with path.open("rb") as file:
        file.seek(offset)
        chunk = file.read(4 * 2**20)  # big files take a few passes, so the event loop never waits on one
    position = offset - len(partial)
    lines = (partial + chunk).split(b"\n")
    state[:] = [offset + len(chunk), lines.pop(), backfill_until]
    for number, line in enumerate(lines):
        if line.strip():
            rec.write(f"/blueos/logs/{path.parent.name}",
                      {"line": line.decode(errors="replace"), "file": path.name, "backfill": position < backfill_until})
        position += len(line) + 1
        if number % 200 == 199:
            yield_to_event_loop()


COMPRESSED = {".gz", ".zip", ".xz", ".bz2", ".zst", ".lz4"}


async def tail_service_logs() -> None:
    # Every file is followed, not just the newest: log files are named and dated by the wall clock, so after a
    # backward step the file being written can look older than the one before it.
    boot_wall = time.time() - time.clock_gettime(BOOTTIME)
    tails: dict[Path, list] = {}  # file -> read_new_lines state
    first_scan = True

    def scan() -> None:  # in a thread: the backlog since boot can be hundreds of MB, too much for the event loop
        if not SERVICE_LOGS.is_dir():
            rec.errors["service logs"] = f"{SERVICE_LOGS} is not there: BlueOS service logs are not recorded"
            return
        rec.errors.pop("service logs", None)
        seen = set()
        for path in SERVICE_LOGS.glob("*/*"):
            try:
                # Rotated logs get compressed; their lines were already read as text, or are from before.
                if path.suffix in COMPRESSED or not path.is_file():
                    continue
                seen.add(path)
                if path not in tails:
                    # At start, read whole only what was written since boot, so the boot sequence is in the log.
                    # Files that show up later are new rotations, read whole.
                    size = path.stat().st_size
                    whole = not first_scan or path.stat().st_mtime > boot_wall
                    tails[path] = [0 if whole else size, b"", size if first_scan else 0]
                read_new_lines(path, tails[path])
            except OSError:  # zipped away mid-scan
                continue
        for gone in tails.keys() - seen:
            del tails[gone]

    while True:
        await asyncio.to_thread(scan)
        first_scan = False
        await asyncio.sleep(1)


def yield_to_event_loop() -> None:
    """For threads writing a backlog: a moment without the GIL or the recorder's lock, so MAVLink isn't held up."""
    time.sleep(0.001)


def parse_docker_line(line: str) -> tuple[str, int | None]:
    """Splits Docker's `timestamps=1` prefix off a log line. Returns the text and Docker's time in Unix ns."""
    stamp, _, text = line.partition(" ")
    try:
        seconds, _, fraction = stamp.rstrip("Z").partition(".")
        whole = datetime.fromisoformat(seconds + "+00:00").timestamp()
        return text, int(whole) * 10**9 + int(fraction.ljust(9, "0")[:9] or 0)
    except ValueError:
        return line, None


def follow_container(container: dict, since_ns: int, backfill_until_ns: int, last_seen: dict) -> None:
    """Runs in its own thread until the container stops: a log stream can stay silent for hours. A failure shows on
    the page, and follow_container_logs starts it again."""
    name = container["Names"][0].lstrip("/")
    try:
        stream_container(container, name, since_ns, backfill_until_ns, last_seen)
    except http.client.IncompleteRead:
        # BlueOS's proxy cuts a log stream now and then, in the middle of a line. Not a failure: the stream is followed
        # again from the last whole line recorded, so the cut line comes again, whole.
        pass
    except Exception as error:
        rec.errors[f"docker logs {name}"] = f"{type(error).__name__}: {error}"


def stream_container(container: dict, name: str, since_ns: int, backfill_until_ns: int, last_seen: dict) -> None:
    base = f"http://{HOST}/docker/containers/{container['Id']}"
    tty = get_json(f"/docker/containers/{container['Id']}/json")["Config"]["Tty"]
    since = f"{since_ns // 10**9}.{since_ns % 10**9:09d}"
    url = f"{base}/logs?follow=1&stdout=1&stderr=1&timestamps=1&since={since}"
    with urllib.request.urlopen(url, timeout=None) as response:
        rec.errors.pop(f"docker logs {name}", None)
        lines = 0
        while True:
            if tty:  # one raw stream
                stream, payload = "stdout", response.readline()
            else:  # 8-byte frame headers: stream type, then payload size
                header = response.read(8)
                if len(header) < 8:
                    return
                stream = "stderr" if header[0] == 2 else "stdout"
                payload = response.read(int.from_bytes(header[4:], "big"))
            if not payload:
                return
            for line in payload.decode(errors="replace").splitlines():
                lines += 1
                if lines % 200 == 0:
                    yield_to_event_loop()
                text, docker_ns = parse_docker_line(line)
                if docker_ns:
                    last_seen[name] = docker_ns
                data = {"line": text, "stream": stream, "docker_time_ns": docker_ns,
                        "backfill": docker_ns is not None and docker_ns < backfill_until_ns}
                rec.write(f"/docker/logs/{name}", data)


async def follow_container_logs() -> None:
    """Logs of every container, BlueOS core and all extensions, through BlueOS's read-only Docker proxy."""
    started_ns = time.time_ns()
    # Docker filters by its own wall-clock stamps. At start that means "since boot"; after a container restarts,
    # "after the last line we got". ponytail: a clock stepped back while a container restarts can still hide lines.
    boot_ns = started_ns - time.clock_gettime_ns(BOOTTIME)
    last_seen: dict[str, int] = {}
    threads: dict[str, Thread] = {}
    while True:
        try:
            containers = await asyncio.to_thread(get_json, "/docker/containers/json")
            rec.errors.pop("/docker/containers/json", None)
            listed = {f"docker logs {c['Names'][0].lstrip('/')}" for c in containers}
            for gone in [k for k in rec.errors if k.startswith("docker logs ") and k not in listed]:
                del rec.errors[gone]  # removed, an extension uninstalled say: nothing left to follow
        except Exception as error:
            rec.errors["/docker/containers/json"] = f"{type(error).__name__}: {error}"
            containers = []
        for container in containers:
            name = container["Names"][0].lstrip("/")
            if name not in threads or not threads[name].is_alive():
                since = last_seen[name] + 1 if name in last_seen else boot_ns
                threads[name] = Thread(target=follow_container, daemon=True,
                                       args=(container, since, started_ns, last_seen))
                threads[name].start()
        await asyncio.sleep(10)


class _Counting:
    """mcap's Writer asks for tell(), which an HTTP response can't answer."""

    def __init__(self, out):
        self.out, self.position = out, 0

    def write(self, data: bytes) -> int:
        self.out.write(data)
        self.position += len(data)
        return len(data)

    def tell(self) -> int:
        return self.position


def chunk_records(data: bytes) -> list:
    """The records of one chunk, read on its own. Raises if it is damaged (its CRC is checked)."""
    found = []
    with suppress(EndOfFile):  # the end of the chunk
        for record in StreamReader(io.BytesIO(data), skip_magic=True, validate_crcs=True).records:
            found.append(record)
    return found


def records(path: Path, problems: list):
    """Every record of a recording, including one still being written or cut short by a power loss. What could not
    be read is added to `problems`."""
    with path.open("rb") as file:
        try:
            reader = make_reader(file)
            summary = reader.get_summary()
            metadata = list(reader.iter_metadata())
        except Exception:  # unfinished: no index
            summary = None
        if summary is not None and summary.chunk_indexes:
            # A finished file is read chunk by chunk, so a damaged chunk costs only its own messages.
            yield from summary.channels.values()
            yield from metadata
            for index in summary.chunk_indexes:
                file.seek(index.chunk_start_offset)
                try:
                    found = chunk_records(file.read(index.chunk_length))
                except Exception as error:
                    problems.append(f"damaged chunk at byte {index.chunk_start_offset}, skipped: {error!r}")
                    continue
                yield from found
            return
        file.seek(0)
        whole = 0  # where the last complete record ends
        try:
            for record in StreamReader(file).records:
                whole = file.tell()
                yield record
        except Exception as error:  # the end of an unfinished file, or damage
            size = path.stat().st_size
            # The file being written can end mid-chunk between two flushes: that part comes in the next download.
            if whole < size and path != getattr(rec, "path", None):
                problems.append(f"the last {size - whole} bytes, from byte {whole}, could not be read "
                                f"(cut by a power loss, or damaged): {error!r}")


def file_metadata(path: Path) -> dict:
    """The metadata records of a recording, finished or not."""
    try:
        with path.open("rb") as file:
            return {m.name: m.metadata for m in make_reader(file).iter_metadata()}
    except Exception:
        return {r.name: r.metadata for r in records(path, []) if isinstance(r, Metadata)}


def boot_id() -> str | None:
    with suppress(OSError):
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return None


spans: dict[Path, tuple] = {}  # path -> ((size, mtime), span): a finished file never changes, so it is read once
# One file at a time, in the background: a big file without an index takes minutes to read on a Pi, and the page asks
# for the list every second.
span_reader = ThreadPoolExecutor(1, thread_name_prefix="spans")
being_read: set[Path] = set()


def file_span(path: Path) -> dict:
    """What a recording says about its own time: its boot, when it started on mono_ns, how long it lasted, and the
    best reference it reached. Reads the whole file only when its summary doesn't say."""
    stat = path.stat()
    key = (stat.st_size, stat.st_mtime_ns)
    if (cached := spans.get(path)) and cached[0] == key:
        return cached[1]
    started, best, first, last = {}, None, None, None
    with suppress(Exception), path.open("rb") as file:  # a finished file: its summary has the span and metadata
        reader = make_reader(file)
        statistics = reader.get_summary().statistics
        first, last = statistics.message_start_time, statistics.message_end_time
        metadata = {m.name: m.metadata for m in reader.iter_metadata()}
        started = metadata.get("recording", {})
        if "time" in metadata:
            m = metadata["time"]
            best = {"source": m["source"], "synced": m["synced"] == "True", "shift_ns": int(m["shift_ns"])}
    if first is None or best is None:  # unfinished, or from a version that didn't write the reference: read it all
        channels, first, last = {}, None, None
        for record in records(path, []):
            if isinstance(record, Metadata) and record.name == "recording":
                started = record.metadata
            elif isinstance(record, Channel):
                channels[record.id] = record.topic
            elif isinstance(record, Message):
                first, last = min(first or record.log_time, record.log_time), max(last or 0, record.log_time)
                # The reference only ever improves, so the last /time/sync record is the best one.
                if channels.get(record.channel_id) == "/time/sync":
                    data = json.loads(record.data)["data"]
                    best = {"source": data["source"], "synced": data["synced"], "shift_ns": data["shift_ns"]}
    span = {"boot_id": started.get("boot_id"),
            "start_mono": int(started["start_mono_ns"]) if "start_mono_ns" in started else None,
            # log_time is mono_ns plus a shift fixed for the file, so its span is the file's duration
            "duration_ns": last - first if first is not None else None,
            "reference": best}
    spans[path] = (key, span)
    return span


def known_span(path: Path) -> dict | None:
    """The span if it was read already. If not, starts reading it in the background and returns None."""
    stat = path.stat()
    if (cached := spans.get(path)) and cached[0] == (stat.st_size, stat.st_mtime_ns):
        return cached[1]
    if path not in being_read:
        being_read.add(path)

        def read() -> None:
            try:
                file_span(path)
            except Exception:  # unreadable: listed without times, not read again until it changes
                spans[path] = ((stat.st_size, stat.st_mtime_ns),
                               {"boot_id": None, "start_mono": None, "duration_ns": None, "reference": None})
            finally:
                being_read.discard(path)

        span_reader.submit(read)
    return None


def final_reference(path: Path) -> dict | None:
    return clock.reference() if path == rec.path else best_reference(file_span(path))


def best_reference(span: dict) -> dict | None:
    best = span["reference"]
    # mono_ns is the same clock for the whole boot, so a better reference reached since that file closed fixes it too.
    current_boot = boot_id()
    same_boot = current_boot is not None and span["boot_id"] == current_boot
    if same_boot and (best is None or RANK[clock.source] > RANK[best["source"]]):
        return clock.reference()
    return best


def export(path: Path, out) -> None:
    """Writes `path` re-stamped with the best time reference of the whole run, which mono_ns makes possible:
    it never jumps within a boot, so one shift is right for every message, before and after the sync."""
    reference = final_reference(path)
    writer = Writer(_Counting(out), index_types=INDEXES)
    writer.start(library=f"blueos-complete-data-logger/{VERSION}")
    channels: dict[int, int] = {}
    problems: list[str] = []
    skipped = 0
    for record in records(path, problems):
        if isinstance(record, Channel):
            if record.id not in channels:  # chunks repeat their channels
                channels[record.id] = writer.register_channel(record.topic, message_encoding="json", schema_id=0)
        elif isinstance(record, Metadata) and record.name != "time":
            writer.add_metadata(record.name, record.metadata)
        elif isinstance(record, Message):
            try:
                envelope = json.loads(record.data)
                log_time = record.log_time
                if reference:
                    envelope["corrected_ns"] = log_time = envelope["mono_ns"] + reference["shift_ns"]
                    envelope["time_synced_externally"] = reference["synced"]
                    envelope["time_source"] = reference["source"]
                writer.add_message(channels[record.channel_id], log_time=log_time,
                                   data=json.dumps(envelope, separators=(",", ":")).encode(),
                                   publish_time=record.publish_time)
            except (ValueError, KeyError, TypeError):
                skipped += 1
    writer.add_metadata("time", {k: str(v) for k, v in (reference or {}).items()})
    if problems or skipped:  # said in the file itself, so a partial download never passes for a whole one
        writer.add_metadata("export_problems", {"skipped_messages": str(skipped),
                                                **{f"problem_{i}": p for i, p in enumerate(problems)}})
    writer.finish()


TOPIC_PART = re.compile(r"[^A-Za-z0-9_.-]+")
MAX_FUTURE_NS = 1_000_000_000  # a sender's sync can be off by about its round trip, never by a second
MAX_PAST_NS = 600 * 10**9  # a sender may buffer, but not for longer than this


def time_info() -> dict:
    mono = time.monotonic_ns()
    return {"mono_ns": mono, "corrected_ns": mono + clock.shift, "shift_ns": clock.shift,
            "time_synced_externally": clock.synced, "time_source": clock.source}


def refuse(state: dict, error: str) -> dict:
    """A refused log is data that never reaches the file, so the operator sees it on the page, not only the client."""
    state["refused"] = state.get("refused", 0) + 1
    rec.errors[f"ingest {state['name']}"] = f"{state['refused']} messages refused: {error}"
    if state["refused"] == 1:
        rec.event(f"client {state['name']}: refusing messages: {error}")
    return {"op": "error", "error": error}


def ingest(state: dict, message) -> dict | None:
    """One message from another service or extension. Returns the reply to send, if any."""
    if not isinstance(message, dict):
        return {"op": "error", "error": "expected a JSON object"}
    op = message.get("op")
    if op == "hello":
        rec.errors.pop(f"ingest {state['name']}", None)
        state.pop("refused", None)
        state["name"] = TOPIC_PART.sub("_", str(message.get("name", ""))).strip("_")[:64] or "unnamed"
        rec.event(f"client {state['name']} connected")
        return None
    if op == "sync":
        return {"op": "sync", "t0": message.get("t0"), **time_info()}
    if op == "log":
        topic, mono = message.get("topic"), message.get("mono_ns")
        parts = [TOPIC_PART.sub("_", part) for part in topic.split("/") if part.strip(".")] if isinstance(topic, str) else []
        if not parts:
            return refuse(state, "log needs a topic")
        now = time.monotonic_ns()
        if mono is not None and (type(mono) is not int or not now - MAX_PAST_NS <= mono <= now + MAX_FUTURE_NS):
            return refuse(state, "mono_ns must be our CLOCK_MONOTONIC in ns, from a sync, not in the future and "
                                 "not over 10 minutes old")
        if rec.stopped or rec.paused or rec.write_error:
            return refuse(state, "recording is stopped from the page" if rec.stopped else
                          "recording is paused: " + (rec.write_error or "disk full"))
        # Always under the sender's own namespace, so nobody can write into /time or /mavlink.
        rec.write(f"/extensions/{state['name']}/{'/'.join(parts)[:200]}", message.get("data"), mono=mono)
        if state.pop("refused", None):
            rec.errors.pop(f"ingest {state['name']}", None)
        return None
    return {"op": "error", "error": f"unknown op {op!r}, expected hello, sync or log"}


async def serve_ingest() -> None:
    async def client(ws) -> None:
        state = {"name": "unnamed"}
        try:
            async for raw in ws:
                try:
                    reply = ingest(state, json.loads(raw))
                except (ValueError, RecursionError):  # RecursionError: nested deeper than the parser goes
                    reply = {"op": "error", "error": "invalid JSON"}
                if reply:
                    await ws.send(json.dumps(reply))
        finally:
            rec.errors.pop(f"ingest {state['name']}", None)
            rec.event(f"client {state['name']} disconnected")

    # A short close_timeout, so a frozen client can't hold up a shutdown.
    async with websockets.serve(client, "0.0.0.0", INGEST_PORT, max_size=2**20, max_queue=16, close_timeout=1):
        await asyncio.Future()


def disk_full(usage) -> bool:
    return usage.free < MIN_FREE_BYTES or (usage.total - usage.free) / usage.total >= MAX_DISK_USE


async def watch_disk() -> None:
    """Pauses before the disk fills, resumes when there is room, and gets recording going again after a write error."""
    while True:
        paused = disk_full(await asyncio.to_thread(shutil.disk_usage, LOG_DIR))
        if paused and not rec.paused:
            rec.event(f"recording paused: disk over {MAX_DISK_USE:.0%} full or under {MIN_FREE_BYTES // 2**20} MB free")
            rec.paused = True
        elif rec.paused and not paused and not rec.closed:
            rec.resume()
        if rec.write_error:
            rec.recover()
        await asyncio.sleep(10)


async def keep_flushed() -> None:
    """Bounds what a power cut can lose to the last FLUSH_S seconds: the chunk in memory is written out, then synced
    to the card, which otherwise could keep finished chunks in its cache for half a minute."""
    while True:
        await asyncio.sleep(FLUSH_S)
        fd = rec.flush()
        if fd is None:
            continue
        try:
            await asyncio.to_thread(os.fsync, fd)
        except OSError as error:
            # A failing card takes writes into the page cache and reports the error here, once. EBADF and EINVAL only
            # mean the file was swapped by a new one meanwhile, and it was synced as it closed.
            if error.errno not in (errno.EBADF, errno.EINVAL):
                with rec.lock:
                    if not rec.write_error and not rec.file.closed and rec.file.fileno() == fd:
                        rec._failed(error)


async def supervise(name: str, run) -> None:
    clear = None
    while True:
        try:
            await run()
            return  # finished on purpose
        except asyncio.CancelledError:
            raise
        except Exception as error:  # a bug in one source must not stop the others from recording
            if clear:
                clear.cancel()
            rec.errors[name] = f"crashed, restarting: {type(error).__name__}: {error}"
            rec.event(f"{name} crashed, restarting: {error!r}")
            await asyncio.sleep(5)
            # Off the page once it has run for a minute without crashing again.
            clear = asyncio.get_running_loop().call_later(60, rec.errors.pop, name, None)


REGISTER_SERVICE = {
    "name": "Single File Logger",
    "description": "Records MAVLink, BlueOS services, kernel and service logs into one MCAP file",
    "icon": "mdi-database-clock",
    "company": "Blue Robotics",
    "version": VERSION,
    "webpage": "https://github.com/rafaellehmkuhl/blueos-complete-data-logger",
    "api": "",
}


def recordings() -> list:
    """Every recording with its number and when it started and ended, in the best time known for it: the clock that
    named the file may have been wrong then."""
    found = []
    for p in sorted(LOG_DIR.glob("*.mcap"), reverse=True):
        try:
            entry = {"name": p.name, "size": p.stat().st_size, "active": p == rec.path and not rec.closed,
                     "number": int(p.name[:4]) if p.name[:4].isdigit() else None,
                     "start": None, "end": None, "synced": None}
        except OSError:  # deleted meanwhile
            continue
        try:  # unreadable: listed without times
            if entry["active"]:
                start, duration, reference = rec.start_mono, None, clock.reference()
            elif (span := known_span(p)) is None:
                entry["pending"] = True
                start = duration = reference = None
            else:
                start, duration, reference = span["start_mono"], span["duration_ns"], best_reference(span)
            if start is not None and reference:
                entry["start"] = iso(start + reference["shift_ns"])
                entry["end"] = iso(start + reference["shift_ns"] + duration) if duration is not None else None
                entry["synced"] = reference["synced"]
        except Exception:  # damage is already skipped by records(), so this is a file that vanished or can't be opened
            pass
        found.append(entry)
    return found


def status() -> dict:
    mono, now = time.monotonic_ns(), time.monotonic()
    try:
        usage = shutil.disk_usage(LOG_DIR)
        disk = {"total": usage.total, "free": usage.free, "max_use": MAX_DISK_USE, "min_free": MIN_FREE_BYTES}
    except OSError:
        disk = None
    return {
        "file": rec.path.name,
        "messages": rec.messages,
        "topics": len(rec.channels),
        "paused": rec.paused,
        "stopped": rec.stopped,
        "auto_record": auto_record,
        "write_error": rec.write_error,
        "dropped": rec.dropped,
        "links": rec.links,
        # Seconds since each connected websocket last sent something: connected but silent is not healthy.
        "idle_s": {name: round(now - rec.seen[name], 1) for name, state in rec.links.items()
                   if state == "connected" and name in rec.seen},
        "mavlink_gaps": mavlink_gaps,
        "failing_endpoints": dict(rec.errors),
        "time": {"source": clock.source, "synced_externally": clock.synced, "syncs": clock.syncs,
                 "corrected": iso(mono + clock.shift), "linux": iso(time.time_ns()), "linux_ntp_synced": ntp_synced()},
        "version": VERSION,
        "disk": disk,
        "recordings": recordings(),
    }


PAGE = (Path(__file__).parent / "index.html").read_bytes()
# A corrected download re-encodes every message in this process, so one at a time keeps a core for recording.
exporting = BoundedSemaphore(1)


class Handler(BaseHTTPRequestHandler):
    timeout = 30  # a client that stops talking (a dropped tether) frees its thread instead of holding it forever

    def log_message(self, *args) -> None:
        pass

    def reply(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def reply_json(self, code: int, body: dict) -> None:
        self.reply(code, json.dumps(body).encode(), "application/json")

    def recording(self, prefix: str) -> Path | None:
        # Only names that are listed in the folder, so a crafted path can never leave it.
        name = unquote(urlparse(self.path).path.removeprefix(prefix))
        return next((p for p in LOG_DIR.glob("*.mcap") if p.name == name), None)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/register_service":
            self.reply_json(200, REGISTER_SERVICE)
        elif path == "/settings":
            self.reply_json(200, {"min": MIN_PERIOD, "max": MAX_PERIOD, "auto_record": auto_record, "polls": [
                {"path": p, "period": periods[p], "default": d, "always": a} for p, d, a in POLLS]})
        elif path == "/status":
            self.reply_json(200, status())
        elif path.startswith("/download/") and (file := self.recording("/download/")):
            if not exporting.acquire(blocking=False):
                self.reply(503, b"Another download is being prepared, try again when it finishes", "text/plain")
                return
            try:
                self.send_response(200)  # no length: the re-stamped file is built while it streams
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Disposition", f'attachment; filename="{file.stem}_synced.mcap"')
                self.end_headers()
                export(file, self.wfile)
            finally:
                exporting.release()
        elif path == "/download_zip":
            names = set(parse_qs(urlparse(self.path).query).get("name", []))
            files = sorted(p for p in LOG_DIR.glob("*.mcap") if p.name in names)  # only listed names, like recording()
            if not files:
                self.reply(404, b"None of these recordings exist", "text/plain")
                return
            if not exporting.acquire(blocking=False):
                self.reply(503, b"Another download is being prepared, try again when it finishes", "text/plain")
                return
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="recordings_{len(files)}_synced.zip"')
                self.end_headers()
                # Stored, not deflated: the files are already compressed. Streamed, so nothing is staged on the card.
                with zipfile.ZipFile(self.wfile, "w", zipfile.ZIP_STORED) as z:
                    for file in files:
                        with z.open(f"{file.stem}_synced.mcap", "w", force_zip64=True) as entry:
                            export(file, entry)
            finally:
                exporting.release()
        elif path.startswith("/raw/") and (file := self.recording("/raw/")):
            size = file.stat().st_size  # the active file keeps growing, so send a snapshot
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.send_header("Content-Disposition", f'attachment; filename="{file.name}"')
            self.end_headers()
            with file.open("rb") as f:
                while size > 0 and (chunk := f.read(min(2**20, size))):
                    self.wfile.write(chunk)
                    size -= len(chunk)
        elif path == "/":
            self.reply(200, PAGE, "text/html; charset=utf-8")
        else:
            self.reply(404, b"Not found", "text/plain")

    def do_POST(self) -> None:
        # Only JSON: a browser can't send that cross-site without a preflight, which this server never approves, so a
        # page on another site can't change settings or delete recordings.
        if self.headers.get_content_type() != "application/json":
            self.reply(415, b"Content-Type must be application/json", "text/plain")
            return
        path = urlparse(self.path).path
        if path == "/settings":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if 0 < length <= 2**16 else None
            except ValueError:
                body = None
            if isinstance(body, dict) and "auto_record" in body and "poll_periods" not in body:
                error = set_auto_record(body["auto_record"])
            else:
                error = apply_periods(body.get("poll_periods") if isinstance(body, dict) else None)
            self.reply_json(400 if error else 200, {"error": error, "file": rec.path.name})
        elif path == "/new_file":
            try:
                finished = rec.rotate()
            except Exception as error:
                self.reply_json(500, {"error": f"could not start a new file: {error}", "file": rec.path.name})
                return
            self.reply_json(200, {"finished": finished.name, "file": rec.path.name})
        elif path in ("/stop", "/start"):
            try:
                rec.stop() if path == "/stop" else rec.restart()
            except Exception as error:
                self.reply_json(500, {"error": f"could not {path[1:]} recording: {error}"})
                return
            self.reply_json(200, {"stopped": rec.stopped, "file": rec.path.name})
        elif (file := self.recording("/delete/")) is None or (file == rec.path and not rec.closed):
            self.reply(404, b"No such finished recording", "text/plain")
        else:
            try:
                file.unlink()
            except OSError as error:
                self.reply(500, f"Could not delete it: {error}".encode(), "text/plain")
                return
            self.send_response(204)
            self.end_headers()


async def main() -> None:
    global rec
    # Every poll can have one request in flight, and a few stalled services must not hold up the healthy ones (or the
    # log scan) in asyncio's default pool of cpus + 4 threads.
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(len(POLLS) + 8))
    load_settings()
    rec = Recorder(clock, record=auto_record)
    if auto_record:
        rec.start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    print(f"Recording to {rec.path}" if auto_record else "Automatic recording is off, waiting for Start",
          f"web interface on port {PORT}", sep=", ", flush=True)

    sources = {f"poll {path}": (lambda p=path, a=always: poll(p, a)) for path, _, always in POLLS}
    sources["mavlink2rest"] = lambda: websocket("mavlink2rest", "/mavlink2rest/ws/mavlink?filter=.*", handle_mavlink)
    sources["kernel"] = lambda: websocket("kernel", "/system-information/ws/kernel_buffer", handle_kernel)
    sources["linux clock"] = watch_linux_clock
    sources["service logs"] = tail_service_logs
    sources["container logs"] = follow_container_logs
    sources["ingest"] = serve_ingest
    sources["disk"] = watch_disk
    sources["flush"] = keep_flushed
    tasks = [asyncio.create_task(supervise(name, run)) for name, run in sources.items()]

    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    await stop.wait()
    # The file first: Docker kills the container 10 s after SIGTERM, and stopping the sources can take a while.
    rec.event("recording stopped")
    rec.finish()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
