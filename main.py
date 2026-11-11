#!/usr/bin/env python3
"""BlueOS Complete Data Logger.

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
import json
import os
import re
import shutil
import signal
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import unquote, urlparse

import websockets
from mcap.reader import make_reader
from mcap.records import Channel, Message, Metadata
from mcap.stream_reader import StreamReader
from mcap.writer import Writer

VERSION = os.environ.get("VERSION", "0.0.0")
HOST = os.environ.get("BLUEOS_HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "9650"))
INGEST_PORT = int(os.environ.get("INGEST_PORT", "9651"))
LOG_DIR = Path(os.environ.get("LOG_DIR", "/app/logs"))
SERVICE_LOGS = Path(os.environ.get("SERVICE_LOGS", "/var/logs/blueos/services"))
MIN_FREE_BYTES = int(os.environ.get("MIN_FREE_MB", "1024")) * 2**20

# SYSTEM_TIME arrives over a websocket, so its shift estimate jitters by the delivery latency.
AUTOPILOT_STEP_NS = 1_000_000_000
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


def valid_period(value) -> bool:
    return type(value) in (int, float) and MIN_PERIOD <= value <= MAX_PERIOD


def load_settings() -> None:
    try:
        saved = json.loads(SETTINGS.read_text()).get("poll_periods", {})
    except (OSError, ValueError, AttributeError):
        return
    periods.update({path: v for path, v in saved.items() if path in DEFAULT_PERIODS and valid_period(v)})


def apply_periods(new) -> str | None:
    """Sets poll periods and starts a new file, so every file has one set of rates. Returns an error, if any."""
    if not isinstance(new, dict) or not new:
        return "expected an object of poll path to period in seconds"
    for path, value in new.items():
        if path not in DEFAULT_PERIODS:
            return f"unknown poll {path!r}"
        if not valid_period(value):
            return f"the period of {path} must be a number of seconds from {MIN_PERIOD} to {MAX_PERIOD}"
    periods.update(new)
    changed = {path: v for path, v in periods.items() if v != DEFAULT_PERIODS[path]}
    temporary = SETTINGS.with_suffix(".tmp")
    temporary.write_text(json.dumps({"poll_periods": changed}, indent=2))
    temporary.replace(SETTINGS)  # a power cut leaves the old settings or the new ones, never half
    rec.event(f"poll periods changed: {json.dumps(changed)}")
    rec.rotate()
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
    def __init__(self, clock: Clock):
        self.clock = clock
        self.generation = 0  # bumped with every new file, so sources that write on change write their state again
        self._open()
        self.dropped = 0
        self.paused = False
        self.errors: dict[str, str] = {}
        self.links: dict[str, str] = {}
        self.lock = Lock()  # the container log threads write directly, so a backlog can't starve the event loop

    def _open(self) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        # The sequence number keeps files in recording order even when the clock that named them was wrong.
        number = max((int(p.name[:4]) for p in LOG_DIR.glob("[0-9][0-9][0-9][0-9]_*.mcap")), default=0) + 1
        stamp = time.strftime("%Y-%m-%dT%H-%M-%SZ", time.gmtime())
        self.path = LOG_DIR / f"{number:04d}_{stamp}.mcap"
        self.file = open(self.path, "xb")  # never overwrite a recording
        # ponytail: data since the last 1 MB chunk is lost on power cut; `mcap recover` salvages the rest.
        self.writer = Writer(self.file)
        self.writer.start(library=f"blueos-complete-data-logger/{VERSION}")
        self.channels: dict[str, int] = {}
        self.messages = 0
        self.generation += 1

    def start(self) -> None:
        """Opens the file with what a reader needs to place it in time on its own."""
        boot_id = Path("/proc/sys/kernel/random/boot_id")
        self.writer.add_metadata("recording", {
            "extension_version": VERSION,
            "boot_id": boot_id.read_text().strip() if boot_id.exists() else "unknown",
            "start_mono_ns": str(time.monotonic_ns()),
            "start_wall_ns": str(time.time_ns()),
        })
        self.writer.add_metadata("poll_periods_s", {path: str(period) for path, period in periods.items()})
        # The reference in use, so a file started after the sync is synchronized from its first message.
        self.write("/time/sync", {**self.clock.reference(), "previous_shift_ns": self.clock.shift, "step_ns": 0,
                                  "boottime_ns": time.clock_gettime_ns(BOOTTIME), "linux_ntp_synced": ntp_synced(),
                                  "file_start": True})

    def rotate(self) -> Path:
        """Closes the current file and starts the next one, without losing or misplacing a message. Returns the
        finished file."""
        finished = self.path
        self.event("closing this file, recording continues in the next one")
        with self.lock:
            self._close()
            self._open()
        self.start()
        self.event(f"started after {finished.name}")
        return finished

    def write(self, topic: str, data, mono: int | None = None) -> None:
        with self.lock:
            self._write(topic, data, mono)

    def _write(self, topic: str, data, mono: int | None) -> None:
        """Records `data` as happening now, or at `mono` (our CLOCK_MONOTONIC) when the sender stamped it."""
        if self.paused:
            self.dropped += 1
            return
        now, wall = time.monotonic_ns(), time.time_ns()
        extra = {}
        if mono is None:
            mono = now
        else:
            wall -= now - mono  # what Linux believed when it happened
            extra["received_mono_ns"] = now
        if topic not in self.channels:
            self.channels[topic] = self.writer.register_channel(topic, message_encoding="json", schema_id=0)
        envelope = {"mono_ns": mono, "wall_ns": wall, "corrected_ns": mono + self.clock.shift,
                    "time_synced_externally": self.clock.synced, "time_source": self.clock.source, **extra,
                    "data": data}
        payload = json.dumps(envelope, separators=(",", ":")).encode()
        # MCAP readers sort by log_time, so it must never jump: monotonic time placed at the wall time of the start.
        log_time = mono + self.clock.start_shift
        self.writer.add_message(self.channels[topic], log_time=log_time, data=payload, publish_time=wall)
        self.messages += 1

    def event(self, text: str) -> None:
        print(text, flush=True)
        self.write("/logger/events", {"event": text})

    def sync(self, source: str, shift: int, **extra) -> bool:
        """Moves the reference to `source` unless a better one is already in use. Returns whether it moved."""
        if RANK[source] < RANK[self.clock.source]:
            return False
        previous = self.clock.shift
        self.clock.shift, self.clock.source = shift, source
        self.clock.syncs += 1
        self.write("/time/sync", {**self.clock.reference(), "previous_shift_ns": previous, "step_ns": shift - previous,
                                  "boottime_ns": time.clock_gettime_ns(BOOTTIME), "linux_ntp_synced": ntp_synced(),
                                  **extra})
        print(f"time reference {source}: corrected time is now {iso(time.monotonic_ns() + shift)}", flush=True)
        return True

    def finish(self) -> None:
        # The reference export() applies to the whole file: corrected = mono_ns + shift_ns.
        with self.lock:
            self.paused = True  # threads still running get dropped from here on
            self._close()

    def _close(self) -> None:
        self.writer.add_metadata("time", {k: str(v) for k, v in self.clock.reference().items()})
        self.writer.finish()
        self.file.close()


clock = Clock()
rec: Recorder  # created in main(), after the event loop exists


def get_json(path: str):
    with urllib.request.urlopen(f"http://{HOST}{path}", timeout=15) as response:
        body = response.read()
    return json.loads(body) if body else None


async def poll(path: str, always: bool) -> None:
    topic = "/blueos" + path.split("?")[0].rstrip("/*")
    last, generation = None, rec.generation
    while True:
        started = time.monotonic()
        if generation != rec.generation:  # a new file: write the current state even if it didn't change
            last, generation = None, rec.generation
        try:
            data, failed = redact(await asyncio.to_thread(get_json, path)), False
            rec.errors.pop(path, None)
        except Exception as error:  # a service being down is data too
            data, failed = {"error": f"{type(error).__name__}: {error}"}, True
            rec.errors[path] = data["error"]
        encoded = json.dumps(data, sort_keys=True)
        if encoded != last or (always and not failed):
            rec.write(topic, data)
        last = encoded
        # Wait out the period, but a new file (which is also how new periods are applied) cuts the wait short.
        while time.monotonic() - started < periods[path] and generation == rec.generation:
            await asyncio.sleep(min(0.5, max(0.0, periods[path] - (time.monotonic() - started))))


def autopilot_time(message: dict) -> None:
    unix_ns = message.get("time_unix_usec", 0) * 1000
    if unix_ns < OLDEST_VALID_NS:  # the autopilot has no time source yet
        return
    # ArduPilot never moves its clock backwards, so after a bad SYSTEM_TIME from a topside it can stay wrong while NTP
    # has already fixed Linux. That's why it ranks below NTP (see RANK).
    shift = unix_ns - time.monotonic_ns()
    if clock.source != "autopilot" or abs(shift - clock.shift) > AUTOPILOT_STEP_NS:
        rec.sync("autopilot", shift, time_unix_usec=message["time_unix_usec"], time_boot_ms=message.get("time_boot_ms"))


async def websocket(name: str, path: str, handle) -> None:
    # Iterating connect() reconnects with backoff when the service restarts or nginx answers 5xx.
    async for ws in websockets.connect(f"ws://{HOST}{path}", max_size=None, open_timeout=10):
        rec.links[name] = "connected"
        rec.event(f"{name} connected")
        try:
            async for raw in ws:
                handle(json.loads(raw))
        except websockets.ConnectionClosed:
            pass
        rec.links[name] = "reconnecting"
        rec.event(f"{name} disconnected")


def handle_mavlink(packet: dict) -> None:
    header, message = packet["header"], packet["message"]
    rec.write(f"/mavlink/{header['system_id']}/{header['component_id']}/{message['type']}", packet)
    if message["type"] == "SYSTEM_TIME" and header["component_id"] == 1:
        autopilot_time(message)


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


def read_new_lines(path: Path, state: list, backfill: bool) -> None:
    offset, partial = state
    if path.stat().st_size < offset:  # truncated in place
        offset = 0
    with path.open("rb") as file:
        file.seek(offset)
        chunk = file.read(4 * 2**20)
    lines = (partial + chunk).split(b"\n")
    state[:] = [offset + len(chunk), lines.pop()]
    for line in lines:
        if line.strip():
            rec.write(f"/blueos/logs/{path.parent.name}",
                      {"line": line.decode(errors="replace"), "file": path.name, "backfill": backfill})


async def tail_service_logs() -> None:
    # Every file is followed, not just the newest: log files are named and dated by the wall clock, so after a
    # backward step the file being written can look older than the one before it.
    boot_wall = time.time() - time.clock_gettime(BOOTTIME)
    tails: dict[Path, list] = {}  # file -> [offset, partial line]
    first_scan = True

    def scan() -> None:  # in a thread: the backlog since boot can be hundreds of MB, too much for the event loop
        if SERVICE_LOGS.is_dir():
            seen = set()
            for path in SERVICE_LOGS.glob("*/*"):
                try:
                    if path.suffix == ".zip" or not path.is_file():
                        continue
                    seen.add(path)
                    if path not in tails:
                        # At start, read whole only what was written since boot, so the boot sequence is in the log.
                        # Files that show up later are new rotations, read whole.
                        whole = not first_scan or path.stat().st_mtime > boot_wall
                        tails[path] = [0 if whole else path.stat().st_size, b""]
                    read_new_lines(path, tails[path], backfill=first_scan)
                except OSError:  # zipped away mid-scan
                    continue
            for gone in tails.keys() - seen:
                del tails[gone]

    while True:
        await asyncio.to_thread(scan)
        first_scan = False
        await asyncio.sleep(1)


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
    """Runs in its own thread until the container stops: a log stream can stay silent for hours."""
    name = container["Names"][0].lstrip("/")
    base = f"http://{HOST}/docker/containers/{container['Id']}"
    tty = get_json(f"/docker/containers/{container['Id']}/json")["Config"]["Tty"]
    since = f"{since_ns // 10**9}.{since_ns % 10**9:09d}"
    url = f"{base}/logs?follow=1&stdout=1&stderr=1&timestamps=1&since={since}"
    with urllib.request.urlopen(url, timeout=None) as response:
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


def records(path: Path):
    """Every record of a recording, including one still being written or cut short by a power loss."""
    with path.open("rb") as file:
        try:
            yield from StreamReader(file).records
        except Exception:  # the unfinished end of the file
            return


def final_reference(path: Path) -> dict:
    if path == rec.path:
        return clock.reference()
    try:  # a finished file has it in its summary
        with path.open("rb") as file:
            for metadata in make_reader(file).iter_metadata():
                if metadata.name == "time":
                    m = metadata.metadata
                    return {"source": m["source"], "synced": m["synced"] == "True", "shift_ns": int(m["shift_ns"])}
    except Exception:
        pass
    # An unfinished file: the reference only ever improves, so the last /time/sync record is the best one.
    channels, best = {}, None
    for record in records(path):
        if isinstance(record, Channel):
            channels[record.id] = record.topic
        elif isinstance(record, Message) and channels.get(record.channel_id) == "/time/sync":
            data = json.loads(record.data)["data"]
            best = {"source": data["source"], "synced": data["synced"], "shift_ns": data["shift_ns"]}
    return best


def export(path: Path, out) -> None:
    """Writes `path` re-stamped with the best time reference of the whole run, which mono_ns makes possible:
    it never jumps within a boot, so one shift is right for every message, before and after the sync."""
    reference = final_reference(path)
    writer = Writer(_Counting(out))
    writer.start(library=f"blueos-complete-data-logger/{VERSION}")
    channels: dict[int, int] = {}
    for record in records(path):
        if isinstance(record, Channel):
            channels[record.id] = writer.register_channel(record.topic, message_encoding="json", schema_id=0)
        elif isinstance(record, Metadata) and record.name != "time":
            writer.add_metadata(record.name, record.metadata)
        elif isinstance(record, Message) and reference:
            envelope = json.loads(record.data)
            envelope["corrected_ns"] = envelope["mono_ns"] + reference["shift_ns"]
            envelope["time_synced_externally"] = reference["synced"]
            envelope["time_source"] = reference["source"]
            writer.add_message(channels[record.channel_id], log_time=envelope["corrected_ns"],
                               data=json.dumps(envelope, separators=(",", ":")).encode(),
                               publish_time=record.publish_time)
    writer.add_metadata("time", {k: str(v) for k, v in (reference or {}).items()})
    writer.finish()


TOPIC_PART = re.compile(r"[^A-Za-z0-9_.-]+")
MAX_FUTURE_NS = 1_000_000_000  # a sender's sync can be off by about its round trip, never by a second


def time_info() -> dict:
    mono = time.monotonic_ns()
    return {"mono_ns": mono, "corrected_ns": mono + clock.shift, "shift_ns": clock.shift,
            "time_synced_externally": clock.synced, "time_source": clock.source}


def ingest(state: dict, message) -> dict | None:
    """One message from another service or extension. Returns the reply to send, if any."""
    if not isinstance(message, dict):
        return {"op": "error", "error": "expected a JSON object"}
    op = message.get("op")
    if op == "hello":
        state["name"] = TOPIC_PART.sub("_", str(message.get("name", ""))).strip("_")[:64] or "unnamed"
        rec.event(f"client {state['name']} connected")
        return None
    if op == "sync":
        return {"op": "sync", "t0": message.get("t0"), **time_info()}
    if op == "log":
        topic, mono = message.get("topic"), message.get("mono_ns")
        parts = [TOPIC_PART.sub("_", part) for part in topic.split("/") if part.strip(".")] if isinstance(topic, str) else []
        if not parts:
            return {"op": "error", "error": "log needs a topic"}
        if mono is not None and (type(mono) is not int or not 0 < mono <= time.monotonic_ns() + MAX_FUTURE_NS):
            return {"op": "error", "error": "mono_ns must be our CLOCK_MONOTONIC in ns, from a sync, not in the future"}
        # Always under the sender's own namespace, so nobody can write into /time or /mavlink.
        rec.write(f"/extensions/{state['name']}/{'/'.join(parts)[:200]}", message.get("data"), mono=mono)
        return None
    return {"op": "error", "error": f"unknown op {op!r}, expected hello, sync or log"}


async def serve_ingest() -> None:
    async def client(ws) -> None:
        state = {"name": "unnamed"}
        try:
            async for raw in ws:
                try:
                    reply = ingest(state, json.loads(raw))
                except ValueError:
                    reply = {"op": "error", "error": "invalid JSON"}
                if reply:
                    await ws.send(json.dumps(reply))
        finally:
            rec.event(f"client {state['name']} disconnected")

    async with websockets.serve(client, "0.0.0.0", INGEST_PORT, max_size=2**20):
        await asyncio.Future()


async def watch_disk() -> None:
    while True:
        paused = shutil.disk_usage(LOG_DIR).free < MIN_FREE_BYTES
        if paused and not rec.paused:
            rec.event(f"recording paused: less than {MIN_FREE_BYTES // 2**20} MB free")
            rec.paused = True
        elif rec.paused and not paused:
            rec.paused = False
            rec.event(f"recording resumed after dropping {rec.dropped} messages")
        await asyncio.sleep(10)


async def supervise(name: str, run) -> None:
    while True:
        try:
            await run()
            return  # finished on purpose
        except asyncio.CancelledError:
            raise
        except Exception as error:  # a bug in one source must not stop the others from recording
            rec.event(f"{name} crashed, restarting: {error!r}")
            await asyncio.sleep(5)


REGISTER_SERVICE = {
    "name": "Complete Data Logger",
    "description": "Records MAVLink, BlueOS services, kernel and service logs into one MCAP file",
    "icon": "mdi-database-clock",
    "company": "Blue Robotics",
    "version": VERSION,
    "webpage": "https://github.com/rafaellehmkuhl/blueos-complete-data-logger",
    "api": "",
}


def status() -> dict:
    mono = time.monotonic_ns()
    return {
        "file": rec.path.name,
        "messages": rec.messages,
        "topics": len(rec.channels),
        "paused": rec.paused,
        "dropped": rec.dropped,
        "links": rec.links,
        "failing_endpoints": rec.errors,
        "time": {"source": clock.source, "synced_externally": clock.synced, "syncs": clock.syncs,
                 "corrected": iso(mono + clock.shift), "linux": iso(time.time_ns()), "linux_ntp_synced": ntp_synced()},
        "version": VERSION,
        "recordings": [{"name": p.name, "size": p.stat().st_size, "active": p == rec.path}
                       for p in sorted(LOG_DIR.glob("*.mcap"), reverse=True)],
    }


PAGE = (Path(__file__).parent / "index.html").read_bytes()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass

    def reply(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def recording(self, prefix: str) -> Path | None:
        # Only names that are listed in the folder, so a crafted path can never leave it.
        name = unquote(urlparse(self.path).path.removeprefix(prefix))
        return next((p for p in LOG_DIR.glob("*.mcap") if p.name == name), None)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/register_service":
            self.reply(200, json.dumps(REGISTER_SERVICE).encode(), "application/json")
        elif path == "/settings":
            body = {"min": MIN_PERIOD, "max": MAX_PERIOD, "polls": [
                {"path": p, "period": periods[p], "default": d, "always": a} for p, d, a in POLLS]}
            self.reply(200, json.dumps(body).encode(), "application/json")
        elif path == "/status":
            self.reply(200, json.dumps(status()).encode(), "application/json")
        elif path.startswith("/download/") and (file := self.recording("/download/")):
            self.send_response(200)  # no length: the re-stamped file is built while it streams
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Disposition", f'attachment; filename="{file.stem}_synced.mcap"')
            self.end_headers()
            export(file, self.wfile)
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
        if urlparse(self.path).path == "/settings":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if 0 < length <= 2**16 else None
            except ValueError:
                body = None
            error = apply_periods(body.get("poll_periods") if isinstance(body, dict) else None)
            self.reply(400 if error else 200, json.dumps({"error": error, "file": rec.path.name}).encode(),
                       "application/json")
            return
        if urlparse(self.path).path == "/new_file":
            finished = rec.rotate()
            self.reply(200, json.dumps({"finished": finished.name, "file": rec.path.name}).encode(), "application/json")
            return
        file = self.recording("/delete/")
        if file is None or file == rec.path:
            self.reply(404, b"No such finished recording", "text/plain")
            return
        file.unlink()
        self.send_response(204)
        self.end_headers()


async def main() -> None:
    global rec
    load_settings()
    rec = Recorder(clock)
    rec.start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    print(f"Recording to {rec.path}, web interface on port {PORT}", flush=True)

    sources = {f"poll {path}": (lambda p=path, a=always: poll(p, a)) for path, _, always in POLLS}
    sources["mavlink2rest"] = lambda: websocket("mavlink2rest", "/mavlink2rest/ws/mavlink?filter=.*", handle_mavlink)
    sources["kernel"] = lambda: websocket("kernel", "/system-information/ws/kernel_buffer", handle_kernel)
    sources["linux clock"] = watch_linux_clock
    sources["service logs"] = tail_service_logs
    sources["container logs"] = follow_container_logs
    sources["ingest"] = serve_ingest
    sources["disk"] = watch_disk
    tasks = [asyncio.create_task(supervise(name, run)) for name, run in sources.items()]

    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    rec.event("recording stopped")
    rec.finish()
    server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
