"""Run with: python test_main.py (needs the packages from requirements.txt). Each test is one promise of the README
or one failure the logger must survive."""

import asyncio
import io
import json
import os
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

os.environ["LOG_DIR"] = tempfile.mkdtemp()
import main  # noqa: E402
import websockets  # noqa: E402
from mcap.reader import make_reader  # noqa: E402

HOUR = 3_600_000_000_000
CPU = "/system-information/system/cpu"


def messages(data: bytes) -> list:
    reader = make_reader(io.BytesIO(data))
    return [(c.topic, m.log_time, json.loads(m.data)) for _, c, m in reader.iter_messages(log_time_order=False)]


def metadata(data: bytes) -> dict:
    return {m.name: m.metadata for m in make_reader(io.BytesIO(data)).iter_metadata()}


def topic(raw: list, name: str) -> list:
    return [e for t, _, e in raw if t == name]


def fresh() -> None:
    """A new recording with the Linux clock as the only reference, as at boot without internet."""
    main.clock.shift = main.clock.start_shift = main.clock.linux = main.linux_shift()
    main.clock.source, main.clock.syncs = "linux_unsynced", 0
    main.autopilot_shifts.clear()
    main.autopilot_system = None
    main.rec = main.Recorder(main.clock)
    main.rec.start()


class Failing:
    """A card that returns EIO."""

    def write(self, data):
        raise OSError(5, "Input/output error")

    def flush(self):
        raise OSError(5, "Input/output error")

    def tell(self):
        return 0


def test_recording() -> None:
    assert main.redact({"user": "a", "Password": "x", "nested": [{"api_key": 1, "ok": 2}]}) == \
        {"user": "a", "Password": "<redacted>", "nested": [{"api_key": "<redacted>", "ok": 2}]}
    assert main.parse_docker_line("2026-11-11T19:00:00.5Z hello world") == ("hello world", 1794423600_500000000)

    fresh()
    main.rec.write("/a", {"v": 1})
    assert main.rec.sync("autopilot", main.clock.shift + HOUR)  # the Linux clock was an hour behind
    main.rec.write("/a", {"v": 2})
    assert not main.rec.sync("linux_unsynced", main.clock.shift - HOUR)  # a worse reference never wins

    # A log line split across two reads is written once, whole.
    log = Path(os.environ["LOG_DIR"]) / "svc" / "service.log"
    log.parent.mkdir()
    log.write_bytes(b"first\nsec")
    state = [0, b"", log.stat().st_size]  # seen at start: what is already there is backfill
    main.read_new_lines(log, state)
    with log.open("ab") as f:
        f.write(b"ond\nthird\n")
    main.read_new_lines(log, state)

    # Another extension logging through the ingest API.
    client = {"name": "unnamed"}
    assert main.ingest(client, {"op": "hello", "name": "My Ext!"}) is None and client["name"] == "My_Ext"
    reply = main.ingest(client, {"op": "sync", "t0": 5})
    assert reply["t0"] == 5 and reply["corrected_ns"] - reply["mono_ns"] == main.clock.shift
    happened = main.time.monotonic_ns() - 10**9
    assert main.ingest(client, {"op": "log", "topic": "../time/sync", "data": {"depth": 3}, "mono_ns": happened}) is None
    assert main.ingest(client, {"op": "log", "topic": "x", "mono_ns": main.time.monotonic_ns() + 5 * 10**9})["op"] == "error"
    assert main.ingest(client, {"op": "log", "topic": "//"})["op"] == "error"
    main.rec.finish()

    raw = messages(main.rec.path.read_bytes())
    a = topic(raw, "/a")
    assert [e["time_synced_externally"] for e in a] == [False, True]
    assert a[1]["corrected_ns"] - a[1]["mono_ns"] - (a[0]["corrected_ns"] - a[0]["mono_ns"]) == HOUR
    # The recorded log_time never jumps when the reference changes: it is mono_ns plus one constant.
    assert len({t - e["mono_ns"] for _, t, e in raw}) == 1
    logged = [(e["data"]["line"], e["data"]["backfill"]) for e in topic(raw, "/blueos/logs/svc")]
    # "second" started before the logger did, so it is backfill even though it was finished later
    assert logged == [("first", True), ("second", True), ("third", False)], logged

    ext = topic(raw, "/extensions/My_Ext/time/sync")
    assert len(ext) == 1 and ext[0]["mono_ns"] == happened and ext[0]["received_mono_ns"] > happened
    assert len(topic(raw, "/time/sync")) == 2, "a client can't write into /time"

    out = io.BytesIO()
    main.export(main.rec.path, out)
    synced = messages(out.getvalue())
    assert len(synced) == len(raw)
    for _, log_time, envelope in synced:  # every message, also the one before the sync, gets the final reference
        assert envelope["time_synced_externally"] and envelope["time_source"] == "autopilot"
        assert log_time == envelope["corrected_ns"] == envelope["mono_ns"] + main.clock.shift
    summary = make_reader(io.BytesIO(out.getvalue())).get_summary()
    topics = [c.topic for c in summary.channels.values()]
    assert len(topics) == len(set(topics)), "every topic once in the download"


def test_new_file() -> None:
    """A new file on request: the old one is closed whole, the new one is synchronized from its first message."""
    fresh()
    main.rec.write("/a", {"v": 3})
    generation = main.rec.generation
    main.rec.sync("autopilot", main.clock.shift + HOUR)
    finished = main.rec.rotate()
    main.rec.write("/a", {"v": 4})
    main.rec.finish()
    assert main.rec.generation == generation + 1 and main.rec.path != finished
    old = messages(finished.read_bytes())  # readable as a finished file, with its summary
    assert [e["data"]["v"] for e in topic(old, "/a")] == [3]
    new = messages(main.rec.path.read_bytes())
    assert new[0][0] == "/time/sync" and new[0][2]["data"]["file_start"] and new[0][2]["time_synced_externally"]
    assert [e["data"]["v"] for e in topic(new, "/a")] == [4]
    # As recorded, each file is placed at the best time known when it started, not at the clock of the first file.
    assert all(t == e["corrected_ns"] for _, t, e in new)
    assert "recording" in metadata(finished.read_bytes()) and "recording" in metadata(main.rec.path.read_bytes())


def test_concurrent_rotations() -> None:
    """Two clients pressing New file while threads write: no error, and every file whole with its start metadata."""
    fresh()
    errors, stop = [], threading.Event()

    def writer() -> None:
        while not stop.is_set():
            main.rec.write("/w", {"x": 1})

    def rotator() -> None:
        try:
            for _ in range(20):
                main.rec.rotate()
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=writer) for _ in range(2)] + [threading.Thread(target=rotator) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads[2:]:
        t.join()
    stop.set()
    for t in threads[:2]:
        t.join()
    main.rec.finish()
    assert not errors, errors
    for path in Path(os.environ["LOG_DIR"]).glob("*.mcap"):
        assert "recording" in metadata(path.read_bytes()), path


def test_poll_periods() -> None:
    """Checked as a whole, saved, applied in a new file that records them. A failed save changes nothing."""
    fresh()
    before = main.rec.path
    assert main.apply_periods({CPU: 0.5, "/nope": 1})  # an error, and nothing applied
    assert main.apply_periods({CPU: 0}) and main.apply_periods({CPU: True}) and main.apply_periods({CPU: "1"})
    assert main.periods[CPU] == 1 and main.rec.path == before
    assert main.apply_periods({CPU: 0.5}) is None
    assert main.periods[CPU] == 0.5 and main.rec.path != before
    assert json.loads(main.SETTINGS.read_text()) == {"poll_periods": {CPU: 0.5}}
    main.periods[CPU] = 1
    main.load_settings()
    assert main.periods[CPU] == 0.5

    current = main.rec.path
    main.SETTINGS.with_suffix(".tmp").mkdir()  # can't be written
    error = main.apply_periods({CPU: 2})
    main.SETTINGS.with_suffix(".tmp").rmdir()
    assert error and "could not save" in error and main.periods[CPU] == 0.5 and main.rec.path == current
    main.rec.finish()
    assert metadata(main.rec.path.read_bytes())["poll_periods_s"][CPU] == "0.5"
    main.periods[CPU] = 1


def test_disk_full() -> None:
    """Recording stops at 95% used, or under the minimum free, whichever comes first."""
    usage = main.shutil._ntuple_diskusage
    assert not main.disk_full(usage(total=100 * 2**30, used=90 * 2**30, free=10 * 2**30))
    assert main.disk_full(usage(total=100 * 2**30, used=95 * 2**30, free=5 * 2**30))
    assert main.disk_full(usage(total=4 * 2**30, used=3.5 * 2**30, free=0.5 * 2**30))


def test_pause_and_resume() -> None:
    """While paused nothing is written and it is counted; after it, state that is written on change is current again."""
    fresh()
    main.rec.paused = True
    main.rec.write("/a", 1)
    generation = main.rec.generation
    assert main.rec.dropped == 1
    client = {"name": "c"}
    assert main.ingest(client, {"op": "log", "topic": "t", "data": 1})["op"] == "error", "the client is told"
    assert "ingest c" in main.rec.errors
    main.rec.resume()
    assert not main.rec.paused and main.rec.generation == generation + 1  # polls write their state again
    assert main.ingest(client, {"op": "log", "topic": "t", "data": 1}) is None and "ingest c" not in main.rec.errors
    main.rec.finish()
    raw = messages(main.rec.path.read_bytes())
    assert not topic(raw, "/a")
    assert topic(raw, "/time/sync")[-1]["data"]["resumed"], "the reference is written again"


def test_write_error() -> None:
    """The card fails: recording stops visibly, the broken writer is never used again, and a new file takes over."""
    fresh()
    main.rec.write("/a", {"v": "before"})
    assert main.rec.flush() is not None
    first = main.rec.path
    main.rec.writer._Writer__stream = Failing()
    main.rec.write("/a", {"v": "lost"})
    assert main.rec.flush() is None
    assert main.rec.write_error and "recording" in main.rec.errors
    status = main.status()
    assert status["write_error"] and "recording" in status["failing_endpoints"]
    counted = main.rec.messages
    main.rec.write("/a", {"v": "dropped"})
    assert main.rec.messages == counted and main.rec.dropped >= 1, "nothing goes into the broken writer"
    try:
        main.rec.rotate()
        raise AssertionError("rotate must refuse while the writer is broken")
    except RuntimeError:
        pass

    assert main.rec.recover()
    assert main.rec.path != first and not main.rec.write_error and "recording" not in main.rec.errors
    main.rec.write("/a", {"v": "after"})
    main.rec.finish()
    raw = messages(main.rec.path.read_bytes())
    assert [e["data"]["v"] for e in topic(raw, "/a")] == ["after"]
    assert any("after a write error" in e["data"]["event"] for e in topic(raw, "/logger/events"))
    kept = [m for r in main.records(first, []) if isinstance(r, main.Message) for m in [json.loads(r.data)]]
    assert any(m["data"] == {"v": "before"} for m in kept), "what was synced before the failure stays readable"


def test_failing_card() -> None:
    """A card fails the way cards do: writes go to the page cache, and the error comes from fsync. And recovery
    attempts on a card that keeps failing leave no empty files behind."""
    fresh()
    main.rec.write("/a", 1)
    real_fsync, real_sleep = main.os.fsync, main.asyncio.sleep
    main.os.fsync = lambda fd: (_ for _ in ()).throw(OSError(5, "Input/output error"))
    calls = []

    async def sleep(seconds, *args):
        calls.append(seconds)
        if len(calls) > 1:
            raise asyncio.CancelledError
    main.asyncio.sleep = sleep
    try:
        asyncio.run(main.keep_flushed())
    except asyncio.CancelledError:
        pass
    finally:
        main.os.fsync, main.asyncio.sleep = real_fsync, real_sleep
    assert main.rec.write_error and "Input/output" in main.rec.write_error, "an fsync error stops recording visibly"

    before = set(main.LOG_DIR.glob("*.mcap"))
    real_writer = main.Writer
    main.Writer = lambda *a, **k: (_ for _ in ()).throw(OSError(28, "No space left on device"))
    try:
        for _ in range(3):
            assert not main.rec.recover()
    finally:
        main.Writer = real_writer
    assert set(main.LOG_DIR.glob("*.mcap")) == before, "no empty file per failed attempt"
    assert main.rec.recover()
    main.rec.finish()

    # A write error in the middle of New file: one new file from recovery, not a stray one left open.
    fresh()
    main.rec.writer._Writer__stream = Failing()
    files = set(main.LOG_DIR.glob("*.mcap"))
    real_event, main.rec.event = main.rec.event, lambda text: main.rec._failed(OSError(5, "EIO"))
    try:
        main.rec.rotate()
        raise AssertionError("rotate must report the failure")
    except RuntimeError:
        pass
    finally:
        del main.rec.event
    assert set(main.LOG_DIR.glob("*.mcap")) == files, "the new file was removed"
    assert main.rec.recover() and len(set(main.LOG_DIR.glob("*.mcap")) - files) == 1
    main.rec.finish()
    assert real_event


def test_stopping() -> None:
    """Once stopping, New file and recovery do nothing: no file is left without its index."""
    fresh()
    main.rec.finish()
    files = set(main.LOG_DIR.glob("*.mcap"))
    try:
        main.rec.rotate()
        raise AssertionError("rotate after finish must refuse")
    except RuntimeError:
        pass
    assert set(main.LOG_DIR.glob("*.mcap")) == files


def test_stop_and_start() -> None:
    """The emergency stop: the file is finished whole, nothing is written or created until Start, which opens a new
    file. A stopped file can be deleted to free the card, and a shutdown while stopped is clean."""
    fresh()
    main.rec.write("/a", {"v": 1})
    stopped = main.rec.path
    main.rec.stop()
    main.rec.write("/a", {"v": 2})
    files = set(main.LOG_DIR.glob("*.mcap"))
    for refused in (main.rec.rotate, main.rec.stop):
        try:
            refused()
            raise AssertionError(f"{refused.__name__} while stopped must refuse")
        except RuntimeError:
            pass
    main.rec.recover()
    assert set(main.LOG_DIR.glob("*.mcap")) == files, "no file is created while stopped"
    assert main.ingest({"name": "x"}, {"op": "log", "topic": "t", "data": 1})["op"] == "error"
    assert [r["active"] for r in main.recordings() if r["name"] == stopped.name] == [False]
    old = messages(stopped.read_bytes())  # finished, with its summary
    assert [e["data"]["v"] for e in topic(old, "/a")] == [1]
    assert "recording stopped from the page" in [e["data"]["event"] for e in topic(old, "/logger/events")]

    main.rec.restart()
    assert main.rec.path != stopped and not main.rec.stopped
    main.rec.write("/a", {"v": 3})
    try:
        main.rec.restart()
        raise AssertionError("start while recording must refuse")
    except RuntimeError:
        pass
    main.rec.stop()
    main.rec.finish()  # SIGTERM while stopped
    new = messages(main.rec.path.read_bytes())
    assert [e["data"]["v"] for e in topic(new, "/a")] == [3]
    assert any("messages not recorded while stopped" in e["data"]["event"] for e in topic(new, "/logger/events"))


def test_removed_container() -> None:
    """The error of a container that was removed (an extension uninstalled) leaves the page with it."""
    fresh()
    main.rec.errors["docker logs gone"] = "HTTPError 404"
    real_get, real_sleep = main.get_json, main.asyncio.sleep
    main.get_json = lambda path: []

    async def stop(seconds, *args):
        raise asyncio.CancelledError
    main.asyncio.sleep = stop
    try:
        asyncio.run(main.follow_container_logs())
    except asyncio.CancelledError:
        pass
    finally:
        main.get_json, main.asyncio.sleep = real_get, real_sleep
    assert "docker logs gone" not in main.rec.errors
    main.rec.finish()


def test_container_stream_cut() -> None:
    """The proxy cuts a container's log stream mid-line: what came whole is recorded, the cut is not an error, and the
    next stream starts after the last whole line, so the cut one comes again."""
    fresh()
    line = b"2026-11-11T19:00:00.500000000Z hello\n"
    stream = io.BytesIO(bytes([1, 0, 0, 0]) + len(line).to_bytes(4, "big") + line + bytes([1, 0, 0, 0, 0, 0, 0, 40]))

    class Cut:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self, n):
            data = stream.read(n)
            if len(data) < n and n != 8:
                raise main.http.client.IncompleteRead(data)
            return data

    real_open, real_get = main.urllib.request.urlopen, main.get_json
    main.urllib.request.urlopen = lambda url, timeout=None: Cut()
    main.get_json = lambda path: {"Config": {"Tty": False}}
    last_seen = {}
    try:
        main.follow_container({"Id": "x", "Names": ["/c"]}, 0, 0, last_seen)
    finally:
        main.urllib.request.urlopen, main.get_json = real_open, real_get
    assert "docker logs c" not in main.rec.errors
    assert last_seen["c"] == 1794423600_500000000, "the next stream starts after this line"
    main.rec.finish()
    assert [e["data"]["line"] for e in topic(messages(main.rec.path.read_bytes()), "/docker/logs/c")] == ["hello"]


def test_active_download() -> None:
    """Downloading the file being written is not flagged as a problem: it is read to its last byte."""
    fresh()
    main.rec.write("/a", 1)
    main.rec.flush()
    out = io.BytesIO()
    main.export(main.rec.path, out)
    assert "export_problems" not in metadata(out.getvalue())
    main.rec.finish()


def test_cannot_create_files() -> None:
    """A new file can't be created: the current one keeps recording, and the caller gets the error."""
    fresh()
    real_open = open
    main.open = lambda *a, **k: (_ for _ in ()).throw(OSError(30, "Read-only file system"))
    try:
        main.rec.rotate()
        raise AssertionError("rotate must raise")
    except OSError:
        pass
    finally:
        main.open = real_open
        del main.open
    assert main.rec.path.exists() and not main.rec.file.closed
    main.rec.write("/a", {"v": "still recording"})
    main.rec.finish()
    assert topic(messages(main.rec.path.read_bytes()), "/a")[0]["data"] == {"v": "still recording"}

    # At start: the page still comes up and says so, and watch_disk keeps trying.
    main.open = lambda *a, **k: (_ for _ in ()).throw(OSError(30, "Read-only file system"))
    try:
        rec = main.Recorder(main.clock)
        rec.start()
        rec.write("/a", 1)
        assert rec.write_error and rec.dropped >= 1
        assert not rec.recover()
    finally:
        del main.open
    assert rec.recover() and rec.path.exists()
    rec.finish()


def test_topic_limit() -> None:
    """A client inventing topics can't push the file past MCAP's 16-bit channel ids and break it for everyone."""
    fresh()
    limit, main.MAX_CHANNELS = main.MAX_CHANNELS, 100
    try:
        client = {"name": "c"}
        for i in range(200):
            main.ingest(client, {"op": "log", "topic": f"request/{i}", "data": i})
        main.handle_mavlink({"header": {"system_id": 1, "component_id": 1, "sequence": 0},
                             "message": {"type": "HEARTBEAT"}})
        assert len(main.rec.channels) <= 101 and "topics" in main.rec.errors
        finished = main.rec.rotate()
        assert "topics" not in main.rec.errors, "the limit is per file"
        main.rec.finish()
    finally:
        main.MAX_CHANNELS = limit
    raw = messages(finished.read_bytes())
    overflow = topic(raw, "/logger/overflow")
    assert overflow and overflow[0]["data"]["topic"].startswith("/extensions/c/request/")
    assert sum(e["data"]["topic"].startswith("/extensions/c/") for e in overflow) + sum(
        t.startswith("/extensions/c/") for t, _, _ in raw) == 200, "nothing lost"
    assert any(e["data"]["topic"] == "/mavlink/1/1/HEARTBEAT" for e in overflow), "MAVLink still recorded"


def test_flush() -> None:
    """A power cut loses at most the last FLUSH_S seconds: after a flush, the file on the card has every message."""
    fresh()
    for i in range(10):
        main.rec.write("/a", {"i": i})
    assert main.rec.flush() is not None
    copy = Path(tempfile.mkdtemp()) / "cut.mcap"
    shutil.copy(main.rec.path, copy)  # what a power cut would leave
    got = [json.loads(r.data)["data"] for r in main.records(copy, []) if isinstance(r, main.Message)]
    assert [g["i"] for g in got if isinstance(g, dict) and "i" in g] == list(range(10))
    main.rec.finish()


def test_power_cut_download() -> None:
    """A file cut by a power loss still downloads, with the reference it reached and a note of what was cut."""
    fresh()
    for i in range(5):
        main.rec.write("/a", {"i": i})
        main.rec.flush()
    main.rec.sync("autopilot", main.clock.shift + HOUR)
    main.rec.flush()
    main.rec.write("/a", {"i": 5})
    main.rec.flush()
    data = main.rec.path.read_bytes()
    main.rec.finish()
    copy = Path(tempfile.mkdtemp()) / "0001_cut.mcap"
    copy.write_bytes(data[:-20])  # cut in the middle of the last chunk
    out = io.BytesIO()
    real_boot, main.boot_id = main.boot_id, lambda: "another boot"  # the reference must come from the file itself
    try:
        main.export(copy, out)
    finally:
        main.boot_id = real_boot
    got = messages(out.getvalue())
    assert [e["data"]["i"] for e in topic(got, "/a")] == [0, 1, 2, 3, 4]
    assert all(e["time_source"] == "autopilot" for _, _, e in got)
    meta = metadata(out.getvalue())
    assert meta["time"]["source"] == "autopilot" and "export_problems" in meta


def test_recording_times() -> None:
    """Each recording lists its number, start and end in the best time known for it, also after a later sync and for a
    file cut by a power loss; the active one has no end yet."""
    fresh()
    main.rec.write("/a", {"v": 1})
    time.sleep(0.05)
    main.rec.write("/a", {"v": 2})
    finished = main.rec.rotate()
    main.rec.sync("autopilot", main.clock.shift + HOUR)  # after the file closed: same boot, so it applies to it too
    listed = {r["name"]: r for r in settled()}
    old, active = listed[finished.name], listed[main.rec.path.name]
    assert old["number"] == int(finished.name[:4]) and old["synced"] and not old["active"]
    start = main.datetime.fromisoformat(old["start"]).timestamp()
    linux = (main.time.time_ns() - (main.time.monotonic_ns() - main.file_span(finished)["start_mono"])) / 1e9
    assert abs(start - (linux + 3600)) < 1, "the start moved to the autopilot's time, an hour ahead of Linux"
    assert main.datetime.fromisoformat(old["end"]).timestamp() - start >= 0.05
    assert active["active"] and active["start"] and active["end"] is None and active["synced"]

    main.rec.write("/a", {"v": 3})
    main.rec.flush()
    cut = main.LOG_DIR / "9998_cut.mcap"
    cut.write_bytes(main.rec.path.read_bytes())  # no summary, as after a power loss
    entry = next(r for r in settled() if r["name"] == cut.name)
    assert entry["start"] and entry["end"] and entry["synced"]
    cut.unlink()
    main.rec.finish()

    # A file that takes long to read never holds up the list the page asks for every second.
    slow = main.LOG_DIR / "9997_slow.mcap"
    slow.write_bytes(main.rec.path.read_bytes())
    real, main.file_span = main.file_span, lambda path: (time.sleep(1), real(path))[1]
    try:
        started = time.monotonic()
        entry = next(r for r in main.recordings() if r["name"] == slow.name)
        assert time.monotonic() - started < 0.5 and entry.get("pending") and entry["start"] is None, (time.monotonic() - started, entry)
        assert next(r for r in settled() if r["name"] == slow.name)["start"]
    finally:
        main.file_span = real
        slow.unlink()


def settled() -> list:
    """The list once the background reads of file times are done."""
    for _ in range(100):
        listed = main.recordings()
        if not any(r.get("pending") for r in listed):
            return listed
        time.sleep(0.05)
    raise AssertionError("file times never read")


def test_damaged_chunk() -> None:
    """A damaged chunk in a finished file costs only its own messages, and the download says so."""
    fresh()
    for i in range(6):
        main.rec.write("/a", {"i": i, "pad": "x" * 2000})
        main.rec.flush()
    main.rec.finish()
    data = bytearray(main.rec.path.read_bytes())
    chunks = make_reader(io.BytesIO(bytes(data))).get_summary().chunk_indexes
    middle = chunks[len(chunks) // 2]
    at = middle.chunk_start_offset + middle.chunk_length // 2
    data[at:at + 16] = b"\xff" * 16
    damaged = Path(tempfile.mkdtemp()) / "0001_damaged.mcap"
    damaged.write_bytes(bytes(data))
    out = io.BytesIO()
    main.export(damaged, out)
    got = [e["data"]["i"] for e in topic(messages(out.getvalue()), "/a")]
    assert len(got) == 5 and got[-1] == 5, got
    assert "damaged chunk" in json.dumps(metadata(out.getvalue())["export_problems"])


def test_old_file_gets_later_sync() -> None:
    """A file closed before the sync, in the same boot, is re-stamped with the reference reached later."""
    fresh()
    main.rec.write("/a", 1)
    old = main.rec.rotate()
    main.rec.sync("autopilot", main.clock.shift + HOUR)
    out = io.BytesIO()
    main.export(old, out)
    main.rec.finish()
    a = topic(messages(out.getvalue()), "/a")[0]
    assert a["time_source"] == "autopilot" and a["time_synced_externally"]
    assert a["corrected_ns"] == a["mono_ns"] + main.clock.shift


def test_autopilot_time() -> None:
    """The autopilot's time: the least delayed sample wins, one system only, and a set of its clock is followed."""
    fresh()
    truth = main.time.time_ns() - main.time.monotonic_ns() + 5 * HOUR  # what the autopilot says is 5 h ahead

    def system_time(system: int, delay_ns: int, offset: int = 0) -> None:
        unix = main.time.monotonic_ns() + truth + offset - delay_ns
        main.handle_mavlink({"header": {"system_id": system, "component_id": 1, "sequence": 0},
                             "message": {"type": "SYSTEM_TIME", "time_unix_usec": unix // 1000, "time_boot_ms": 1}})

    system_time(1, 900_000_000)  # the first one arrives late, during a backlog
    assert main.clock.source == "autopilot"
    for _ in range(20):
        system_time(1, 5_000_000)
    assert abs(main.clock.shift - truth) < 20_000_000, (main.clock.shift - truth) / 1e6
    syncs = main.clock.syncs
    for _ in range(5):
        system_time(2, 0, offset=30 * 10**9)  # another vehicle, 30 s off
    assert main.clock.syncs == syncs and abs(main.clock.shift - truth) < 20_000_000
    assert "SYSTEM_TIME 2" in main.rec.errors
    system_time(1, 0, offset=HOUR)  # the autopilot's clock was set
    assert abs(main.clock.shift - truth - HOUR) < 20_000_000
    main.rec.finish()


def test_mavlink_gaps() -> None:
    fresh()
    main.mavlink_gaps = 0
    main.mavlink_sequence.clear()
    for sequence in (250, 251, 254, 255, 0, 1, 200):  # lost 252 and 253; wrapping is not a loss, nor is a restart
        main.handle_mavlink({"header": {"system_id": 1, "component_id": 1, "sequence": sequence},
                             "message": {"type": "HEARTBEAT"}})
    for sequence in (9, 12, 15):  # the camera manager's components share one counter: not losses
        main.handle_mavlink({"header": {"system_id": 1, "component_id": 100, "sequence": sequence},
                             "message": {"type": "HEARTBEAT"}})
    assert main.mavlink_gaps == 2, main.mavlink_gaps
    main.rec.finish()


def test_kernel_ring_once() -> None:
    """linux2rest sends the whole ring buffer on every connection: each kernel message is recorded once."""
    fresh()
    main.last_kernel_sequence = -1
    ring = [{"sequence_number": 1, "message": "boot"}, {"sequence_number": 2, "message": "usb"}]
    main.handle_kernel(ring)
    main.handle_kernel(ring + [{"sequence_number": 3, "message": "new"}])  # reconnected
    main.rec.finish()
    assert [e["data"]["message"] for e in topic(messages(main.rec.path.read_bytes()), "/linux/kernel")] == \
        ["boot", "usb", "new"]


def test_invalid_numbers() -> None:
    """NaN from a Python client is recorded as null: no JSON reader accepts NaN, so it would make the message unreadable."""
    fresh()
    client = {"name": "py"}
    main.ingest(client, {"op": "log", "topic": "t", "data": {"v": float("nan"), "w": float("inf"), "ok": 1}})
    main.rec.finish()
    raw = main.rec.path.read_bytes()
    for _, _, envelope in messages(raw):
        json.dumps(envelope, allow_nan=False)
    assert topic(messages(raw), "/extensions/py/t")[0]["data"] == {"v": None, "w": None, "ok": 1}


def test_ingest_limits() -> None:
    fresh()
    early = {"name": "unnamed"}
    assert main.ingest(early, {"op": "log"})["op"] == "error" and "ingest unnamed" in main.rec.errors
    main.ingest(early, {"op": "hello", "name": "sonar"})
    assert "ingest unnamed" not in main.rec.errors, "a refusal before hello doesn't stay under the old name"
    client = {"name": "c"}
    old = main.time.monotonic_ns() - 3600 * 10**9
    assert main.ingest(client, {"op": "log", "topic": "t", "mono_ns": old})["op"] == "error", "an hour old"
    assert "ingest c" in main.rec.errors, "a refused message shows on the page"
    assert main.ingest(client, {"op": "log", "topic": "t", "data": 1}) is None
    assert "ingest c" not in main.rec.errors
    main.rec.finish()
    events = [e["data"]["event"] for e in topic(messages(main.rec.path.read_bytes()), "/logger/events")]
    assert any("refusing" in e for e in events)


async def _supervised() -> None:
    fresh()
    runs = []

    async def flaky() -> None:
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("boom")
        await asyncio.Event().wait()

    sleep = main.asyncio.sleep
    main.asyncio.sleep = lambda s, *a: sleep(0)  # skip the 5 s pause before a restart
    try:
        task = asyncio.create_task(main.supervise("flaky", flaky))
        await sleep(0.05)
        assert "flaky" in main.rec.errors and "boom" in main.rec.errors["flaky"], "a crash shows on the page"
        assert len(runs) == 2 and not task.done(), "and the source runs again"
        task.cancel()
    finally:
        main.asyncio.sleep = sleep
    main.rec.finish()


def test_supervise() -> None:
    asyncio.run(_supervised())


async def _websocket() -> None:
    """A bad message costs only itself, and a link that is connected but silent shows how long it has been silent."""
    fresh()
    sent = asyncio.Event()

    async def server(ws) -> None:
        await ws.send(json.dumps({"header": {"system_id": 1, "component_id": 1, "sequence": 0},
                                  "message": {"type": "HEARTBEAT"}}))
        await ws.send(json.dumps({"unexpected": True}))
        await ws.send("not json")
        await ws.send(json.dumps({"header": {"system_id": 1, "component_id": 1, "sequence": 1},
                                  "message": {"type": "HEARTBEAT"}}))
        sent.set()
        await ws.wait_closed()

    async with websockets.serve(server, "127.0.0.1", 0) as ws_server:
        port = ws_server.sockets[0].getsockname()[1]
        host, main.HOST = main.HOST, f"127.0.0.1:{port}"
        try:
            task = asyncio.create_task(main.websocket("mavlink2rest", "/", main.handle_mavlink))
            await sent.wait()
            await asyncio.sleep(1.2)
            status = main.status()
            assert status["links"]["mavlink2rest"] == "connected"
            assert status["idle_s"]["mavlink2rest"] >= 1, status["idle_s"]
            assert "2 unexpected messages" in status["failing_endpoints"]["mavlink2rest"]
            task.cancel()
        finally:
            main.HOST = host
    main.rec.finish()
    raw = messages(main.rec.path.read_bytes())
    assert len(topic(raw, "/mavlink/1/1/HEARTBEAT")) == 2
    assert len(topic(raw, "/logger/unparsed/mavlink2rest")) == 2
    assert sum("mavlink2rest connected" in e["data"]["event"] for e in topic(raw, "/logger/events")) == 1


def test_websocket() -> None:
    asyncio.run(_websocket())


async def _poll() -> None:
    """Change-only state is written when it changes and in every new file; a failure is written once, and clears."""
    fresh()
    path = "/system-information/platform"
    answers = [b'{"a":1}', b'{"a":1}', OSError("down"), OSError("down"), b'{"a":1}', b'{"a":2}']
    calls = []

    def fetch(p):
        calls.append(p)
        answer = answers[min(len(calls), len(answers)) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    real, main.fetch = main.fetch, fetch
    main.periods[path] = 0.1
    try:
        task = asyncio.create_task(main.poll(path, False))
        while len(calls) < 3:
            await asyncio.sleep(0.02)
        assert path in main.rec.errors
        while len(calls) < 6:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.05)
        assert path not in main.rec.errors
        first = main.rec.rotate()
        await asyncio.sleep(0.3)
        task.cancel()
    finally:
        main.fetch = real
        main.periods[path] = main.DEFAULT_PERIODS[path]
    main.rec.finish()
    written = [e["data"] for e in topic(messages(first.read_bytes()), "/blueos/system-information/platform")]
    assert written == [{"a": 1}, {"error": "OSError: down"}, {"a": 1}, {"a": 2}], written
    again = topic(messages(main.rec.path.read_bytes()), "/blueos/system-information/platform")
    assert again and again[0]["data"] == {"a": 2}, "a new file has the current state from its start"


def test_poll() -> None:
    asyncio.run(_poll())


def test_http() -> None:
    """The API the page uses, and what it must refuse."""
    fresh()
    server = main.ThreadingHTTPServer(("127.0.0.1", 0), main.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    def call(path, method="GET", body=None, content_type="application/json"):
        request = urllib.request.Request(base + path, method=method, data=body,
                                         headers={"Content-Type": content_type} if body is not None else {})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    try:
        code, body = call("/status")
        status = json.loads(body)
        for key in ("file", "messages", "topics", "paused", "stopped", "write_error", "dropped", "links", "idle_s",
                    "mavlink_gaps", "failing_endpoints", "time", "version", "disk", "recordings"):
            assert key in status, key  # what index.html reads
        assert call("/")[0] == 200 and call("/settings")[0] == 200
        assert call("/download/..%2Fsettings.json")[0] == 404
        assert call("/raw/../../etc/passwd")[0] == 404
        active = main.rec.path.name
        assert call(f"/delete/{active}", "POST", b"{}")[0] == 404, "the file being written can't be deleted"
        assert call("/new_file", "POST", b"{}", "text/plain")[0] == 415, "only JSON, so other sites can't post"
        assert call("/settings", "POST", json.dumps({"poll_periods": {CPU: 0.5}}).encode(), "text/plain")[0] == 415
        assert main.periods[CPU] == 1
        assert call("/settings", "POST", b'{"poll_periods": {"/nope": 1}}')[0] == 400
        code, body = call("/new_file", "POST", b"{}")
        assert code == 200 and json.loads(body)["finished"] == active
        assert call(f"/delete/{active}", "POST", b"{}")[0] == 204 and not (main.LOG_DIR / active).exists()
        code, body = call(f"/raw/{main.rec.path.name}")
        assert code == 200 and body.startswith(b"\x89MCAP")
        assert call(f"/download/{main.rec.path.name}")[0] == 200
        with main.exporting:  # one download at a time
            assert call(f"/download/{main.rec.path.name}")[0] == 503
        assert call("/stop", "POST", b"{}", "text/plain")[0] == 415
        stopped = main.rec.path.name
        assert call("/stop", "POST", b"{}")[0] == 200 and json.loads(call("/status")[1])["stopped"]
        assert call("/stop", "POST", b"{}")[0] == 500
        assert call(f"/delete/{stopped}", "POST", b"{}")[0] == 204, "a stopped file can be deleted to free space"
        code, body = call("/start", "POST", b"{}")
        assert code == 200 and json.loads(body)["file"] != stopped and not json.loads(body)["stopped"]
    finally:
        server.shutdown()
        main.rec.finish()


if __name__ == "__main__":
    for name, test in list(globals().items()):
        if name.startswith("test_"):
            test()
            print("ok", name, flush=True)
    print("ok")
