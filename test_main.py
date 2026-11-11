"""Run with: python test_main.py (needs the packages from requirements.txt)."""

import io
import json
import os
import tempfile
from pathlib import Path

os.environ["LOG_DIR"] = tempfile.mkdtemp()
import main  # noqa: E402
from mcap.reader import make_reader  # noqa: E402

HOUR = 3_600_000_000_000


def messages(data: bytes) -> list:
    reader = make_reader(io.BytesIO(data))
    return [(c.topic, m.log_time, json.loads(m.data)) for _, c, m in reader.iter_messages(log_time_order=False)]


def test() -> None:
    assert main.redact({"user": "a", "Password": "x", "nested": [{"api_key": 1, "ok": 2}]}) == \
        {"user": "a", "Password": "<redacted>", "nested": [{"api_key": "<redacted>", "ok": 2}]}
    assert main.parse_docker_line("2026-11-11T19:00:00.5Z hello world") == ("hello world", 1794423600_500000000)

    main.clock.source = "linux_unsynced"
    main.rec = main.Recorder(main.clock)
    main.rec.write("/a", {"v": 1})
    assert main.rec.sync("autopilot", main.clock.shift + HOUR)  # the Linux clock was an hour behind
    main.rec.write("/a", {"v": 2})
    assert not main.rec.sync("linux_unsynced", main.clock.shift - HOUR)  # a worse reference never wins

    # A log line split across two reads is written once, whole.
    log = Path(os.environ["LOG_DIR"]) / "svc" / "service.log"
    log.parent.mkdir()
    log.write_bytes(b"first\nsec")
    state = [0, b""]
    main.read_new_lines(log, state, backfill=True)
    with log.open("ab") as f:
        f.write(b"ond\n")
    main.read_new_lines(log, state, backfill=False)

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
    a = [e for topic, _, e in raw if topic == "/a"]
    assert [e["time_synced_externally"] for e in a] == [False, True]
    assert a[1]["corrected_ns"] - a[1]["mono_ns"] - (a[0]["corrected_ns"] - a[0]["mono_ns"]) == HOUR
    # The recorded log_time never jumps when the reference changes: it is mono_ns plus one constant.
    assert len({t - e["mono_ns"] for _, t, e in raw}) == 1
    assert [e["data"]["line"] for topic, _, e in raw if topic == "/blueos/logs/svc"] == ["first", "second"]

    ext = [e for topic, _, e in raw if topic == "/extensions/My_Ext/time/sync"]
    assert len(ext) == 1 and ext[0]["mono_ns"] == happened and ext[0]["received_mono_ns"] > happened
    assert sum(topic == "/time/sync" for topic, _, _ in raw) == 1, "a client can't write into /time"

    out = io.BytesIO()
    main.export(main.rec.path, out)
    synced = messages(out.getvalue())
    assert len(synced) == len(raw)
    for _, log_time, envelope in synced:  # every message, also the one before the sync, gets the final reference
        assert envelope["time_synced_externally"] and envelope["time_source"] == "autopilot"
        assert log_time == envelope["corrected_ns"] == envelope["mono_ns"] + main.clock.shift

    # A new file on request: the old one is closed whole, the new one is synchronized from its first message.
    main.rec = main.Recorder(main.clock)
    main.rec.start()
    main.rec.write("/a", {"v": 3})
    generation = main.rec.generation
    finished = main.rec.rotate()
    main.rec.write("/a", {"v": 4})
    main.rec.finish()
    assert main.rec.generation == generation + 1 and main.rec.path != finished
    old = messages(finished.read_bytes())  # readable as a finished file, with its summary
    assert [e["data"]["v"] for topic, _, e in old if topic == "/a"] == [3]
    new = messages(main.rec.path.read_bytes())
    assert new[0][0] == "/time/sync" and new[0][2]["data"]["file_start"] and new[0][2]["time_synced_externally"]
    assert [e["data"]["v"] for topic, _, e in new if topic == "/a"] == [4]

    # Poll periods: checked as a whole, saved, applied in a new file that records them.
    main.rec = main.Recorder(main.clock)
    main.rec.start()
    cpu = "/system-information/system/cpu"
    before = main.rec.path
    assert main.apply_periods({cpu: 0.5, "/nope": 1})  # an error, and nothing applied
    assert main.apply_periods({cpu: 0}) and main.apply_periods({cpu: True}) and main.apply_periods({cpu: "1"})
    assert main.periods[cpu] == 1 and main.rec.path == before
    assert main.apply_periods({cpu: 0.5}) is None
    assert main.periods[cpu] == 0.5 and main.rec.path != before
    assert json.loads(main.SETTINGS.read_text()) == {"poll_periods": {cpu: 0.5}}
    main.periods[cpu] = 1
    main.load_settings()
    assert main.periods[cpu] == 0.5
    main.rec.finish()
    reader = make_reader(io.BytesIO(main.rec.path.read_bytes()))
    assert {m.name: m.metadata for m in reader.iter_metadata()}["poll_periods_s"][cpu] == "0.5"
    print("ok")


if __name__ == "__main__":
    test()
