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
    main.rec.finish()

    raw = messages(main.rec.path.read_bytes())
    a = [e for topic, _, e in raw if topic == "/a"]
    assert [e["time_synced_externally"] for e in a] == [False, True]
    assert a[1]["corrected_ns"] - a[1]["mono_ns"] - (a[0]["corrected_ns"] - a[0]["mono_ns"]) == HOUR
    log_times = [t for _, t, _ in raw]
    assert log_times == sorted(log_times), "the recorded file must never jump back"
    assert [e["data"]["line"] for topic, _, e in raw if topic == "/blueos/logs/svc"] == ["first", "second"]

    out = io.BytesIO()
    main.export(main.rec.path, out)
    synced = messages(out.getvalue())
    assert len(synced) == len(raw)
    for _, log_time, envelope in synced:  # every message, also the one before the sync, gets the final reference
        assert envelope["time_synced_externally"] and envelope["time_source"] == "autopilot"
        assert log_time == envelope["corrected_ns"] == envelope["mono_ns"] + main.clock.shift
    print("ok")


if __name__ == "__main__":
    test()
