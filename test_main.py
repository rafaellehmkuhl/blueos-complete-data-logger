"""Run with: python test_main.py (needs the packages from requirements.txt)."""

import json
import os
import tempfile
from pathlib import Path

os.environ["LOG_DIR"] = tempfile.mkdtemp()
import main  # noqa: E402
from mcap.reader import make_reader  # noqa: E402


def test() -> None:
    assert main.redact({"user": "a", "Password": "x", "nested": [{"api_key": 1, "ok": 2}]}) == \
        {"user": "a", "Password": "<redacted>", "nested": [{"api_key": "<redacted>", "ok": 2}]}

    main.rec = main.Recorder(main.clock)
    main.rec.write("/a", {"v": 1})
    main.rec.sync("autopilot_system_time", main.clock.shift + 3_600_000_000_000)  # clock jumps one hour
    main.rec.write("/a", {"v": 2})

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

    with main.rec.path.open("rb") as f:
        reader = make_reader(f)
        messages = [(c.topic, m.log_time, json.loads(m.data)) for _, c, m in reader.iter_messages(log_time_order=False)]
        metadata = {m.name: m.metadata for m in reader.iter_metadata()}
    a = [(t, e) for topic, t, e in messages if topic == "/a"]
    for log_time, envelope in a:
        assert log_time == envelope["corrected_ns"]
    assert a[0][1]["corrected_ns"] - a[0][1]["mono_ns"] == a[1][1]["corrected_ns"] - a[1][1]["mono_ns"] - 3_600_000_000_000
    sync = next(e["data"] for topic, _, e in messages if topic == "/time/sync")
    assert sync["source"] == "autopilot_system_time" and sync["step_ns"] == 3_600_000_000_000
    assert [e["data"]["line"] for topic, _, e in messages if topic == "/blueos/logs/svc"] == ["first", "second"]
    assert int(metadata["time_sync"]["final_shift_ns"]) == main.clock.shift
    print("ok")


if __name__ == "__main__":
    test()
