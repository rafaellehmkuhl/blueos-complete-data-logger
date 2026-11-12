"""End to end: a fake BlueOS (REST polls, the MAVLink and kernel websockets, the Docker proxy), the real main.py as a
process, then the MCAP files read back. Run with: python test_e2e.py (needs the packages from requirements.txt).

What it checks: every kind of source lands in the file; a service outage is recorded and clears; after a BlueOS core
restart MAVLink is recorded again within seconds; New file over HTTP; and a stop closes the file whole well within
Docker's 10 s, even with a client that stopped answering."""

import asyncio
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import websockets
from mcap.reader import make_reader
from websockets.datastructures import Headers
from websockets.http11 import Response

FAKE, HTTP, INGEST = 18080, 19650, 19651
MAIN = Path(__file__).parent / "main.py"
state = {"cpu_fails": False}
CONTAINER = {"Id": "abc123", "Names": ["/my-extension"]}
# (Unix seconds, stream, text), stamped as Docker does when the logger asks for timestamps=1.
CONTAINER_LOG = [(time.time() - 2.5, 1, "container says hi"), (time.time() - 1, 2, "an error")]


def docker_stamp(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f000Z")


def frame(stream: int, text: str) -> bytes:
    """Docker's multiplexed log stream: 8-byte header, then the payload."""
    payload = text.encode()
    return bytes([stream, 0, 0, 0]) + struct.pack(">I", len(payload)) + payload


async def process_request(conn, request):
    path = request.path
    if path.startswith(("/mavlink2rest/ws", "/system-information/ws")):
        return None  # websocket upgrade
    if path == "/system-information/system/cpu":
        if state["cpu_fails"]:
            return Response(503, "Service Unavailable", Headers(), b"down")
        body = json.dumps([{"usage": 12.5}]).encode()
    elif path == "/docker/containers/json":
        body = json.dumps([CONTAINER]).encode()
    elif path == "/docker/containers/abc123/json":
        body = json.dumps({"Config": {"Tty": False}}).encode()
    elif path.startswith("/docker/containers/abc123/logs"):
        # Docker returns only the lines after `since`; the logger asks from the last line it got.
        since = float(parse_qs(urlparse(path).query)["since"][0])
        state["log_requests"] = state.get("log_requests", 0) + 1
        body = b"".join(frame(stream, f"{docker_stamp(at)} {text}\n") for at, stream, text in CONTAINER_LOG
                        if at > since)
    elif path == "/beacon/v1.0/services":
        body = json.dumps({"wifi": {"password": "hunter2"}}).encode()
    else:
        return Response(404, "Not Found", Headers(), b"")
    return Response(200, "OK", Headers([("Content-Type", "application/json")]), body)


async def ws_handler(ws):
    if ws.request.path.startswith("/mavlink2rest"):
        try:
            for i in range(10**9):
                await ws.send(json.dumps({"header": {"system_id": 1, "component_id": 1, "sequence": i % 256},
                                          "message": {"type": "HEARTBEAT", "custom_mode": i}}))
                if i % 10 == 0:
                    unix = (time.time_ns() + 3600 * 10**9) // 1000
                    await ws.send(json.dumps({"header": {"system_id": 1, "component_id": 1, "sequence": i % 256},
                                              "message": {"type": "SYSTEM_TIME", "time_unix_usec": unix,
                                                          "time_boot_ms": 1}}))
                await asyncio.sleep(0.02)
        except websockets.ConnectionClosed:
            pass
    else:
        await ws.send(json.dumps([{"sequence_number": 1, "message": "boot"}, {"sequence_number": 2, "message": "usb"}]))
        await ws.wait_closed()


def fake_blueos():
    return websockets.serve(ws_handler, "127.0.0.1", FAKE, process_request=process_request)


def get(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{HTTP}{path}", timeout=5) as r:
        return json.loads(r.read())


def post(path):
    request = urllib.request.Request(f"http://127.0.0.1:{HTTP}{path}", method="POST", data=b"{}",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as r:
        return json.loads(r.read())


def frozen_client() -> socket.socket:
    """An ingest client that connects and then never answers again, like a topside that froze."""
    sock = socket.create_connection(("127.0.0.1", INGEST))
    sock.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                 b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
    sock.recv(4096)
    return sock


async def until(check, timeout: float, what: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if result := await asyncio.to_thread(check):
                return result
        except Exception:
            pass
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


async def main():
    logs, svc = Path(tempfile.mkdtemp()), Path(tempfile.mkdtemp())
    (svc / "beacon").mkdir()
    (svc / "beacon" / "beacon.log").write_text("old line\n")
    env = {**os.environ, "BLUEOS_HOST": f"127.0.0.1:{FAKE}", "PORT": str(HTTP), "INGEST_PORT": str(INGEST),
           "LOG_DIR": str(logs), "SERVICE_LOGS": str(svc), "MIN_FREE_MB": "1", "MAX_DISK_USE_PERCENT": "100"}
    server = await fake_blueos()
    proc = subprocess.Popen([sys.executable, "-u", str(MAIN)], env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    try:
        await until(lambda: get("/status")["links"].get("mavlink2rest") == "connected", 10, "MAVLink")
        async with websockets.connect(f"ws://127.0.0.1:{INGEST}") as ws:
            await ws.send(json.dumps({"op": "hello", "name": "e2e"}))
            await ws.send(json.dumps({"op": "log", "topic": "depth", "data": {"m": 3}}))
            await ws.send("not json")
            assert json.loads(await ws.recv())["op"] == "error"
        with (svc / "beacon" / "beacon.log").open("a") as f:
            f.write("new line\n")

        state["cpu_fails"] = True
        await until(lambda: "/system-information/system/cpu" in get("/status")["failing_endpoints"], 5, "outage shown")
        state["cpu_fails"] = False
        await until(lambda: "/system-information/system/cpu" not in get("/status")["failing_endpoints"], 5,
                    "outage cleared")

        # A BlueOS core restart: everything goes away for a while, then comes back.
        server.close()
        await server.wait_closed()
        await until(lambda: get("/status")["links"]["mavlink2rest"] == "reconnecting", 5, "the link to drop")
        await asyncio.sleep(8)
        server = await fake_blueos()
        back = time.monotonic()
        await until(lambda: get("/status")["links"]["mavlink2rest"] == "connected", 10, "MAVLink after a restart")
        assert time.monotonic() - back < 4, f"reconnected after {time.monotonic() - back:.1f} s"

        # The container's log stream ended; the logger follows it again from where it was.
        await until(lambda: state.get("log_requests", 0) >= 2, 15, "the container logs to be followed again")
        first = (await asyncio.to_thread(post, "/new_file"))["finished"]
        await asyncio.sleep(2)
        frozen = frozen_client()
        stopped = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        out = (await asyncio.to_thread(proc.communicate, timeout=20))[0].decode()
        took = time.monotonic() - stopped
        assert proc.returncode == 0, out[-2000:]
        assert took < 5, f"stopping took {took:.1f} s, Docker kills at 10 s"
        frozen.close()
    finally:
        if proc.poll() is None:
            proc.kill()
        server.close()

    files = sorted(logs.glob("*.mcap"))
    assert len(files) == 2 and files[0].name == first, files
    topics = {}
    for f in files:  # both finished: indexed reads must work
        with f.open("rb") as file:
            for _, ch, m in make_reader(file).iter_messages():
                topics.setdefault(ch.topic, []).append(json.loads(m.data))
    for t in ["/mavlink/1/1/HEARTBEAT", "/linux/kernel", "/blueos/logs/beacon", "/extensions/e2e/depth",
              "/blueos/system-information/system/cpu", "/time/sync", "/logger/events", "/docker/logs/my-extension"]:
        assert t in topics, (t, sorted(topics))
    kernel = [e["data"]["message"] for e in topics["/linux/kernel"]]
    assert kernel.count("boot") == 1, "the kernel ring buffer is recorded once, also across reconnects"
    assert any("error" in e["data"] for e in topics["/blueos/system-information/system/cpu"]), "outage recorded"
    assert topics["/blueos/beacon/v1.0/services"][0]["data"]["wifi"]["password"] == "<redacted>"
    assert any(e["data"]["source"] == "autopilot" for e in topics["/time/sync"])
    lines = [(e["data"]["line"], e["data"]["stream"]) for e in topics["/docker/logs/my-extension"]]
    assert ("container says hi", "stdout") in lines and ("an error", "stderr") in lines, lines
    assert state["log_requests"] >= 2, "the log stream ended and was followed again"
    assert lines.count(("container says hi", "stdout")) == 1, "a restarted log stream continues, not repeats"
    events = [e["data"]["event"] for e in topics["/logger/events"]]
    assert "recording stopped" in events and any("mavlink2rest disconnected" in e for e in events)
    print("e2e ok:", {f.name: f.stat().st_size for f in files}, len(topics), "topics")


asyncio.run(main())
