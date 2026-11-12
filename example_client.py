"""Logs into the Powerful and Unofficial Single File Logger for BlueOS from another service or extension.

Usage: python example_client.py [ws://blueos.local:9651]
"""

import asyncio
import json
import sys
import time

import websockets


async def sync(ws) -> tuple[int, int]:
    """Returns (offset, round trip): logger's monotonic ns = this computer's monotonic ns + offset."""
    best = None
    for _ in range(8):  # keep the exchange with the shortest round trip, the least delayed one
        t0 = time.monotonic_ns()
        await ws.send(json.dumps({"op": "sync", "t0": t0}))
        reply = json.loads(await ws.recv())
        t2 = time.monotonic_ns()
        if best is None or t2 - t0 < best[1]:
            best = (reply["mono_ns"] - (t0 + t2) // 2, t2 - t0, reply)
    offset, round_trip, reply = best
    print(f"offset {offset} ns, round trip {round_trip / 1e6:.2f} ms, logger time {reply['time_source']}, "
          f"synchronized externally: {reply['time_synced_externally']}")
    return offset, round_trip


async def main(url: str) -> None:
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"op": "hello", "name": "example-client"}))
        offset, _ = await sync(ws)  # redo it now and then: two monotonic clocks drift apart slowly
        for depth in range(5):
            happened = time.monotonic_ns()  # stamp when it happens, not when it is sent
            await ws.send(json.dumps({"op": "log", "topic": "sonar/depth", "data": {"depth_m": depth},
                                      "mono_ns": happened + offset}))
            await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ws://blueos.local:9651"))
