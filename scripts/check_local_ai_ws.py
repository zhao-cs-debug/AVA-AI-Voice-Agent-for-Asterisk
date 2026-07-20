import asyncio
import json
import os

import websockets


async def main() -> None:
    url = os.getenv("CHECK_WS_URL", "ws://local.zaiguwang.com:28765")
    token = str(os.getenv("LOCAL_WS_AUTH_TOKEN") or "").strip()
    async with websockets.connect(url, open_timeout=8, ping_interval=None, max_size=None) as websocket:
        if token:
            await websocket.send(json.dumps({"type": "auth", "auth_token": token}))
            auth = json.loads(await asyncio.wait_for(websocket.recv(), timeout=8))
            if auth.get("status") != "ok":
                raise RuntimeError("authentication failed")
        await websocket.send(json.dumps({"type": "status"}))
        status = json.loads(await asyncio.wait_for(websocket.recv(), timeout=8))
        if status.get("status") != "ok":
            raise RuntimeError("status request failed")
        print("local_ai_websocket_ok")


asyncio.run(main())
