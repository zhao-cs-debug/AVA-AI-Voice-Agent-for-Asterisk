import asyncio
import time
from pathlib import Path

import yaml

from src.strategy_v1 import StrategyV1SessionManager


async def main() -> None:
    context = yaml.safe_load(Path("/app/config/contexts/cenani_agent_4.yaml").read_text(encoding="utf-8"))
    runtime = context["strategy_runtime"]
    events = []

    async def record(_call_id, event):
        events.append(event)

    manager = StrategyV1SessionManager(event_callback=record)
    started = time.perf_counter()
    await manager.start("strategy-smoke", runtime, {"客户姓名": "测试客户", "公司名": "测试公司"})
    segments = [
        item
        async for item in manager.stream_turn(
            "strategy-smoke",
            "你好，请问你是哪边的？",
            turn_id="smoke-turn-1",
        )
    ]
    await manager.end("strategy-smoke")
    if not segments or not str(segments[0].get("text") or "").strip():
        raise RuntimeError("strategy service returned no response")
    print({
        "success": True,
        "segments": len(segments),
        "first_response_ms": segments[0].get("latency_ms"),
        "total_ms": int((time.perf_counter() - started) * 1000),
        "event_types": [event.get("event_type") for event in events],
    })


asyncio.run(main())
