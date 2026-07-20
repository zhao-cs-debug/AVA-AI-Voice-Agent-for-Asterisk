import asyncio
import unittest

from aiohttp import web

from strategy_v1 import (
    StrategyV1Error,
    StrategyV1SessionManager,
    normalize_strategy_tts_text,
)


def runtime_config() -> dict:
    return {
        "enabled": True,
        "mode": "real",
        "network": {"external_id": "network-1"},
        "template": {"version_id": 1, "version": 1},
        "settings": {
            "ai": {"title": "项目对接", "gender": "女", "background": "负责项目对接"},
            "human": {
                "title": "客户",
                "gender": "",
                "background_template": "客户 {{客户姓名}}，来自 {公司名}。",
            },
        },
        "timeouts": {
            "session_setup_seconds": 2,
            "first_response_seconds": 1,
            "second_response_grace_seconds": 0.1,
        },
        "config_hash": "hash-1",
    }


class StrategyTTSNormalizationTests(unittest.TestCase):
    def test_trailing_ellipsis_is_replaced_with_chinese_comma(self) -> None:
        cases = {
            "稍等...": "稍等，",
            "稍等……": "稍等，",
            "稍等…": "稍等，",
            "稍等……”": "稍等，”",
        }

        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(normalize_strategy_tts_text(source), expected)

    def test_non_trailing_ellipsis_is_preserved(self) -> None:
        self.assertEqual(normalize_strategy_tts_text("稍等…我查一下。"), "稍等…我查一下。")
        self.assertEqual(normalize_strategy_tts_text("已经处理完成。"), "已经处理完成。")


class StrategyV1ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.create_count = 0
        self.settings_payload = None
        self.events = []
        self.received_inputs = []
        app = web.Application()
        app.router.add_post("/api/v1/external/sessions", self.create_session)
        app.router.add_put("/api/v1/external/sessions/{session_id}/settings", self.put_settings)
        app.router.add_get("/api/v1/external/sessions/{session_id}/ws", self.websocket)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]

        async def event_callback(call_id, event):
            self.events.append((call_id, event))

        self.manager = StrategyV1SessionManager(
            base_url=f"http://127.0.0.1:{port}",
            api_key="test-key",
            event_callback=event_callback,
        )

    async def asyncTearDown(self) -> None:
        await self.manager.end("call-1")
        await self.runner.cleanup()

    async def create_session(self, request: web.Request) -> web.Response:
        self.assertEqual(request.headers.get("Authorization"), "Bearer test-key")
        self.create_count += 1
        return web.json_response({
            "session_id": "session-1",
            "websocket_path": "/api/v1/external/sessions/session-1/ws",
        })

    async def put_settings(self, request: web.Request) -> web.Response:
        self.settings_payload = await request.json()
        return web.json_response({
            "settings_confirmed": True,
            "websocket_path": "/api/v1/external/sessions/session-1/ws",
        })

    async def websocket(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json({"type": "session.preparing"})
        await ws.send_json({"type": "session.ready"})
        started = await ws.receive_json()
        self.assertEqual(started, {"type": "session.start"})
        await ws.send_json({"type": "session.started"})
        turn_number = 0
        async for message in ws:
            if message.type != web.WSMsgType.TEXT:
                continue
            payload = message.json()
            if payload.get("type") == "session.end":
                await ws.send_json({"type": "session.ended"})
                break
            if payload.get("type") != "input.text":
                continue
            self.received_inputs.append(payload)
            text = str(payload.get("text") or "")
            if text == "orphan-second-before-first":
                await ws.send_json({"type": "response[1]", "text": "stale second response"})
                await ws.send_json({"type": "response[0]", "text": "current first response"})
                continue
            if text == "触发超时":
                await asyncio.sleep(1.2)
                continue
            if text == "触发空回复":
                await ws.send_json({"type": "response[0]", "text": ""})
                continue
            if text == "触发断线":
                await ws.close(code=1011, message=b"test disconnect")
                break
            turn_number += 1
            await ws.send_json({"type": "input.accepted"})
            if turn_number == 3:
                await asyncio.sleep(0.05)
            await ws.send_json({"type": "response[0]", "text": f"首段{turn_number}"})
            if turn_number == 1:
                await ws.send_json({"type": "response[1]", "text": "第二段"})
        return ws

    async def test_persistent_session_two_segments_optional_second_and_barge_in(self) -> None:
        first = await self.manager.start(
            "call-1",
            runtime_config(),
            {"客户姓名": "张三", "公司名": "示例公司"},
        )
        second = await self.manager.start("call-1", runtime_config(), {})
        self.assertIs(first, second)
        self.assertEqual(self.create_count, 1)
        self.assertEqual(
            set(self.settings_payload),
            {"confirm", "network", "ai", "human"},
        )
        self.assertEqual(
            self.settings_payload["network"],
            {"mode": "existing", "id": "network-1"},
        )
        self.assertEqual(self.settings_payload["human"]["background"], "客户 张三，来自 示例公司。")

        first_segments = [item async for item in self.manager.stream_turn("call-1", "你好", turn_id="turn-1")]
        self.assertEqual([item["text"] for item in first_segments], ["首段1", "第二段"])

        second_segments = [item async for item in self.manager.stream_turn("call-1", "可以介绍下吗", turn_id="turn-2")]
        self.assertEqual([item["text"] for item in second_segments], ["首段2"])

        async def collect_third():
            return [item async for item in self.manager.stream_turn("call-1", "等一下", turn_id="turn-3")]

        task = asyncio.create_task(collect_third())
        await asyncio.sleep(0.01)
        self.assertEqual(self.manager.mark_barge_in("call-1"), "turn-3")
        self.assertEqual(await task, [])
        self.assertTrue(self.manager.is_turn_stale("call-1", "turn-3"))
        self.manager.clear_stale_turn("call-1", "turn-3")
        self.assertFalse(self.manager.is_turn_stale("call-1", "turn-3"))
        discarded = [event for _, event in self.events if event["event_type"] == "barge_in_discarded"]
        self.assertTrue(any(event.get("turn_id") == "turn-3" for event in discarded))

    async def test_hidden_opening_is_sent_once_without_customer_turn_event(self) -> None:
        await self.manager.start("call-1", runtime_config(), {})

        first = [
            item
            async for item in self.manager.stream_opening(
                "call-1",
                "喂，你好？",
                turn_id="opening-call-1",
            )
        ]
        second = [item async for item in self.manager.stream_opening("call-1", "喂，你好？")]

        self.assertEqual([item["text"] for item in first], ["首段1", "第二段"])
        self.assertEqual(second, [])
        self.assertEqual(
            self.received_inputs,
            [{"type": "input.text", "turn_id": "opening-call-1", "text": "喂，你好？"}],
        )
        opening_customer_events = [
            event
            for _, event in self.events
            if event["event_type"] == "turn_started" and event.get("turn_id") == "opening-call-1"
        ]
        self.assertEqual(opening_customer_events, [])

    async def test_first_response_timeout_is_typed(self) -> None:
        await self.manager.start("call-1", runtime_config(), {})
        with self.assertRaises(StrategyV1Error) as raised:
            _ = [item async for item in self.manager.stream_turn("call-1", "触发超时")]
        self.assertEqual(raised.exception.code, "first_response_timeout")

    async def test_empty_first_response_is_rejected(self) -> None:
        await self.manager.start("call-1", runtime_config(), {})
        with self.assertRaises(StrategyV1Error) as raised:
            _ = [item async for item in self.manager.stream_turn("call-1", "触发空回复")]
        self.assertEqual(raised.exception.code, "empty_response")

    async def test_disconnect_is_typed(self) -> None:
        await self.manager.start("call-1", runtime_config(), {})
        with self.assertRaises(StrategyV1Error) as raised:
            _ = [item async for item in self.manager.stream_turn("call-1", "触发断线")]
        self.assertEqual(raised.exception.code, "connection_closed")

    async def test_orphan_second_segment_is_discarded_before_current_first_response(self) -> None:
        await self.manager.start("call-1", runtime_config(), {})

        segments = [
            item
            async for item in self.manager.stream_turn(
                "call-1",
                "orphan-second-before-first",
                turn_id="turn-after-barge-in",
            )
        ]

        self.assertEqual([item["text"] for item in segments], ["current first response"])
        discarded = [event for _, event in self.events if event["event_type"] == "barge_in_discarded"]
        self.assertTrue(any(event.get("reason") == "orphan_second_segment" for event in discarded))


if __name__ == "__main__":
    unittest.main()
