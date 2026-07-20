from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional, Set
from urllib.parse import urlparse

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed


class StrategyV1Error(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


EventCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]

_TRAILING_ELLIPSIS_RE = re.compile(
    r"(?:\.{2,}|…+)(?P<closers>[\"'”’」』）》】]*)\s*$"
)


def normalize_strategy_tts_text(text: str) -> str:
    """Make unfinished strategy segments safe for speech synthesis."""
    normalized = str(text or "").strip()
    return _TRAILING_ELLIPSIS_RE.sub(
        lambda match: f"，{match.group('closers')}",
        normalized,
    )


@dataclass
class StrategyTurn:
    turn_id: str
    started_at: float
    stale: bool = False


@dataclass
class StrategySession:
    call_id: str
    runtime: Dict[str, Any]
    external_session_id: str
    websocket: Any
    turn_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_turn: Optional[StrategyTurn] = None
    expired_turn_id: Optional[str] = None
    stale_turn_ids: Set[str] = field(default_factory=set)
    opening_sent: bool = False
    closed: bool = False


class StrategyV1SessionManager:
    """One strict V1 strategy WebSocket per AVA call."""

    def __init__(
        self,
        *,
        event_callback: Optional[EventCallback] = None,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> None:
        self.base_url = str(
            base_url or os.getenv("STRATEGY_NETWORK_API_BASE_URL") or "https://demo.shiwentech.com"
        ).strip().rstrip("/")
        self.api_key = str(api_key if api_key is not None else os.getenv("STRATEGY_NETWORK_API_KEY") or "").strip()
        self.event_callback = event_callback
        self.sessions: Dict[str, StrategySession] = {}
        self._sessions_lock = asyncio.Lock()

    async def _emit(self, call_id: str, event_type: str, **payload: Any) -> None:
        if not self.event_callback:
            return
        event = {
            "event_type": event_type,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            **{key: value for key, value in payload.items() if value is not None},
        }
        await self.event_callback(call_id, event)

    @staticmethod
    def _render_background(template: str, variables: Dict[str, Any]) -> str:
        values = {str(key): "" if value is None else str(value) for key, value in (variables or {}).items()}

        def replace_double(match: re.Match[str]) -> str:
            return values.get(match.group(1).strip(), "")

        def replace_single(match: re.Match[str]) -> str:
            return values.get(match.group(1).strip(), "")

        rendered = re.sub(r"\{\{\s*([^{}]+?)\s*\}\}", replace_double, str(template or ""))
        rendered = re.sub(r"\{\s*([^{}]+?)\s*\}", replace_single, rendered)
        rendered = re.sub(r"[ \t]+", " ", rendered)
        rendered = re.sub(r" *\n *", "\n", rendered)
        rendered = re.sub(r"\n{3,}", "\n\n", rendered)
        return rendered.strip()

    def _settings_payload(self, runtime: Dict[str, Any], variables: Dict[str, Any]) -> Dict[str, Any]:
        network = runtime.get("network") if isinstance(runtime.get("network"), dict) else {}
        settings = runtime.get("settings") if isinstance(runtime.get("settings"), dict) else {}
        ai = dict(settings.get("ai") or {}) if isinstance(settings.get("ai"), dict) else {}
        human = dict(settings.get("human") or {}) if isinstance(settings.get("human"), dict) else {}
        external_id = str(network.get("external_id") or "").strip()
        if not external_id:
            raise StrategyV1Error("network_missing", "策略模板缺少外部网络 ID")
        if not str(ai.get("title") or "").strip() or not str(ai.get("background") or "").strip():
            raise StrategyV1Error("ai_settings_invalid", "策略模板缺少 AI 称谓或背景")
        background_template = str(human.pop("background_template", human.get("background") or ""))
        human["background"] = self._render_background(background_template, variables)
        return {
            "confirm": True,
            "network": {"mode": "existing", "id": external_id},
            "ai": {
                "title": str(ai.get("title") or "").strip(),
                "gender": str(ai.get("gender") or "").strip(),
                "background": str(ai.get("background") or "").strip(),
            },
            "human": {
                "title": str(human.get("title") or "").strip(),
                "gender": str(human.get("gender") or "").strip(),
                "background": str(human.get("background") or "").strip(),
            },
        }

    def _headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise StrategyV1Error("api_key_missing", "AVA 服务器未配置策略服务 API Key")
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    async def _http_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        path: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        async with session.request(method, f"{self.base_url}{path}", json=payload) as response:
            raw = await response.text()
            try:
                data = json.loads(raw) if raw else {}
            except json.JSONDecodeError as exc:
                raise StrategyV1Error("invalid_http_response", "策略服务返回了无效 JSON") from exc
            if response.status >= 400:
                message = str(data.get("message") or data.get("detail") or f"HTTP {response.status}")
                raise StrategyV1Error("http_error", message[:1000])
            if not isinstance(data, dict):
                raise StrategyV1Error("invalid_http_response", "策略服务返回了无效响应")
            return data

    async def start(
        self,
        call_id: str,
        runtime: Dict[str, Any],
        variables: Optional[Dict[str, Any]] = None,
    ) -> StrategySession:
        async with self._sessions_lock:
            existing = self.sessions.get(call_id)
            if existing and not existing.closed:
                return existing
        timeout_cfg = runtime.get("timeouts") if isinstance(runtime.get("timeouts"), dict) else {}
        timeout = float(timeout_cfg.get("session_setup_seconds") or 15)
        try:
            state = await asyncio.wait_for(
                self._start(call_id, runtime, variables or {}),
                timeout=max(1.0, timeout),
            )
        except asyncio.TimeoutError as exc:
            raise StrategyV1Error("session_setup_timeout", "策略会话建立超时") from exc
        async with self._sessions_lock:
            self.sessions[call_id] = state
        await self._emit(
            call_id,
            "session_started",
            external_session_id=state.external_session_id,
            template=runtime.get("template"),
            network=runtime.get("network"),
            config_hash=runtime.get("config_hash"),
        )
        return state

    async def _start(
        self,
        call_id: str,
        runtime: Dict[str, Any],
        variables: Dict[str, Any],
    ) -> StrategySession:
        parsed = urlparse(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise StrategyV1Error("base_url_invalid", "策略服务地址无效")
        headers = self._headers()
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as http:
            created = await self._http_json(http, "POST", "/api/v1/external/sessions", {})
            session_id = str(created.get("session_id") or "").strip()
            websocket_path = str(created.get("websocket_path") or "").strip()
            if not session_id or not websocket_path:
                raise StrategyV1Error("session_response_invalid", "创建会话响应缺少必要字段")
            confirmed = await self._http_json(
                http,
                "PUT",
                f"/api/v1/external/sessions/{session_id}/settings",
                self._settings_payload(runtime, variables),
            )
            websocket_path = str(confirmed.get("websocket_path") or websocket_path).strip()
            if not confirmed.get("settings_confirmed") or not websocket_path.startswith("/api/v1/external/sessions/"):
                raise StrategyV1Error("settings_not_confirmed", "策略服务没有确认会话设置")

        websocket_url = f"{'wss' if parsed.scheme == 'https' else 'ws'}://{parsed.netloc}{websocket_path}"
        websocket = await websockets.connect(
            websocket_url,
            additional_headers={"Authorization": f"Bearer {self.api_key}"},
            open_timeout=15,
            ping_interval=20,
            ping_timeout=20,
            max_size=4 * 1024 * 1024,
        )
        try:
            while True:
                event = await self._receive_event(websocket)
                event_type = str(event.get("type") or "")
                if event_type == "session.ready":
                    await websocket.send(json.dumps({"type": "session.start"}))
                elif event_type == "session.started":
                    break
                elif event_type == "error":
                    raise StrategyV1Error("session_start_failed", self._event_error(event))
        except Exception:
            await websocket.close()
            raise
        return StrategySession(
            call_id=call_id,
            runtime=runtime,
            external_session_id=session_id,
            websocket=websocket,
        )

    @staticmethod
    async def _receive_event(websocket: Any) -> Dict[str, Any]:
        try:
            raw = await websocket.recv()
        except ConnectionClosed as exc:
            raise StrategyV1Error("connection_closed", "策略服务连接已断开") from exc
        try:
            event = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StrategyV1Error("invalid_ws_event", "策略服务返回了无效事件") from exc
        if not isinstance(event, dict):
            raise StrategyV1Error("invalid_ws_event", "策略服务返回了无效事件")
        return event

    @staticmethod
    def _event_error(event: Dict[str, Any]) -> str:
        return str(event.get("message") or event.get("code") or "策略服务返回错误")[:1000]

    async def stream_opening(
        self,
        call_id: str,
        text: str = "喂，你好？",
        *,
        turn_id: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        state = self.sessions.get(call_id)
        if not state or state.closed:
            raise StrategyV1Error("session_not_ready", "策略会话尚未建立")
        if state.opening_sent:
            return
        state.opening_sent = True
        async for segment in self.stream_turn(
            call_id,
            text,
            turn_id=turn_id or f"opening-{uuid.uuid4().hex}",
            publish_input_event=False,
        ):
            yield segment

    async def stream_turn(
        self,
        call_id: str,
        text: str,
        *,
        turn_id: Optional[str] = None,
        publish_input_event: bool = True,
    ) -> AsyncIterator[Dict[str, Any]]:
        state = self.sessions.get(call_id)
        if not state or state.closed:
            raise StrategyV1Error("session_not_ready", "策略会话尚未建立")
        cleaned = str(text or "").strip()
        if not cleaned:
            raise StrategyV1Error("empty_input", "客户话术不能为空")
        timeout_cfg = state.runtime.get("timeouts") if isinstance(state.runtime.get("timeouts"), dict) else {}
        first_timeout = float(timeout_cfg.get("first_response_seconds") or 8)
        second_grace = float(timeout_cfg.get("second_response_grace_seconds") or 5)
        effective_turn_id = str(turn_id or uuid.uuid4().hex)

        async with state.turn_lock:
            turn = StrategyTurn(turn_id=effective_turn_id, started_at=time.perf_counter())
            state.active_turn = turn
            await state.websocket.send(json.dumps(
                {"type": "input.text", "turn_id": effective_turn_id, "text": cleaned},
                ensure_ascii=False,
            ))
            if publish_input_event:
                await self._emit(
                    call_id,
                    "turn_started",
                    external_session_id=state.external_session_id,
                    turn_id=effective_turn_id,
                    customer_text=cleaned,
                )
            first_received = False
            first_deadline = time.monotonic() + max(0.1, first_timeout)
            second_deadline: Optional[float] = None
            try:
                while True:
                    deadline = second_deadline if first_received else first_deadline
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining <= 0:
                        if first_received:
                            state.expired_turn_id = effective_turn_id
                            return
                        raise StrategyV1Error("first_response_timeout", "策略服务首段回复超时")
                    try:
                        event = await asyncio.wait_for(self._receive_event(state.websocket), timeout=remaining)
                    except asyncio.TimeoutError as exc:
                        if first_received:
                            state.expired_turn_id = effective_turn_id
                            return
                        raise StrategyV1Error("first_response_timeout", "策略服务首段回复超时") from exc
                    event_type = str(event.get("type") or "")
                    if event_type == "error":
                        raise StrategyV1Error("external_error", self._event_error(event))
                    if event_type == "session.ended":
                        state.closed = True
                        raise StrategyV1Error("session_ended", "策略会话已结束")
                    if event_type == "response[1]" and not first_received:
                        discarded_turn_id = state.expired_turn_id or effective_turn_id
                        discard_reason = (
                            "expired_second_segment"
                            if state.expired_turn_id
                            else "orphan_second_segment"
                        )
                        await self._emit(
                            call_id,
                            "barge_in_discarded",
                            external_session_id=state.external_session_id,
                            turn_id=discarded_turn_id,
                            segment_index=1,
                            text=str(event.get("text") or ""),
                            reason=discard_reason,
                        )
                        if state.expired_turn_id:
                            state.expired_turn_id = None
                        continue
                    if event_type not in {"response[0]", "response[1]"}:
                        continue

                    index = 0 if event_type == "response[0]" else 1
                    segment_text = str(event.get("text") or "").strip()
                    if index == 0 and not segment_text:
                        raise StrategyV1Error("empty_response", "策略服务返回了空回复")
                    if not segment_text:
                        if index == 1:
                            return
                        continue
                    latency_ms = int((time.perf_counter() - turn.started_at) * 1000)
                    if turn.stale:
                        await self._emit(
                            call_id,
                            "barge_in_discarded",
                            external_session_id=state.external_session_id,
                            turn_id=effective_turn_id,
                            segment_index=index,
                            text=segment_text,
                            reason="barge_in",
                        )
                    else:
                        await self._emit(
                            call_id,
                            "response_segment",
                            external_session_id=state.external_session_id,
                            turn_id=effective_turn_id,
                            segment_index=index,
                            text=segment_text,
                            latency_ms=latency_ms,
                            total_latency_ms=latency_ms,
                        )
                        yield {
                            "turn_id": effective_turn_id,
                            "index": index,
                            "text": segment_text,
                            "latency_ms": latency_ms,
                        }
                    if index == 0:
                        first_received = True
                        second_deadline = time.monotonic() + max(0.1, second_grace)
                    else:
                        return
            finally:
                # AVA still owns this turn while its TTS audio is playing.
                # _run_strategy_pipeline_turn calls complete_turn after playback cleanup.
                pass

    def mark_barge_in(self, call_id: str) -> Optional[str]:
        state = self.sessions.get(call_id)
        if not state or not state.active_turn:
            return None
        state.active_turn.stale = True
        state.stale_turn_ids.add(state.active_turn.turn_id)
        return state.active_turn.turn_id

    def is_turn_stale(self, call_id: str, turn_id: str) -> bool:
        state = self.sessions.get(call_id)
        return bool(
            state
            and (
                turn_id in state.stale_turn_ids
                or (
                    state.active_turn
                    and state.active_turn.turn_id == turn_id
                    and state.active_turn.stale
                )
            )
        )

    def complete_turn(self, call_id: str, turn_id: str) -> None:
        state = self.sessions.get(call_id)
        if state:
            if state.active_turn and state.active_turn.turn_id == turn_id:
                state.active_turn = None
            state.stale_turn_ids.discard(turn_id)

    def clear_stale_turn(self, call_id: str, turn_id: str) -> None:
        self.complete_turn(call_id, turn_id)

    async def record_failure(self, call_id: str, reason: str) -> None:
        state = self.sessions.get(call_id)
        await self._emit(
            call_id,
            "failure",
            external_session_id=state.external_session_id if state else None,
            reason=str(reason or "策略服务异常")[:2000],
        )

    async def end(self, call_id: str) -> None:
        async with self._sessions_lock:
            state = self.sessions.pop(call_id, None)
        if not state:
            return
        try:
            if not state.closed:
                await state.websocket.send(json.dumps({"type": "session.end"}))
        except Exception:
            pass
        try:
            await state.websocket.close()
        except Exception:
            pass
        state.closed = True
        await self._emit(
            call_id,
            "session_ended",
            external_session_id=state.external_session_id,
        )
