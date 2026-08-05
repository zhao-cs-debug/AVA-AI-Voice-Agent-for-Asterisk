from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.providers.external_strategy_agent import (
    ExternalStrategyAgentProvider,
    ExternalStrategyProtocolError,
)
from src.providers.external_strategy_config import ExternalStrategyAgentConfig


class FakeWebSocket:
    def __init__(self, events=None, timeline=None):
        self.events = list(events or [])
        self.timeline = timeline if timeline is not None else []
        self.sent_json = []
        self.sent_bytes = []
        self.closed = False
        self._hold = asyncio.Event()

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for event in self.events:
            yield event
        await self._hold.wait()

    async def send_json(self, payload):
        self.timeline.append(("ws", payload["type"]))
        self.sent_json.append(payload)

    async def send_bytes(self, payload):
        self.sent_bytes.append(payload)

    async def close(self):
        self.closed = True
        self._hold.set()


def provider_config(**overrides):
    values = {
        "base_url": "https://strategy.example",
        "api_key": "secret-key",
        "input_encoding": "linear16",
        "input_sample_rate_hz": 16000,
        "provider_input_sample_rate_hz": 16000,
        "output_sample_rate_hz": 24000,
        "audio_chunk_ms": 200,
        "close_timeout_sec": 0.01,
    }
    values.update(overrides)
    return ExternalStrategyAgentConfig.from_dict(values)


def provider_context(**overrides):
    external_strategy = {
        "network": {"asset_id": 3, "external_id": "network-016"},
        "ai": {"title": "AI顾问", "gender": "女", "background": "负责核实客户需求"},
        "human": {"title": "客户", "gender": "", "background_template": "客户姓名：{caller_name}"},
        "voice": {"value_field": "legacy_field", "value": "voice-2", "label": "女声二"},
        "audio": {"input_sample_rate": 16000, "output_sample_rate": 24000, "chunk_ms": 200},
    }
    external_strategy.update(overrides)
    return {"caller_name": "张女士", "external_strategy": external_strategy}


def test_provider_is_ready_when_external_service_authentication_is_disabled():
    provider = ExternalStrategyAgentProvider(
        provider_config(api_key=""),
        AsyncMock(),
    )

    assert provider.is_ready() is True


def test_context_rejects_audio_frame_larger_than_documented_limit():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())

    with pytest.raises(ValueError, match="256 KiB"):
        provider._validated_context(provider_context(audio={
            "input_sample_rate": 192000,
            "output_sample_rate": 24000,
            "chunk_ms": 1000,
        }))


@pytest.mark.asyncio
async def test_unauthenticated_http_session_omits_empty_authorization_header():
    provider = ExternalStrategyAgentProvider(
        provider_config(api_key=""),
        AsyncMock(),
    )

    session = await provider._http_session()
    try:
        assert "Authorization" not in session.headers
        assert session.headers["Content-Type"] == "application/json"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_start_session_runs_full_http_lifecycle_and_uses_returned_worker_url():
    events = []

    async def on_event(event):
        events.append(event)

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    request = AsyncMock(side_effect=[
        {
            "session_id": "session-1",
            "settings_url": "https://strategy.example/settings/session-1",
            "websocket_path": "wss://strategy.example/session-1",
        },
        {
            "settings_confirmed": True,
            "settings": {
                "ai_title": "AI顾问",
                "strategy_network_id": "network-016",
                "preserve_me": "yes",
            },
            "websocket_path": "wss://strategy.example/session-1",
        },
        {
            "enabled": True,
            "value_field": "qwen3_voice_uuid",
            "voices": [
                {"value": "voice-1", "label": "女声一"},
                {"value": "voice-2", "label": "女声二"},
            ],
        },
        {
            "ok": True,
            "settings": {
                "ai_title": "AI顾问",
                "strategy_network_id": "network-016",
                "preserve_me": "yes",
                "qwen3_voice_uuid": "voice-2",
            },
        },
    ])
    websocket = FakeWebSocket([
        {
            "type": "session.ready",
            "mode": "voice",
            "audio": {
                "input": {"encoding": "pcm_s16le", "sample_rate": 16000, "channels": 1},
                "output": {"encoding": "pcm_s16le", "sample_rate": 24000, "channels": 1},
            },
        },
        {"type": "session.started", "session_id": "session-1"},
    ])
    provider._request_json = request
    provider._connect_websocket = AsyncMock(return_value=websocket)

    await provider.start_session("call-1", context=provider_context())

    assert request.await_args_list[0].args[:2] == ("POST", "/api/v1/external/sessions")
    assert request.await_args_list[0].kwargs["payload"]["mode"] == "voice"
    assert request.await_args_list[1].args[:2] == (
        "PUT",
        "https://strategy.example/settings/session-1",
    )
    settings_payload = request.await_args_list[1].kwargs["payload"]
    assert settings_payload["network"] == {"mode": "existing", "id": "network-016"}
    assert settings_payload["human"]["background"] == "客户姓名：张女士"
    assert request.await_args_list[2].args[:2] == (
        "GET",
        "/api/tts/voices?conn_id=session-1",
    )
    saved = request.await_args_list[3].kwargs["payload"]["settings"]
    assert saved["preserve_me"] == "yes"
    assert saved["qwen3_voice_uuid"] == "voice-2"
    assert "legacy_field" not in saved
    provider._connect_websocket.assert_awaited_once_with("wss://strategy.example/session-1")
    assert websocket.sent_json[0] == {
        "type": "session.start",
        "input_audio_sample_rate": 16000,
    }
    assert events[-1]["type"] == "session_started"

    await provider.stop_session()

    assert {"type": "session.end"} in websocket.sent_json
    assert websocket.closed is True


@pytest.mark.asyncio
async def test_start_session_keeps_server_voice_when_catalog_has_no_choices():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())
    request = AsyncMock(side_effect=[
        {
            "session_id": "session-empty-catalog",
            "settings_url": "/api/v1/external/sessions/session-empty-catalog/settings",
            "websocket_path": "/api/v1/external/sessions/session-empty-catalog/stream",
        },
        {
            "settings_confirmed": True,
            "settings": {
                "ai_title": "AI顾问",
                "strategy_network_id": "network-016",
                "server_selected_voice": "automatic",
            },
        },
        {
            "enabled": True,
            "value_field": "qwen3_voice_uuid",
            "voices": [],
        },
    ])
    websocket = FakeWebSocket([
        {
            "type": "session.ready",
            "mode": "voice",
            "audio": {
                "input": {"encoding": "pcm_s16le", "sample_rate": 16000, "channels": 1},
                "output": {"encoding": "pcm_s16le", "sample_rate": 24000, "channels": 1},
            },
        },
        {"type": "session.started", "session_id": "session-empty-catalog"},
    ])
    provider._request_json = request
    provider._connect_websocket = AsyncMock(return_value=websocket)

    await provider.start_session("call-empty-catalog", context=provider_context())

    assert request.await_count == 3
    provider._connect_websocket.assert_awaited_once_with(
        "wss://strategy.example/api/v1/external/sessions/session-empty-catalog/stream"
    )
    await provider.stop_session()


@pytest.mark.asyncio
async def test_audio_events_are_ordered_deduplicated_and_acknowledged_after_enqueue():
    timeline = []

    async def on_event(event):
        timeline.append(("event", event["type"]))
        if event["type"] == "AgentAudio":
            return True

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    provider._call_id = "call-2"
    provider._session_id = "session-2"
    provider._connected = True
    provider._ws = FakeWebSocket(timeline=timeline)
    pcm = b"\x01\x00" * 240
    payload = {
        "type": "response.audio.delta",
        "response_id": "reply-1",
        "chunk_id": "reply-1-1",
        "audio": base64.b64encode(pcm).decode("ascii"),
        "sample_rate": 24000,
        "text": "您好",
        "final": True,
    }

    await provider._handle_event({
        "type": "response[0]",
        "response_id": "reply-1",
        "timestamp_ms": 100,
        "text": "您好",
        "final": True,
    })
    await provider._handle_event(payload)
    await provider._handle_event(payload)

    event_types = [name for source, name in timeline if source == "event"]
    assert event_types == ["agent_transcript", "AgentAudio", "AgentAudioDone"]
    assert timeline.index(("event", "AgentAudio")) < timeline.index(("ws", "response.audio.playback"))
    playback = [item for item in provider._ws.sent_json if item["type"] == "response.audio.playback"]
    assert len(playback) == 1
    assert playback[0]["chunk_id"] == "reply-1-1"
    assert playback[0]["last"] is True


@pytest.mark.asyncio
async def test_rejected_audio_is_not_acknowledged_or_marked_as_duplicate():
    accepted = False
    emitted = []

    async def on_event(event):
        emitted.append(event)
        if event["type"] == "AgentAudio":
            return accepted

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    provider._call_id = "call-retry"
    provider._session_id = "session-retry"
    provider._connected = True
    provider._ws = FakeWebSocket()
    payload = {
        "type": "response.audio.delta",
        "response_id": "reply-retry",
        "chunk_id": "reply-retry-1",
        "audio": base64.b64encode(b"\x01\x00" * 240).decode("ascii"),
        "sample_rate": 24000,
        "text": "请稍等",
        "final": True,
    }

    await provider._handle_event(payload)

    assert provider._ws.sent_json == []
    assert "reply-retry-1" not in provider._seen_chunk_ids
    assert not any(event["type"] == "AgentAudioDone" for event in emitted)

    accepted = True
    await provider._handle_event(payload)

    assert [item["type"] for item in provider._ws.sent_json] == ["response.audio.playback"]
    assert "reply-retry-1" in provider._seen_chunk_ids
    assert any(event["type"] == "AgentAudioDone" for event in emitted)


@pytest.mark.asyncio
async def test_audio_requires_explicit_local_playback_acceptance_before_acknowledgement():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock(return_value=None))
    provider._call_id = "call-no-acceptance"
    provider._session_id = "session-no-acceptance"
    provider._connected = True
    provider._ws = FakeWebSocket()
    payload = {
        "type": "response.audio.delta",
        "response_id": "reply-no-acceptance",
        "chunk_id": "reply-no-acceptance-1",
        "audio": base64.b64encode(b"\x01\x00" * 240).decode("ascii"),
        "sample_rate": 24000,
        "text": "请稍等",
        "final": False,
    }

    await provider._handle_event(payload)

    assert provider._ws.sent_json == []
    assert "reply-no-acceptance-1" not in provider._seen_chunk_ids


@pytest.mark.asyncio
async def test_session_ready_rejects_non_mono_output_before_starting():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())
    provider._call_id = "call-invalid-ready"
    provider._session_id = "session-invalid-ready"
    provider._connected = True
    provider._ws = FakeWebSocket()

    with pytest.raises(ExternalStrategyProtocolError, match="mono"):
        await provider._handle_event({
            "type": "session.ready",
            "mode": "voice",
            "audio": {
                "input": {
                    "encoding": "pcm_s16le",
                    "sample_rate": 16000,
                    "channels": 1,
                },
                "output": {
                    "encoding": "pcm_s16le",
                    "sample_rate": 24000,
                    "channels": 2,
                },
            },
        })

    assert provider._ws.sent_json == []


@pytest.mark.asyncio
async def test_audio_delta_rejects_non_mono_and_per_response_rate_changes():
    async def on_event(event):
        if event["type"] == "AgentAudio":
            return True

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    provider._call_id = "call-audio-contract"
    provider._session_id = "session-audio-contract"
    provider._connected = True
    provider._ws = FakeWebSocket()
    encoded = base64.b64encode(b"\x01\x00" * 240).decode("ascii")

    with pytest.raises(ExternalStrategyProtocolError, match="mono"):
        await provider._handle_event({
            "type": "response.audio.delta",
            "response_id": "reply-contract",
            "chunk_id": "reply-contract-invalid",
            "audio": encoded,
            "sample_rate": 24000,
            "channels": 2,
            "final": False,
        })

    await provider._handle_event({
        "type": "response.audio.delta",
        "response_id": "reply-contract",
        "chunk_id": "reply-contract-1",
        "audio": encoded,
        "sample_rate": 24000,
        "channels": 1,
        "final": False,
    })

    with pytest.raises(ExternalStrategyProtocolError, match="sample rate changed"):
        await provider._handle_event({
            "type": "response.audio.delta",
            "response_id": "reply-contract",
            "chunk_id": "reply-contract-2",
            "audio": encoded,
            "sample_rate": 16000,
            "channels": 1,
            "final": True,
        })

    playback = [item for item in provider._ws.sent_json if item["type"] == "response.audio.playback"]
    assert [item["chunk_id"] for item in playback] == ["reply-contract-1"]


@pytest.mark.asyncio
async def test_transcript_probe_interrupt_ping_and_blocked_audio_protocol():
    emitted = []

    async def on_event(event):
        emitted.append(event)

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    provider._call_id = "call-3"
    provider._session_id = "session-3"
    provider._connected = True
    provider._ws = FakeWebSocket()

    await provider._handle_event({
        "type": "input.transcript",
        "timestamp_ms": 101,
        "sequence": 1,
        "text": "中间",
        "final": False,
    })
    await provider._handle_event({
        "type": "input.transcript",
        "timestamp_ms": 102,
        "sequence": 2,
        "text": "最终内容",
        "final": True,
    })
    provider._played_text["reply-2"] = "已经播放"
    await provider._handle_event({
        "type": "response.audio.probe",
        "probe_id": "probe-1",
        "response_id": "reply-2",
        "chunk_id": "reply-2-2",
    })
    await provider._handle_event({
        "type": "response.audio.interrupted",
        "response_id": "reply-2",
        "chunk_id": "reply-2-2",
        "blocked_response_ids": ["reply-3"],
        "truncated_text": "已经",
    })
    await provider._handle_event({"type": "ping", "timestamp_ms": 777})
    await provider._handle_event({
        "type": "response.audio.delta",
        "response_id": "reply-3",
        "chunk_id": "reply-3-1",
        "audio": base64.b64encode(b"\x00\x00" * 10).decode("ascii"),
        "sample_rate": 24000,
        "final": False,
    })

    assert [event["type"] for event in emitted] == ["transcript", "interruption"]
    assert emitted[0]["text"] == "最终内容"
    sent = {item["type"]: item for item in provider._ws.sent_json}
    assert sent["response.audio.probe_result"]["visible_text"] == "已经播放"
    assert sent["response.audio.interrupt_ack"]["response_id"] == "reply-2"
    assert sent["pong"]["client_timestamp_ms"] == 777
    assert not any(item["type"] == "response.audio.playback" for item in provider._ws.sent_json)


@pytest.mark.asyncio
async def test_send_audio_converts_to_pcm16_and_batches_200ms_frames():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())
    provider._call_id = "call-4"
    provider._connected = True
    provider._ws = FakeWebSocket()

    await provider.send_audio(b"\x01\x00" * 800, sample_rate=16000, encoding="linear16")
    assert provider._ws.sent_bytes == []

    await provider.send_audio(b"\x02\x00" * 2400, sample_rate=16000, encoding="linear16")

    assert len(provider._ws.sent_bytes) == 1
    assert len(provider._ws.sent_bytes[0]) == 6400


@pytest.mark.parametrize(
    ("value", "websocket"),
    [
        ("http://strategy.example/session/settings", False),
        ("https://attacker.example/session/settings", False),
        ("ws://strategy.example/session/stream", True),
        ("wss://attacker.example/session/stream", True),
    ],
)
def test_server_returned_urls_cannot_leave_or_downgrade_the_configured_origin(value, websocket):
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())

    with pytest.raises(ExternalStrategyProtocolError, match="unsafe URL"):
        provider._bounded_url(value, websocket=websocket)


@pytest.mark.asyncio
async def test_remote_session_end_notifies_call_layer_and_closes_transport():
    emitted = []

    async def on_event(event):
        emitted.append(event)

    provider = ExternalStrategyAgentProvider(provider_config(), on_event)
    provider._call_id = "call-remote-end"
    provider._session_id = "session-remote-end"
    provider._connected = True
    provider._closing = False
    provider._startup = asyncio.get_running_loop().create_future()
    provider._startup.set_result(None)
    provider._ws = FakeWebSocket()

    await provider._handle_event({"type": "session.ended", "reason": "server_shutdown"})

    assert provider._remote_ended is True
    assert provider._ended.is_set()
    assert provider._ws.closed is True
    assert emitted == [
        {
            "type": "ProviderDisconnected",
            "call_id": "call-remote-end",
            "provider": "external_strategy_agent",
            "reason": "server_shutdown",
        }
    ]


@pytest.mark.asyncio
async def test_remote_session_end_before_start_fails_startup_without_waiting_for_timeout():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())
    provider._call_id = "call-early-end"
    provider._session_id = "session-early-end"
    provider._connected = True
    provider._closing = False
    provider._startup = asyncio.get_running_loop().create_future()
    provider._ws = FakeWebSocket()

    await provider._handle_event({"type": "session.ended", "reason": "settings_invalid"})

    with pytest.raises(ExternalStrategyProtocolError, match="settings_invalid"):
        await asyncio.wait_for(asyncio.shield(provider._startup), timeout=0.05)


@pytest.mark.asyncio
async def test_websocket_connect_uses_aiohttp_39_compatible_bounded_arguments():
    provider = ExternalStrategyAgentProvider(provider_config(), AsyncMock())
    websocket = FakeWebSocket()
    ws_connect = AsyncMock(return_value=websocket)
    provider._http = SimpleNamespace(closed=False, ws_connect=ws_connect)

    connected = await provider._connect_websocket("wss://strategy.example/session/stream")

    assert connected is websocket
    kwargs = ws_connect.await_args.kwargs
    assert kwargs["timeout"] == provider.config.connect_timeout_sec
    assert kwargs["receive_timeout"] is None
    assert kwargs["max_msg_size"] == provider.config.max_message_bytes


def test_automatic_voice_selection_prefers_matching_ai_gender_before_first_item():
    catalog = {
        "enabled": True,
        "value_field": "dynamic_voice_field",
        "voices": [
            {"value": "voice-male", "gender": "男"},
            {"value": "voice-female", "gender": "女"},
        ],
    }

    selected = ExternalStrategyAgentProvider._select_voice(catalog, {}, ai_gender="女")

    assert selected == {"value_field": "dynamic_voice_field", "value": "voice-female"}
