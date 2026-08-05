from __future__ import annotations

import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.config.loaders import load_yaml_with_env_expansion
from src.config.provider_instances import (
    API_KEY_COMPATIBLE_KINDS,
    FULL_AGENT_KINDS,
    FULL_AGENT_KINDS_WITH_NATIVE_TTS_GATING,
    provider_kind,
    validate_provider_instances,
)
from src.core.transport_orchestrator import TransportOrchestrator
from src.core.models import CallSession
from src.engine import Engine


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_external_strategy_agent_is_registered_as_full_agent():
    assert "external_strategy_agent" in FULL_AGENT_KINDS
    assert "external_strategy_agent" in FULL_AGENT_KINDS_WITH_NATIVE_TTS_GATING
    assert "external_strategy_agent" in API_KEY_COMPATIBLE_KINDS
    assert provider_kind(
        "customer_strategy_agent",
        {"type": "external_strategy_agent", "enabled": True},
    ) == "external_strategy_agent"

    validate_provider_instances({
        "default_provider": "customer_strategy_agent",
        "providers": {
            "customer_strategy_agent": {
                "type": "external_strategy_agent",
                "enabled": True,
            },
        },
    })


def test_external_strategy_context_is_loaded_without_affecting_other_fields():
    external_strategy = {
        "network": {"asset_id": 3, "external_id": "network-016"},
        "ai": {"title": "AI顾问", "gender": "女", "background": "负责核实需求"},
        "human": {"title": "客户", "gender": "", "background_template": "姓名：{caller_name}"},
        "voice": {"value_field": "qwen3_voice_uuid", "value": "voice-2"},
        "audio": {"input_sample_rate": 16000, "output_sample_rate": 24000, "chunk_ms": 200},
    }
    orchestrator = TransportOrchestrator({
        "contexts": {
            "strategy_demo": {
                "provider": "customer_strategy_agent",
                "profile": "telephony_ulaw_8k",
                "prompt": "legacy prompt remains available",
                "external_strategy": external_strategy,
            },
        },
    })

    context = orchestrator.get_context_config("strategy_demo")

    assert context is not None
    assert context.provider == "customer_strategy_agent"
    assert context.prompt == "legacy prompt remains available"
    assert context.external_strategy == external_strategy


def test_external_strategy_config_resolves_api_key_from_named_environment(monkeypatch):
    monkeypatch.setenv("TEST_STRATEGY_API_KEY", "resolved-secret")
    engine = Engine.__new__(Engine)
    engine.config = types.SimpleNamespace()

    config = engine._build_external_strategy_config({
        "type": "external_strategy_agent",
        "enabled": True,
        "base_url": "https://strategy.example",
        "api_key_env": "TEST_STRATEGY_API_KEY",
    }, "customer_strategy_agent")

    assert config is not None
    assert config.api_key == "resolved-secret"
    assert config.base_url == "https://strategy.example"


def test_default_external_strategy_provider_reuses_established_base_url_env(monkeypatch):
    monkeypatch.setenv("STRATEGY_NETWORK_API_BASE_URL", "https://strategy.override.example")

    config = load_yaml_with_env_expansion(str(REPO_ROOT / "config" / "ai-agent.yaml"))

    provider = config["providers"]["external_strategy_agent"]
    assert provider["base_url"] == "https://strategy.override.example"


def test_only_external_strategy_keeps_native_barge_in_audio_during_tts():
    engine = Engine.__new__(Engine)
    engine.provider_kinds = {
        "strategy-v1": "external_strategy_agent",
        "private-v2": "local_hybrid",
        "strategy-v3": "local_hybrid",
        "openai": "openai_realtime",
    }

    assert engine._provider_keeps_live_input_during_tts("strategy-v1") is True
    assert engine._provider_keeps_live_input_during_tts("private-v2") is False
    assert engine._provider_keeps_live_input_during_tts("strategy-v3") is False
    assert engine._provider_keeps_live_input_during_tts("openai") is False


@pytest.mark.asyncio
async def test_external_strategy_rtp_forwards_live_audio_while_tts_is_playing():
    call_id = "strategy-full-duplex"
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="strategy-v1",
    )
    session.provider_session_active = True
    session.audio_capture_enabled = False
    provider = SimpleNamespace(
        config=SimpleNamespace(continuous_input=True),
        get_capabilities=lambda: SimpleNamespace(requires_continuous_audio=True),
        send_audio=AsyncMock(),
    )
    engine = Engine.__new__(Engine)
    engine.provider_kinds = {"strategy-v1": "external_strategy_agent"}
    engine.providers = {"strategy-v1": provider}
    engine._call_providers = {call_id: provider}
    engine._provider_start_tasks = {}
    engine._pipeline_forced = {}
    engine._pipeline_queues = {}
    engine.session_store = SimpleNamespace(get_by_call_id=AsyncMock(return_value=session))
    engine._save_session = AsyncMock()
    engine._consume_attended_transfer_screening_audio = lambda *_args, **_kwargs: False
    engine._session_has_pending_attended_transfer = lambda *_args, **_kwargs: False
    engine._encode_for_provider = lambda *_args, **_kwargs: (b"provider-pcm", "linear16", 16000)
    engine.audio_capture = SimpleNamespace(append_encoded=lambda *_args, **_kwargs: None)
    engine._publish_audio_to_voiceai = AsyncMock()
    engine._maybe_provider_barge_in_fallback = AsyncMock()
    engine.rtp_server = SimpleNamespace(sample_rate=16000)
    engine.config = SimpleNamespace(default_provider="strategy-v1")
    engine.conversation_coordinator = None

    await engine._on_rtp_audio(call_id, 1234, b"\x01\x00" * 320)

    provider.send_audio.assert_awaited_once_with(
        b"provider-pcm",
        sample_rate=16000,
        encoding="linear16",
    )
