import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.config import AppConfig, VllmOmniTTSProviderConfig
from src.engine import Engine
from src.pipelines import PipelineUnavailableError, resolve_channel_runtime_override
from src.pipelines.vllm_omni import VllmOmniTTSAdapter


class _FakeContent:
    def __init__(self, chunks=None, gate=None):
        self._chunks = list(chunks or [])
        self._gate = gate

    async def iter_any(self):
        if self._gate is not None:
            await self._gate.wait()
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(self, status=200, body=b"", chunks=None, gate=None):
        self.status = status
        self._body = body
        self.content = _FakeContent(chunks=chunks, gate=gate)
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    async def read(self):
        return self._body

    def close(self):
        self.closed = True


class _FakeSession:
    def __init__(self, response):
        self.response = response
        self.requests = []
        self.closed = False

    def post(self, url, *, json=None, headers=None, timeout=None):
        self.requests.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return self.response

    def get(self, url, *, headers=None, timeout=None):
        self.requests.append({"url": url, "headers": headers, "timeout": timeout})
        return self.response

    async def close(self):
        self.closed = True


def _config():
    return AppConfig(
        default_provider="vllm_omni_tts",
        providers={
            "vllm_omni_tts": {
                "type": "vllm_omni",
                "enabled": True,
                "tts_base_url": "http://127.0.0.1:18091/v1/audio/speech",
                "tts_model": "openbmb/VoxCPM2",
                "voice": "default",
                "response_format": "pcm",
                "target_encoding": "pcm16",
                "target_sample_rate_hz": 48000,
            }
        },
        asterisk={"host": "127.0.0.1", "username": "ari", "password": "secret"},
        llm={"initial_greeting": "hi", "prompt": "prompt", "model": "local"},
        pipelines={
            "vllm_omni": {
                "stt": "local_stt",
                "llm": "local_llm",
                "tts": "vllm_omni_tts",
                "options": {"tts": {"format": {"encoding": "pcm16", "sample_rate": 48000}}},
            }
        },
        active_pipeline="vllm_omni",
    )


def test_explicit_pipeline_takes_precedence_over_legacy_provider_override():
    assert resolve_channel_runtime_override(
        "vllm_omni_hybrid",
        "local_hybrid",
        {"local_hybrid"},
    ) == ("pipeline", "vllm_omni_hybrid")


def test_legacy_provider_override_behavior_is_preserved_without_pipeline():
    assert resolve_channel_runtime_override("", "google_live", {"google_live"}) == (
        "provider",
        "google_live",
    )
    assert resolve_channel_runtime_override(None, "local_hybrid", set()) == (
        "pipeline",
        "local_hybrid",
    )


@pytest.mark.asyncio
async def test_explicit_unavailable_pipeline_fails_closed():
    engine = Engine.__new__(Engine)
    engine.providers = {}
    engine.pipeline_orchestrator = SimpleNamespace(
        enabled=True,
        get_pipeline=Mock(
            side_effect=PipelineUnavailableError(
                "vllm_omni_hybrid",
                "pipeline does not exist",
            )
        ),
    )
    session = SimpleNamespace(call_id="call-1", provider_name=None)

    with pytest.raises(PipelineUnavailableError):
        await engine._assign_pipeline_to_session(
            session,
            pipeline_name="vllm_omni_hybrid",
            strict=True,
        )


@pytest.mark.asyncio
async def test_vllm_omni_streams_pcm_and_passes_voice_clone_fields():
    pcm_20ms = b"\x01\x00" * 960
    response = _FakeResponse(chunks=[pcm_20ms, pcm_20ms])
    session = _FakeSession(response)
    adapter = VllmOmniTTSAdapter(
        "vllm_omni_tts",
        _config(),
        VllmOmniTTSProviderConfig(
            tts_base_url="http://127.0.0.1:18091/v1/audio/speech",
            tts_model="openbmb/VoxCPM2",
            voice="default",
            response_format="pcm",
            source_sample_rate_hz=48000,
            target_encoding="pcm16",
            target_sample_rate_hz=48000,
        ),
        {"format": {"encoding": "pcm16", "sample_rate": 48000}},
        session_factory=lambda: session,
    )

    chunks = [
        chunk
        async for chunk in adapter.synthesize(
            "call-1",
            "Hello from the call.",
            {
                "ref_audio": "data:audio/wav;base64,cmVm",
                "ref_text": "Reference transcript.",
            },
        )
    ]

    assert chunks == [pcm_20ms, pcm_20ms]
    request = session.requests[0]
    assert request["url"].endswith("/v1/audio/speech")
    assert request["json"] == {
        "model": "openbmb/VoxCPM2",
        "input": "Hello from the call.",
        "voice": "default",
        "response_format": "pcm",
        "stream": True,
        "stream_format": "audio",
        "ref_audio": "data:audio/wav;base64,cmVm",
        "ref_text": "Reference transcript.",
    }
    assert response.closed


@pytest.mark.asyncio
async def test_vllm_omni_http_error_contains_status_and_body():
    response = _FakeResponse(status=503, body=b"model warming up")
    session = _FakeSession(response)
    adapter = VllmOmniTTSAdapter(
        "vllm_omni_tts",
        _config(),
        VllmOmniTTSProviderConfig(),
        {},
        session_factory=lambda: session,
    )

    with pytest.raises(RuntimeError, match="HTTP 503.*model warming up"):
        await anext(adapter.synthesize("call-2", "hello", {}))

    assert response.closed


@pytest.mark.asyncio
async def test_vllm_omni_health_check_does_not_require_api_key():
    response = _FakeResponse(status=200, body=b'{"status":"ok"}')
    session = _FakeSession(response)
    adapter = VllmOmniTTSAdapter(
        "vllm_omni_tts",
        _config(),
        VllmOmniTTSProviderConfig(
            tts_base_url="http://127.0.0.1:18091/v1/audio/speech",
            api_key=None,
        ),
        {},
        session_factory=lambda: session,
    )

    result = await adapter.validate_connectivity({})

    assert result["healthy"] is True
    assert session.requests[0]["url"] == "http://127.0.0.1:18091/health"


@pytest.mark.asyncio
async def test_vllm_omni_cancellation_closes_upstream_response():
    gate = asyncio.Event()
    response = _FakeResponse(chunks=[b"never"], gate=gate)
    session = _FakeSession(response)
    adapter = VllmOmniTTSAdapter(
        "vllm_omni_tts",
        _config(),
        VllmOmniTTSProviderConfig(),
        {},
        session_factory=lambda: session,
    )

    task = asyncio.create_task(anext(adapter.synthesize("call-3", "hello", {})))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert response.closed
