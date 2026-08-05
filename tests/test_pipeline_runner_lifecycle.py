import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.config import AppConfig
from src.engine import Engine, PipelineStartupError
from src.pipelines.base import STTComponent, LLMComponent, TTSComponent


class _StubSTT(STTComponent):
    async def transcribe(self, call_id, audio_pcm16, sample_rate_hz, options):
        return "hi"


class _StubLLM(LLMComponent):
    async def generate(self, call_id, transcript, context, options):
        return "hello"


class _StubTTS(TTSComponent):
    async def synthesize(self, call_id, text, options):
        yield b"ulaw-bytes"


class _StubResolution:
    def __init__(self):
        self.pipeline_name = "stub"
        self.stt_adapter = _StubSTT()
        self.llm_adapter = _StubLLM()
        self.tts_adapter = _StubTTS()
        self.stt_options = {}
        self.llm_options = {}
        self.tts_options = {}
        self.prepared = True

    def component_summary(self):
        return {"stt": "stub", "llm": "stub", "tts": "stub"}


class _TrackedSTT(_StubSTT):
    def __init__(self, events, fail=False):
        self.events = events
        self.fail = fail

    async def open_call(self, call_id, options):
        self.events.append("open:stt")
        if self.fail:
            raise RuntimeError("stt unavailable")

    async def close_call(self, call_id):
        self.events.append("close:stt")


class _TrackedLLM(_StubLLM):
    def __init__(self, events, fail=False):
        self.events = events
        self.fail = fail

    async def open_call(self, call_id, options):
        self.events.append("open:llm")
        if self.fail:
            raise RuntimeError("llm unavailable")

    async def close_call(self, call_id):
        self.events.append("close:llm")


class _TrackedTTS(_StubTTS):
    def __init__(self, events, fail=False):
        self.events = events
        self.fail = fail

    async def open_call(self, call_id, options):
        self.events.append("open:tts")
        if self.fail:
            raise RuntimeError("tts unavailable")

    async def close_call(self, call_id):
        self.events.append("close:tts")


@pytest.mark.asyncio
async def test_pipeline_runner_lifecycle(monkeypatch):
    # Minimal AppConfig, orchestrator presence is enough; we will stub its output
    config_data = {
        "default_provider": "local",
        "providers": {"local": {"enabled": True}},
        "asterisk": {"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        "llm": {"initial_greeting": "hi", "prompt": "You are helpful", "model": "gpt-4o"},
        "pipelines": {"local_only": {}},
        "active_pipeline": "local_only",
        "audio_transport": "externalmedia",
    }
    app_config = AppConfig(**config_data)

    engine = Engine(app_config)
    engine.pipeline_orchestrator._started = True

    # Stub orchestrator to return a fake resolution with in-memory adapters
    def fake_get_pipeline(call_id, pipeline_name=None):
        return _StubResolution()

    monkeypatch.setattr(engine.pipeline_orchestrator, "get_pipeline", fake_get_pipeline)

    # Register a fake session
    from src.core.models import CallSession
    call_id = "call-abc"
    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local_only"
    await engine.session_store.upsert_call(session)

    # Start pipeline runner explicitly
    await engine._ensure_pipeline_runner(session, forced=True)

    assert call_id in engine._pipeline_tasks
    assert call_id in engine._pipeline_queues

    # Feed some audio and then cleanup
    q = engine._pipeline_queues[call_id]
    await q.put(b"\x00\x00" * 512)  # short chunk; runner will batch and continue

    await engine._cleanup_call(call_id)

    # Runner should be cancelled and queues/flags cleared
    assert call_id not in engine._pipeline_tasks
    assert call_id not in engine._pipeline_queues
    assert call_id not in engine._pipeline_forced


@pytest.mark.asyncio
async def test_cleanup_events_share_one_single_flight_task(monkeypatch):
    app_config = AppConfig(
        default_provider="local",
        providers={"local": {"enabled": True}},
        asterisk={"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        pipelines={},
        audio_transport="externalmedia",
    )
    engine = Engine(app_config)
    from src.core.models import CallSession

    call_id = "cleanup-single-flight"
    await engine.session_store.upsert_call(CallSession(call_id=call_id, caller_channel_id=call_id))
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fake_cleanup_once(identifier):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()

    monkeypatch.setattr(engine, "_cleanup_call_once", fake_cleanup_once)
    first = asyncio.create_task(engine._cleanup_call(call_id))
    await entered.wait()
    second = asyncio.create_task(engine._cleanup_call(call_id))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert calls == 1


@pytest.mark.asyncio
async def test_cleanup_failure_can_be_retried(monkeypatch):
    app_config = AppConfig(
        default_provider="local",
        providers={"local": {"enabled": True}},
        asterisk={"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        pipelines={},
        audio_transport="externalmedia",
    )
    engine = Engine(app_config)
    from src.core.models import CallSession

    call_id = "cleanup-retry"
    await engine.session_store.upsert_call(CallSession(call_id=call_id, caller_channel_id=call_id))
    attempts = 0

    async def fake_cleanup_once(identifier):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary cleanup failure")

    monkeypatch.setattr(engine, "_cleanup_call_once", fake_cleanup_once)
    with pytest.raises(RuntimeError):
        await engine._cleanup_call(call_id)
    await engine._cleanup_call(call_id)

    assert attempts == 2


@pytest.mark.asyncio
async def test_cleanup_continues_when_waiting_event_handler_is_cancelled(monkeypatch):
    app_config = AppConfig(
        default_provider="local",
        providers={"local": {"enabled": True}},
        asterisk={"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        pipelines={},
        audio_transport="externalmedia",
    )
    engine = Engine(app_config)
    from src.core.models import CallSession

    call_id = "cleanup-cancelled-waiter"
    await engine.session_store.upsert_call(CallSession(call_id=call_id, caller_channel_id=call_id))
    entered = asyncio.Event()
    release = asyncio.Event()
    attempts = 0

    async def fake_cleanup_once(identifier):
        nonlocal attempts
        attempts += 1
        entered.set()
        await release.wait()

    monkeypatch.setattr(engine, "_cleanup_call_once", fake_cleanup_once)
    waiter = asyncio.create_task(engine._cleanup_call(call_id))
    await entered.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    await asyncio.sleep(0.05)
    await engine._cleanup_call(call_id)

    assert attempts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failed_component", "expected_events"),
    [
        ("stt", ["open:stt"]),
        ("llm", ["open:stt", "open:llm", "close:stt"]),
        ("tts", ["open:stt", "open:llm", "open:tts", "close:llm", "close:stt"]),
    ],
)
async def test_pipeline_startup_failure_closes_opened_components_in_reverse_order(
    failed_component,
    expected_events,
):
    app_config = AppConfig(
        default_provider="local",
        providers={"local": {"enabled": True}},
        asterisk={"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        pipelines={},
        audio_transport="externalmedia",
    )
    engine = Engine(app_config)
    events = []
    pipeline = _StubResolution()
    pipeline.stt_adapter = _TrackedSTT(events, fail=failed_component == "stt")
    pipeline.llm_adapter = _TrackedLLM(events, fail=failed_component == "llm")
    pipeline.tts_adapter = _TrackedTTS(events, fail=failed_component == "tts")

    with pytest.raises(PipelineStartupError) as error:
        await engine._open_pipeline_adapters(
            "startup-failure",
            pipeline,
            {},
            strategy_runtime=None,
        )

    assert error.value.component == failed_component
    assert events == expected_events


@pytest.mark.asyncio
async def test_pipeline_failure_is_persisted_and_published(monkeypatch):
    app_config = AppConfig(
        default_provider="local",
        providers={"local": {"enabled": True}},
        asterisk={"host": "127.0.0.1", "port": 8088, "username": "u", "password": "p", "app_name": "ai-voice-agent"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        pipelines={},
        audio_transport="externalmedia",
    )
    engine = Engine(app_config)
    from src.core.models import CallSession

    call_id = "pipeline-failure"
    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local_only"
    await engine.session_store.upsert_call(session)
    publish_failure = AsyncMock()
    monkeypatch.setattr(engine, "_publish_pipeline_failure_to_voiceai", publish_failure)

    await engine._handle_pipeline_runner_failure(
        call_id,
        PipelineStartupError("tts", RuntimeError("voice unavailable")),
    )

    saved = await engine.session_store.get_by_call_id(call_id)
    assert saved is not None
    assert saved.error_message.startswith("pipeline_startup_failed:")
    assert saved.conversation_history[-1]["error_code"] == "pipeline_startup_failed"
    publish_failure.assert_awaited_once()


@pytest.mark.asyncio
async def test_pipeline_runner_coordinates_extracted_lifecycle(monkeypatch):
    engine = Engine.__new__(Engine)
    events = []
    ingestion_started = asyncio.Event()
    release_ingestion = asyncio.Event()
    context = SimpleNamespace(
        pipeline=object(),
        llm_options={"system_prompt": "prompt"},
        strategy_runtime=None,
    )

    async def step(name, result=None):
        events.append(name)
        return result

    async def play_greeting(current):
        await asyncio.wait_for(ingestion_started.wait(), timeout=0.05)
        events.append("greeting")
        release_ingestion.set()

    async def run_audio(current):
        events.append("audio-start")
        ingestion_started.set()
        await release_ingestion.wait()
        events.append("audio-end")

    monkeypatch.setattr(engine, "_resolve_pipeline_runner_context", lambda call_id: step("resolve", context))
    monkeypatch.setattr(engine, "_open_pipeline_adapters", lambda *args, **kwargs: step("open"))
    monkeypatch.setattr(engine, "_start_pipeline_strategy", lambda current: step("strategy", True))
    monkeypatch.setattr(engine, "_play_pipeline_greeting", play_greeting)
    monkeypatch.setattr(engine, "_run_pipeline_audio_ingestion", run_audio)
    monkeypatch.setattr(engine, "_handle_pipeline_runner_failure", lambda *args: step("failure"))
    engine._pipeline_tasks = {}

    await engine._pipeline_runner("coordinated-call")

    assert events == ["resolve", "open", "strategy", "audio-start", "greeting", "audio-end"]


@pytest.mark.parametrize("call_kind", ["test", "campaign", "standard"])
def test_strategy_runtime_accepts_all_call_kinds(call_kind):
    runtime = {"enabled": True, "mode": "real", "real_scope": "all_calls"}
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(contexts={"strategy-context": {"strategy_runtime": runtime}})
    session = SimpleNamespace(
        context_name="strategy-context",
        provider_overrides={"VOICEAI_CALL_KIND": call_kind},
    )

    assert engine._strategy_runtime_for_session(session) == runtime


@pytest.mark.asyncio
async def test_transcript_sink_cleanup_task_is_tracked_and_cancellable():
    engine = Engine.__new__(Engine)
    engine._voiceai_transcript_sinks = {"sink-call": "http://127.0.0.1:8000"}
    engine._voiceai_transcript_sink_tasks = set()

    engine._schedule_voiceai_transcript_sink_cleanup("sink-call")

    tasks = tuple(engine._voiceai_transcript_sink_tasks)
    assert len(tasks) == 1
    tasks[0].cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0)
    assert not engine._voiceai_transcript_sink_tasks


def test_cleanup_effect_idempotency_keys_are_independent():
    engine = Engine.__new__(Engine)
    engine._cleanup_effects_completed = {}

    engine._mark_cleanup_effect_done("call-id", "call_history")
    engine._mark_cleanup_effect_done("call-id", "transcript_email:a@example.com")

    assert engine._cleanup_effect_done("call-id", "call_history") is True
    assert engine._cleanup_effect_done("call-id", "transcript_email:a@example.com") is True
    assert engine._cleanup_effect_done("call-id", "post_call_tools") is False
