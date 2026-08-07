import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.config import AppConfig
from src.core.turn_lifecycle import TurnLifecycleState
from src.engine import Engine, PipelineRunnerContext, PipelineStartupError
from src.pipelines.base import LLMResponse, STTComponent, LLMComponent, TTSComponent


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
    assert engine._pipeline_transcript_queues[call_id].maxsize == 64

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
async def test_cleanup_marks_call_terminating_before_cleanup_work(monkeypatch):
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

    call_id = "cleanup-termination-gate"
    await engine.session_store.upsert_call(CallSession(call_id=call_id, caller_channel_id=call_id))
    observed = []

    async def fake_cleanup_once(identifier):
        observed.append(engine._is_pipeline_call_terminating(call_id))

    monkeypatch.setattr(engine, "_cleanup_call_once", fake_cleanup_once)

    await engine._cleanup_call(call_id)

    assert observed == [True]


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
async def test_streaming_final_confirms_barge_in_while_dialog_is_waiting(monkeypatch):
    engine = Engine.__new__(Engine)
    confirmed = asyncio.Event()
    release_dialog = asyncio.Event()

    class StreamingSTT:
        async def start_stream(self, call_id, options):
            return None

        async def stop_stream(self, call_id):
            return None

        async def send_audio(self, call_id, audio, fmt):
            return None

        async def iter_events(self, call_id):
            yield {
                "text": "customer interruption",
                "is_final": True,
                "is_partial": False,
                "event_id": "asr-final-1",
            }

    async def confirm_candidate(call_id, text):
        confirmed.set()
        return True

    async def wait_for_current_turn(_context):
        await release_dialog.wait()

    engine._last_transcript_ts = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(side_effect=confirm_candidate)
    monkeypatch.setattr(engine, "_run_pipeline_dialog", wait_for_current_turn)

    inbound_queue = asyncio.Queue()
    await inbound_queue.put(None)
    context = PipelineRunnerContext(
        call_id="barge-during-playback",
        session=SimpleNamespace(),
        pipeline=SimpleNamespace(stt_adapter=StreamingSTT()),
        strategy_runtime=None,
        llm_options={},
        stt_options={},
        inbound_queue=inbound_queue,
        buffer_queue=asyncio.Queue(maxsize=4),
        transcript_queue=asyncio.Queue(maxsize=4),
        use_streaming=True,
        stream_format="pcm16",
        commit_bytes=320,
    )

    ingestion = asyncio.create_task(engine._run_pipeline_audio_ingestion(context))
    await asyncio.wait_for(confirmed.wait(), timeout=0.5)

    assert not ingestion.done()
    engine._confirm_pipeline_barge_in_candidate.assert_awaited_once_with(
        "barge-during-playback",
        "customer interruption",
    )

    release_dialog.set()
    await asyncio.wait_for(ingestion, timeout=0.5)


@pytest.mark.asyncio
async def test_customer_turn_is_persisted_before_llm_failure(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "customer-before-llm-failure"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer committed text")
    await transcript_queue.put(None)
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(side_effect=RuntimeError("llm unavailable")),
        ),
        tts_adapter=SimpleNamespace(),
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
        dialog_ready_event=asyncio.Event(),
    )

    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="stream",
        tools=None,
    )
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()

    dialog_task = asyncio.create_task(engine._run_pipeline_dialog(context))
    await asyncio.sleep(0)

    engine._publish_pipeline_customer_turn.assert_not_awaited()
    pipeline.llm_adapter.generate.assert_not_awaited()

    context.dialog_ready_event.set()
    await dialog_task

    assert session.conversation_history[0]["role"] == "user"
    assert session.conversation_history[0]["content"] == "customer committed text"
    assert session.conversation_history[0]["lifecycle_state"] == "customer_committed"
    engine._publish_pipeline_customer_turn.assert_awaited_once()
    pipeline.llm_adapter.generate.assert_awaited_once()
    assert engine._pipeline_turn_trackers[call_id].active_turn.state is TurnLifecycleState.FAILED


@pytest.mark.asyncio
async def test_terminating_pipeline_call_does_not_start_llm_or_tts():
    engine = Engine.__new__(Engine)
    call_id = "terminating-before-dialog-turn"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("不应提交给模型")
    await transcript_queue.put(None)

    class TTSAdapter:
        downstream_mode_override = "file"

        def __init__(self):
            self.calls = 0

        async def synthesize(self, current_call_id, text, options):
            self.calls += 1
            yield b"audio"

    tts_adapter = TTSAdapter()
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value="不应生成的回答"),
        ),
        tts_adapter=tts_adapter,
        tts_options={},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
        dialog_ready_event=None,
    )
    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="file",
        tools=None,
    )
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = {call_id}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()
    engine.playback_manager = SimpleNamespace(
        play_audio=AsyncMock(return_value="playback"),
        wait_for_playback_end=AsyncMock(return_value=True),
    )

    await engine._run_pipeline_dialog(context)

    pipeline.llm_adapter.generate.assert_not_awaited()
    assert tts_adapter.calls == 0
    engine._publish_pipeline_customer_turn.assert_not_awaited()


def test_call_termination_marks_active_assistant_turn_interrupted():
    engine = Engine.__new__(Engine)
    call_id = "terminate-active-assistant"
    tracker = engine._get_pipeline_turn_tracker(call_id)
    turn = tracker.commit_customer("客户问题")
    turn.mark_ai_generating()
    turn.mark_ai_generated("正在播放的回答")
    turn.mark_ai_playing("stream-1", started_at=10.0)

    engine._mark_pipeline_turn_terminated(call_id, reason="call-terminated")

    assert turn.state is TurnLifecycleState.INTERRUPTED
    assert turn.audible_text_complete is False
    assert turn.interruption_reason == "call-terminated"


@pytest.mark.asyncio
async def test_greeting_tts_failure_stops_partial_stream(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "greeting-tts-failure"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)

    class FailingTTS:
        downstream_mode_override = "stream"

        async def synthesize(self, current_call_id, text, options):
            yield b"partial-audio"
            raise RuntimeError("tts exploded")

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}
            self.stop_calls = 0

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            stream_id = "greeting-stream"
            self.active_streams[current_call_id] = {
                "stream_id": stream_id,
                "streaming_task": None,
                "first_real_emit_ts": 10.0,
                "last_real_emit_ts": 10.5,
                "real_tx_bytes": 400,
                "queued_target_total_bytes": 800,
            }
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            self.stop_calls += 1
            self.active_streams.pop(current_call_id, None)
            return True

    manager = StreamingManager()
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=SimpleNamespace(
            tts_adapter=FailingTTS(),
            tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
        ),
        strategy_runtime=None,
    )
    engine.config = SimpleNamespace(
        llm=SimpleNamespace(initial_greeting="hello customer"),
        downstream_mode="stream",
    )
    engine.transport_orchestrator = SimpleNamespace(get_context_config=lambda _name: None)
    engine.streaming_playback_manager = manager
    engine._pipeline_turn_trackers = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._apply_prompt_template_substitution = lambda text, _session: text
    engine._enqueue_pipeline_stream_chunk = AsyncMock(return_value=True)
    engine._publish_pipeline_assistant_turn = AsyncMock()

    await engine._play_pipeline_greeting(context)

    assert manager.stop_calls == 1
    assert call_id not in manager.active_streams
    engine._publish_pipeline_assistant_turn.assert_awaited_once()
    published_turn = engine._publish_pipeline_assistant_turn.await_args.args[1]
    assert published_turn.state is TurnLifecycleState.INTERRUPTED
    assert published_turn.audible_text_complete is False


@pytest.mark.asyncio
async def test_streaming_overlap_failure_after_audio_does_not_retry_serial_llm(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "streaming-overlap-partial-failure"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)

    class StreamingLLM:
        supports_streaming = True

        def __init__(self):
            self.generate = AsyncMock(return_value="duplicate serial answer")

        async def generate_stream(self, current_call_id, transcript, context, options):
            yield "First sentence. "
            raise RuntimeError("stream failed after audio")

    class TTSAdapter:
        downstream_mode_override = "stream"

        async def synthesize(self, current_call_id, text, options):
            yield b"audio"

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            stream_id = "partial-stream"
            self.active_streams[current_call_id] = {
                "stream_id": stream_id,
                "streaming_task": None,
                "first_real_emit_ts": 10.0,
                "last_real_emit_ts": 10.5,
                "real_tx_bytes": 400,
                "queued_target_total_bytes": 800,
            }
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            self.active_streams.pop(current_call_id, None)
            return True

    llm_adapter = StreamingLLM()
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=llm_adapter,
        tts_adapter=TTSAdapter(),
        tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )
    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=True,
        ),
        downstream_mode="stream",
        tools=None,
    )
    engine.streaming_playback_manager = StreamingManager()
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()
    engine._assign_session_provider = lambda current_session, provider: setattr(
        current_session,
        "provider_name",
        provider,
    )

    await engine._run_pipeline_dialog(context)

    llm_adapter.generate.assert_not_awaited()
    engine._publish_pipeline_assistant_turn.assert_awaited_once()
    assert session.conversation_history[-1]["lifecycle_state"] == "interrupted"


@pytest.mark.asyncio
async def test_assistant_final_waits_for_physical_stream_completion(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "assistant-after-playback"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)
    playback_started = asyncio.Event()
    release_playback = asyncio.Event()

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            async def physical_playback():
                playback_started.set()
                await release_playback.wait()

            stream_id = "stream-physical"
            self.active_streams[current_call_id] = {
                "stream_id": stream_id,
                "streaming_task": asyncio.create_task(physical_playback()),
                "first_real_emit_ts": 10.0,
                "last_real_emit_ts": 12.0,
                "tx_bytes": 8000,
                "queued_total_bytes": 8000,
                "end_reason": "end-of-stream",
            }
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            return True

    class TTSAdapter:
        downstream_mode_override = "stream"

        async def synthesize(self, current_call_id, text, options):
            yield b"audio"

    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value="assistant response"),
        ),
        tts_adapter=TTSAdapter(),
        tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )

    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="stream",
        tools=None,
    )
    engine.streaming_playback_manager = StreamingManager()
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()
    engine._assign_session_provider = lambda current_session, provider: setattr(
        current_session,
        "provider_name",
        provider,
    )

    dialog_task = asyncio.create_task(engine._run_pipeline_dialog(context))
    await asyncio.wait_for(playback_started.wait(), timeout=0.5)
    await asyncio.sleep(0)

    engine._publish_pipeline_assistant_turn.assert_not_awaited()
    assert not dialog_task.done()

    release_playback.set()
    await asyncio.wait_for(dialog_task, timeout=0.5)

    engine._publish_pipeline_assistant_turn.assert_awaited_once()
    assert session.conversation_history[-1]["role"] == "assistant"
    assert session.conversation_history[-1]["lifecycle_state"] == "completed"


@pytest.mark.asyncio
async def test_tool_only_continuation_uses_active_turn_lifecycle(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "tool-only-continuation"
    from src.core.models import CallSession
    from src.tools.registry import tool_registry

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("please run the tool")
    await transcript_queue.put(None)

    llm_adapter = SimpleNamespace(
        supports_streaming=False,
        generate=AsyncMock(
            side_effect=[
                LLMResponse(
                    text="",
                    tool_calls=[
                        {"name": "request_transcript", "parameters": {}}
                    ],
                ),
                LLMResponse(text="The tool has completed."),
            ]
        ),
    )
    pipeline = SimpleNamespace(
        llm_options={
            "turn_settlement_timeout_sec": 0,
            "tools": ["request_transcript"],
        },
        llm_adapter=llm_adapter,
        tts_adapter=SimpleNamespace(),
        tts_options={},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={"tools": ["request_transcript"]},
        transcript_queue=transcript_queue,
    )

    class FakeTool:
        slow_response_threshold_ms = 0
        slow_response_message = ""

        async def execute(self, args, tool_context):
            return {"status": "success", "message": "tool result"}

    fake_tool = FakeTool()
    monkeypatch.setattr(
        tool_registry,
        "get",
        lambda name: fake_tool if name == "request_transcript" else None,
    )
    monkeypatch.setattr(tool_registry, "canonicalize_tool_name", lambda name: name)

    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="stream",
        tools=None,
        dict=lambda: {},
    )
    engine.ari_client = SimpleNamespace()
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._play_pipeline_followup_turn = AsyncMock()

    await engine._run_pipeline_dialog(context)

    engine._play_pipeline_followup_turn.assert_awaited_once()
    followup_args = engine._play_pipeline_followup_turn.await_args.args
    assert followup_args[0] == call_id
    assert followup_args[3] is engine._pipeline_turn_trackers[call_id].active_turn
    assert followup_args[4] == "The tool has completed."


@pytest.mark.asyncio
async def test_tool_followup_final_waits_for_physical_playback(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "tool-followup-physical-playback"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    playback_started = asyncio.Event()
    release_playback = asyncio.Event()

    async def wait_for_playback_end(*_args, **_kwargs):
        playback_started.set()
        await release_playback.wait()
        return True

    async def synthesize(*_args, **_kwargs):
        yield b"followup-audio"

    engine.playback_manager = SimpleNamespace(
        play_audio=AsyncMock(return_value="followup-playback"),
        wait_for_playback_end=AsyncMock(side_effect=wait_for_playback_end),
    )
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._publish_pipeline_assistant_turn = AsyncMock()
    pipeline = SimpleNamespace(
        tts_adapter=SimpleNamespace(synthesize=synthesize),
        tts_options={},
    )
    turn = engine._get_pipeline_turn_tracker(call_id).commit_customer("customer text")
    turn.mark_ai_generating()
    conversation_history = []

    followup_task = asyncio.create_task(
        engine._play_pipeline_followup_turn(
            call_id,
            session,
            pipeline,
            turn,
            "The tool has completed.",
            conversation_history,
        )
    )
    await asyncio.wait_for(playback_started.wait(), timeout=0.5)

    engine._publish_pipeline_assistant_turn.assert_not_awaited()
    assert conversation_history == []

    release_playback.set()
    await asyncio.wait_for(followup_task, timeout=0.5)

    engine._publish_pipeline_assistant_turn.assert_awaited_once_with(call_id, turn)
    assert turn.state is TurnLifecycleState.COMPLETED
    assert conversation_history[-1]["turn_id"] == turn.turn_id
    assert conversation_history[-1]["lifecycle_state"] == "completed"


@pytest.mark.asyncio
async def test_streaming_tts_failure_after_audio_does_not_replay_full_file(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "streaming-tts-partial-failure"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)

    class FailingTTSAdapter:
        downstream_mode_override = "stream"

        def __init__(self):
            self.synthesize_calls = 0

        async def synthesize(self, current_call_id, text, options):
            self.synthesize_calls += 1
            yield b"partial-audio"
            raise RuntimeError("tts failed after audio")

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            stream_id = "partial-stream"
            self.active_streams[current_call_id] = {
                "stream_id": stream_id,
                "streaming_task": None,
                "first_real_emit_ts": 10.0,
                "last_real_emit_ts": 10.5,
                "real_tx_bytes": 400,
                "queued_target_total_bytes": 800,
            }
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            self.active_streams.pop(current_call_id, None)
            return True

    tts_adapter = FailingTTSAdapter()
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value="assistant response"),
        ),
        tts_adapter=tts_adapter,
        tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )

    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="stream",
        tools=None,
    )
    engine.streaming_playback_manager = StreamingManager()
    engine.playback_manager = SimpleNamespace(
        play_audio=AsyncMock(return_value="file-playback"),
        wait_for_playback_end=AsyncMock(return_value=True),
    )
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()
    engine._assign_session_provider = lambda current_session, provider: setattr(
        current_session,
        "provider_name",
        provider,
    )

    await engine._run_pipeline_dialog(context)

    assert tts_adapter.synthesize_calls == 1
    engine.playback_manager.play_audio.assert_not_awaited()
    engine._publish_pipeline_assistant_turn.assert_awaited_once()
    assert session.conversation_history[-1]["lifecycle_state"] == "interrupted"


@pytest.mark.asyncio
async def test_streaming_tts_failure_before_audio_uses_file_fallback(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "streaming-tts-pre-audio-failure"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)

    class RecoveringTTSAdapter:
        downstream_mode_override = "stream"

        def __init__(self):
            self.synthesize_calls = 0

        async def synthesize(self, current_call_id, text, options):
            self.synthesize_calls += 1
            if self.synthesize_calls == 1:
                raise RuntimeError("stream failed before audio")
            yield b"complete-file-audio"

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            stream_id = "empty-stream"
            self.active_streams[current_call_id] = {
                "stream_id": stream_id,
                "streaming_task": None,
                "real_tx_bytes": 0,
                "queued_target_total_bytes": 0,
            }
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            self.active_streams.pop(current_call_id, None)
            return True

    tts_adapter = RecoveringTTSAdapter()
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value="assistant response"),
        ),
        tts_adapter=tts_adapter,
        tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )

    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="stream",
        tools=None,
    )
    engine.streaming_playback_manager = StreamingManager()
    engine.playback_manager = SimpleNamespace(
        play_audio=AsyncMock(return_value="file-playback"),
        wait_for_playback_end=AsyncMock(return_value=True),
    )
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()
    engine._assign_session_provider = lambda current_session, provider: setattr(
        current_session,
        "provider_name",
        provider,
    )

    await engine._run_pipeline_dialog(context)

    assert tts_adapter.synthesize_calls == 2
    engine.playback_manager.play_audio.assert_awaited_once()
    engine._publish_pipeline_assistant_turn.assert_awaited_once()
    assert session.conversation_history[-1]["lifecycle_state"] == "completed"


@pytest.mark.asyncio
async def test_greeting_session_retry_clears_failed_attempt_state(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "greeting-session-retry"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)

    class RetryingTTS:
        downstream_mode_override = "stream"

        def __init__(self):
            self.synthesize_calls = 0
            self.open_calls = 0

        async def synthesize(self, current_call_id, text, options):
            self.synthesize_calls += 1
            if self.synthesize_calls == 1:
                raise RuntimeError("session expired")
            yield b"greeting-audio"

        async def open_call(self, current_call_id, options):
            self.open_calls += 1

    class StreamingManager:
        def __init__(self):
            self.active_streams = {}
            self.start_calls = 0

        async def start_streaming_playback(self, current_call_id, queue, **_kwargs):
            self.start_calls += 1
            stream_id = f"greeting-stream-{self.start_calls}"
            info = {
                "stream_id": stream_id,
                "streaming_task": None,
                "real_tx_bytes": 0,
                "queued_target_total_bytes": 0,
            }
            if self.start_calls == 2:
                info.update(
                    {
                        "first_real_emit_ts": 10.0,
                        "last_real_emit_ts": 11.0,
                        "real_tx_bytes": 8000,
                        "queued_target_total_bytes": 8000,
                        "end_reason": "end-of-stream",
                    }
                )
            self.active_streams[current_call_id] = info
            return stream_id

        def is_stream_active(self, current_call_id, stream_id=None):
            info = self.active_streams.get(current_call_id)
            return bool(info and info["stream_id"] == stream_id)

        async def stop_streaming_playback(self, current_call_id):
            self.active_streams.pop(current_call_id, None)
            return True

    tts_adapter = RetryingTTS()
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=SimpleNamespace(
            tts_adapter=tts_adapter,
            tts_options={"format": {"encoding": "ulaw", "sample_rate": 8000}},
        ),
        strategy_runtime=None,
    )
    engine.config = SimpleNamespace(
        llm=SimpleNamespace(initial_greeting="hello customer"),
        downstream_mode="stream",
    )
    engine.transport_orchestrator = SimpleNamespace(get_context_config=lambda _name: None)
    engine.streaming_playback_manager = StreamingManager()
    engine._pipeline_turn_trackers = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._apply_prompt_template_substitution = lambda text, _session: text
    engine._enqueue_pipeline_stream_chunk = AsyncMock(return_value=True)
    engine._publish_pipeline_assistant_turn = AsyncMock()

    await engine._play_pipeline_greeting(context)

    greeting_turn = engine._pipeline_turn_trackers[call_id].greeting_turn
    assert tts_adapter.synthesize_calls == 2
    assert tts_adapter.open_calls == 1
    assert greeting_turn.state is TurnLifecycleState.COMPLETED
    assert greeting_turn.failure_reason is None
    engine._publish_pipeline_assistant_turn.assert_awaited_once_with(call_id, greeting_turn)


@pytest.mark.asyncio
async def test_empty_llm_response_marks_turn_failed(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "empty-llm-response"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)
    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value=LLMResponse(text="")),
        ),
        tts_adapter=SimpleNamespace(),
        tts_options={},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )
    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="file",
        tools=None,
    )
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()

    await engine._run_pipeline_dialog(context)

    turn = engine._pipeline_turn_trackers[call_id].active_turn
    assert turn.state is TurnLifecycleState.FAILED
    assert turn.failure_reason == "llm_empty_response"


@pytest.mark.asyncio
async def test_file_tts_without_audio_marks_turn_failed(monkeypatch):
    engine = Engine.__new__(Engine)
    call_id = "file-tts-no-audio"
    from src.core.models import CallSession

    session = CallSession(call_id=call_id, caller_channel_id=call_id)
    session.pipeline_name = "local-only"
    session.provider_name = "pipeline"
    transcript_queue = asyncio.Queue()
    await transcript_queue.put("customer text")
    await transcript_queue.put(None)

    async def synthesize(*_args, **_kwargs):
        if False:
            yield b""

    pipeline = SimpleNamespace(
        llm_options={"turn_settlement_timeout_sec": 0},
        llm_adapter=SimpleNamespace(
            supports_streaming=False,
            generate=AsyncMock(return_value="assistant response"),
        ),
        tts_adapter=SimpleNamespace(
            downstream_mode_override="file",
            synthesize=synthesize,
        ),
        tts_options={},
    )
    context = SimpleNamespace(
        call_id=call_id,
        session=session,
        pipeline=pipeline,
        strategy_runtime=None,
        llm_options={},
        transcript_queue=transcript_queue,
    )
    engine.config = SimpleNamespace(
        streaming=SimpleNamespace(
            pipeline_filler_enabled=False,
            pipeline_streaming_overlap=False,
        ),
        downstream_mode="file",
        tools=None,
    )
    engine.playback_manager = SimpleNamespace(play_audio=AsyncMock())
    engine._pipeline_turn_trackers = {}
    engine._pipeline_barge_in_candidates = {}
    engine._last_transcript_ts = {}
    engine.session_store = SimpleNamespace(upsert_call=AsyncMock())
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    engine._publish_pipeline_customer_turn = AsyncMock()
    engine._publish_pipeline_assistant_turn = AsyncMock()

    await engine._run_pipeline_dialog(context)

    turn = engine._pipeline_turn_trackers[call_id].active_turn
    assert turn.state is TurnLifecycleState.FAILED
    assert turn.failure_reason == "tts_no_audio"
    engine.playback_manager.play_audio.assert_not_awaited()
    engine._publish_pipeline_assistant_turn.assert_not_awaited()


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
        dialog_ready_event=asyncio.Event(),
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
    assert context.dialog_ready_event.is_set()


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
