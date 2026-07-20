import pytest
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.core.conversation_coordinator import ConversationCoordinator
from src.core.models import CallSession
from src.core.session_store import SessionStore
from src.core.streaming_playback_manager import StreamingPlaybackManager
from src.audio.resampler import pcm16le_to_mulaw
from src.engine import Engine


class _DummyARI:
    pass


@pytest.mark.asyncio
async def test_end_segment_gating_only_clears_once_with_coordinator(monkeypatch):
    """
    Regression test: end_segment_gating must not clear gating twice when a
    ConversationCoordinator is present.
    """
    session_store = SessionStore()
    call_id = "call-1"
    stream_id = "stream-1"

    await session_store.upsert_call(
        CallSession(call_id=call_id, caller_channel_id="caller-1", provider_name="local")
    )

    coordinator = ConversationCoordinator(session_store)
    await coordinator.on_tts_start(call_id, stream_id)

    original_clear = session_store.clear_gating_token
    mocked_clear = AsyncMock(side_effect=original_clear)
    monkeypatch.setattr(session_store, "clear_gating_token", mocked_clear)

    mgr = StreamingPlaybackManager(
        session_store=session_store,
        ari_client=_DummyARI(),
        conversation_coordinator=coordinator,
        streaming_config={},
    )
    mgr.active_streams[call_id] = {"stream_id": stream_id}

    await mgr.end_segment_gating(call_id)

    assert mocked_clear.await_count == 1


@pytest.mark.asyncio
async def test_start_streaming_playback_normalizes_audiosocket_slin(monkeypatch):
    session_store = SessionStore()
    call_id = "call-slin"
    await session_store.upsert_call(
        CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    )

    mgr = StreamingPlaybackManager(
        session_store=session_store,
        ari_client=_DummyARI(),
        conversation_coordinator=None,
        streaming_config={},
        audio_transport="audiosocket",
    )
    mgr.audiosocket_format = "slin"

    class _DummyTask:
        def cancel(self):
            return None

    def _fake_create_task(coro):
        try:
            coro.close()
        except Exception:
            pass
        return _DummyTask()

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)

    q: asyncio.Queue = asyncio.Queue()
    stream_id = await mgr.start_streaming_playback(
        call_id,
        q,
        playback_type="pipeline-tts",
        source_encoding="mulaw",
        source_sample_rate=8000,
    )
    assert stream_id is not None
    info = mgr.active_streams[call_id]
    assert info.get("target_format") == "slin"
    assert info.get("target_sample_rate") == 8000


@pytest.mark.asyncio
async def test_start_streaming_playback_normalizes_externalmedia_ulaw(monkeypatch):
    session_store = SessionStore()
    call_id = "call-ulaw"
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.external_media_codec = "ulaw"
    await session_store.upsert_call(session)

    mgr = StreamingPlaybackManager(
        session_store=session_store,
        ari_client=_DummyARI(),
        conversation_coordinator=None,
        streaming_config={},
        audio_transport="externalmedia",
    )
    mgr.audiosocket_format = "slin"

    class _DummyTask:
        def cancel(self):
            return None

    def _fake_create_task(coro):
        try:
            coro.close()
        except Exception:
            pass
        return _DummyTask()

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)

    q: asyncio.Queue = asyncio.Queue()
    stream_id = await mgr.start_streaming_playback(
        call_id,
        q,
        playback_type="pipeline-tts",
        source_encoding="mulaw",
        source_sample_rate=8000,
    )
    assert stream_id is not None
    info = mgr.active_streams[call_id]
    assert info.get("target_format") == "ulaw"
    assert info.get("target_sample_rate") == 8000


@pytest.mark.asyncio
async def test_stop_streaming_playback_can_drain_native_owner_task():
    mgr = StreamingPlaybackManager(
        session_store=SessionStore(),
        ari_client=_DummyARI(),
        conversation_coordinator=None,
        streaming_config={},
    )
    call_id = "drain-native-stream"
    release = asyncio.Event()

    async def finish_stream():
        await release.wait()
        mgr.active_streams.pop(call_id, None)

    owner_task = asyncio.create_task(finish_stream())
    mgr.active_streams[call_id] = {
        "stream_id": "stream-drain",
        "streaming_task": owner_task,
    }

    drain_task = asyncio.create_task(
        mgr.stop_streaming_playback(call_id, drain=True, drain_timeout=1.0)
    )
    await asyncio.sleep(0)
    assert not drain_task.done()

    release.set()
    assert await asyncio.wait_for(drain_task, timeout=1.0) is True
    assert call_id not in mgr.active_streams


@pytest.mark.asyncio
@pytest.mark.parametrize("playback_type", ["strategy-opening", "strategy-tts"])
async def test_strategy_audio_trims_leading_mulaw_silence_with_preroll(playback_type):
    mgr = StreamingPlaybackManager(
        session_store=SessionStore(),
        ari_client=_DummyARI(),
        conversation_coordinator=None,
        streaming_config={},
        audio_transport="audiosocket",
    )
    call_id = "strategy-leading-silence"
    mgr.audiosocket_format = "slin"
    mgr.active_streams[call_id] = {
        "playback_type": playback_type,
        "source_encoding": "mulaw",
        "source_sample_rate": 8000,
        "target_format": "slin",
        "target_sample_rate": 8000,
    }

    silence = b"\xff" * 160
    for _ in range(125):
        assert await mgr._process_audio_chunk(call_id, silence) is None

    active_pcm = (1000).to_bytes(2, "little", signed=True) * 160
    active_mulaw = pcm16le_to_mulaw(active_pcm)
    first_audio = await mgr._process_audio_chunk(call_id, active_mulaw)

    assert first_audio is not None
    assert len(first_audio) == 1280
    assert mgr.active_streams[call_id]["leading_silence_trim_done"] is True
    assert mgr.active_streams[call_id]["leading_silence_trimmed_bytes"] == 39040

    next_audio = await mgr._process_audio_chunk(call_id, active_mulaw)
    assert next_audio is not None
    assert len(next_audio) == 320


@pytest.mark.asyncio
async def test_strategy_opening_remains_interruptible():
    call_id = "interruptible-opening"
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.media_rx_confirmed = True

    action_order = []

    async def stop_playback_action(_call_id):
        action_order.append("stop-playback")

    def mark_strategy_stale(_call_id):
        action_order.append("mark-strategy-stale")
        return "turn-1"

    stop_playback = AsyncMock(side_effect=stop_playback_action)
    engine = object.__new__(Engine)
    engine.session_store = SimpleNamespace(
        get_by_call_id=AsyncMock(return_value=session),
        list_playbacks_for_call=AsyncMock(return_value=[]),
        clear_gating_token=AsyncMock(),
    )
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={call_id: {"playback_type": "strategy-opening"}},
        stop_streaming_playback=stop_playback,
    )
    engine.strategy_session_manager = SimpleNamespace(mark_barge_in=mark_strategy_stale)
    engine._provider_stream_queues = {}
    engine._provider_stream_formats = {}
    engine._provider_coalesce_buf = {}
    engine.ari_client = SimpleNamespace(stop_playback=AsyncMock())
    engine.conversation_coordinator = None
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(provider_output_suppress_ms=0),
    )
    engine._save_session = AsyncMock()
    engine._call_providers = {}

    applied = await engine._apply_barge_in_action(
        call_id,
        source="talkdetect",
        reason="ChannelTalkingStarted",
    )

    assert applied is True
    assert action_order[:2] == ["mark-strategy-stale", "stop-playback"]
    stop_playback.assert_awaited_once_with(call_id)


@pytest.mark.asyncio
async def test_strategy_turn_waits_for_first_audio_before_starting_playback():
    call_id = "strategy-waits-for-audio"
    response_waiting = asyncio.Event()
    release_response = asyncio.Event()

    class _DelayedStrategy:
        async def stream_turn(self, *_args, **_kwargs):
            response_waiting.set()
            await release_response.wait()
            yield {"turn_id": "turn-1", "index": 0, "text": "response"}

        def is_turn_stale(self, *_args, **_kwargs):
            return False

    class _TTS:
        async def synthesize(self, *_args, **_kwargs):
            yield b"\x7f" * 160

    start_playback = AsyncMock(return_value="stream-1")
    stop_playback = AsyncMock(return_value=True)
    engine = object.__new__(Engine)
    engine.strategy_session_manager = _DelayedStrategy()
    engine.streaming_playback_manager = SimpleNamespace(
        start_streaming_playback=start_playback,
        is_stream_active=lambda _call_id, stream_id=None: stream_id == "stream-1",
        stop_streaming_playback=stop_playback,
    )
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=_TTS(),
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )

    turn_task = asyncio.create_task(
        engine._run_strategy_pipeline_turn(
            call_id,
            session,
            pipeline,
            {},
            "customer response",
            [],
        )
    )
    await asyncio.wait_for(response_waiting.wait(), timeout=1.0)

    start_playback.assert_not_awaited()

    release_response.set()
    await asyncio.wait_for(turn_task, timeout=1.0)
    start_playback.assert_awaited_once()
    stream_queue = start_playback.await_args.args[1]
    assert stream_queue.get_nowait() == b"\x7f" * 160
    assert stream_queue.get_nowait() is None
    stop_playback.assert_awaited_once_with(call_id, drain=True)


@pytest.mark.asyncio
async def test_stale_strategy_turn_does_not_start_empty_playback():
    call_id = "stale-strategy-turn"

    class _StaleStrategy:
        async def stream_turn(self, *_args, **_kwargs):
            yield {"turn_id": "turn-stale", "index": 0, "text": "old response"}

        def is_turn_stale(self, *_args, **_kwargs):
            return True

    class _TTS:
        def __init__(self):
            self.calls = 0

        async def synthesize(self, *_args, **_kwargs):
            self.calls += 1
            yield b"\x7f" * 160

    tts = _TTS()
    start_playback = AsyncMock(return_value="stream-stale")
    engine = object.__new__(Engine)
    engine.strategy_session_manager = _StaleStrategy()
    engine.streaming_playback_manager = SimpleNamespace(
        start_streaming_playback=start_playback,
    )
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=tts,
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )

    await engine._run_strategy_pipeline_turn(
        call_id,
        session,
        pipeline,
        {},
        "newer customer response",
        [],
    )

    start_playback.assert_not_awaited()
    assert tts.calls == 0


@pytest.mark.asyncio
async def test_barge_in_during_tts_drains_strategy_turn_before_returning():
    call_id = "strategy-barge-in-drains-turn"

    class _DrainableStrategy:
        def __init__(self):
            self.stale = False
            self.drained = asyncio.Event()
            self.stream = None

        async def _segments(self):
            try:
                yield {"turn_id": "turn-drain", "index": 0, "text": "first response"}
                yield {"turn_id": "turn-drain", "index": 1, "text": "late second response"}
            finally:
                self.drained.set()

        def stream_turn(self, *_args, **_kwargs):
            self.stream = self._segments()
            return self.stream

        def is_turn_stale(self, *_args, **_kwargs):
            return self.stale

    class _InterruptingTTS:
        def __init__(self, strategy):
            self.strategy = strategy
            self.calls = []

        async def synthesize(self, _call_id, text, _options):
            self.calls.append(text)
            yield b"\x7f" * 160
            self.strategy.stale = True
            yield b"\x7f" * 160

    strategy = _DrainableStrategy()
    tts = _InterruptingTTS(strategy)
    start_playback = AsyncMock(return_value="stream-drain")
    stop_playback = AsyncMock(return_value=True)
    engine = object.__new__(Engine)
    engine.strategy_session_manager = strategy
    engine.streaming_playback_manager = SimpleNamespace(
        start_streaming_playback=start_playback,
        is_stream_active=lambda _call_id, stream_id=None: stream_id == "stream-drain",
        stop_streaming_playback=stop_playback,
    )
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=tts,
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )

    await engine._run_strategy_pipeline_turn(
        call_id,
        session,
        pipeline,
        {},
        "customer response",
        [],
    )

    assert strategy.drained.is_set()
    assert tts.calls == ["first response"]
    start_playback.assert_awaited_once()
    stop_playback.assert_awaited_once_with(call_id)


@pytest.mark.asyncio
async def test_barge_in_during_optional_second_window_skips_eos_on_stopped_stream():
    call_id = "strategy-barge-in-after-first-segment"
    playback_state = SimpleNamespace(active=True)

    class _OptionalSecondWindowStrategy:
        def __init__(self):
            self.stale = False
            self.cleared = []

        async def stream_turn(self, *_args, **_kwargs):
            yield {"turn_id": "turn-window", "index": 0, "text": "first response"}
            # Model a barge-in while V1 waits for an optional response[1].
            self.stale = True
            playback_state.active = False

        def is_turn_stale(self, *_args, **_kwargs):
            return self.stale

        def clear_stale_turn(self, _call_id, turn_id):
            self.cleared.append(turn_id)
            self.stale = False

    class _TTS:
        async def synthesize(self, *_args, **_kwargs):
            yield b"\x7f" * 160

    strategy = _OptionalSecondWindowStrategy()
    start_playback = AsyncMock(return_value="stream-window")
    engine = object.__new__(Engine)
    engine.strategy_session_manager = strategy
    engine.streaming_playback_manager = SimpleNamespace(
        start_streaming_playback=start_playback,
        is_stream_active=lambda _call_id, stream_id=None: (
            playback_state.active and stream_id == "stream-window"
        ),
        stop_streaming_playback=AsyncMock(return_value=True),
    )
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=_TTS(),
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )

    await engine._run_strategy_pipeline_turn(
        call_id,
        session,
        pipeline,
        {},
        "customer response",
        [],
    )

    stream_queue = start_playback.await_args.args[1]
    assert stream_queue.get_nowait() == b"\x7f" * 160
    assert stream_queue.empty()
    assert len(strategy.cleared) == 1


@pytest.mark.asyncio
async def test_barge_in_unblocks_full_strategy_audio_queue_and_releases_turn():
    call_id = "strategy-barge-in-full-queue"

    class _DrainableStrategy:
        def __init__(self):
            self.stale = False
            self.drained = asyncio.Event()
            self.stream = None

        async def _segments(self):
            try:
                yield {"turn_id": "turn-full", "index": 0, "text": "fast response"}
                yield {"turn_id": "turn-full", "index": 1, "text": "late response"}
            finally:
                self.drained.set()

        def stream_turn(self, *_args, **_kwargs):
            self.stream = self._segments()
            return self.stream

        def is_turn_stale(self, *_args, **_kwargs):
            return self.stale

    class _FastTTS:
        def __init__(self):
            self.queue_saturated = asyncio.Event()

        async def synthesize(self, *_args, **_kwargs):
            for index in range(300):
                if index == 256:
                    self.queue_saturated.set()
                yield b"\x7f" * 160

    strategy = _DrainableStrategy()
    tts = _FastTTS()
    playback_state = SimpleNamespace(active=True)
    engine = object.__new__(Engine)
    engine.strategy_session_manager = strategy
    engine.streaming_playback_manager = SimpleNamespace(
        start_streaming_playback=AsyncMock(return_value="stream-full"),
        is_stream_active=lambda _call_id, stream_id=None: (
            playback_state.active and stream_id == "stream-full"
        ),
        stop_streaming_playback=AsyncMock(return_value=True),
    )
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=tts,
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )

    turn_task = asyncio.create_task(
        engine._run_strategy_pipeline_turn(
            call_id,
            session,
            pipeline,
            {},
            "customer response",
            [],
        )
    )
    await asyncio.wait_for(tts.queue_saturated.wait(), timeout=1.0)
    strategy.stale = True
    playback_state.active = False

    await asyncio.wait_for(turn_task, timeout=1.0)
    assert strategy.drained.is_set()


@pytest.mark.asyncio
async def test_consecutive_strategy_turns_wait_for_native_playback_cleanup():
    call_id = "strategy-consecutive-turns"

    class _TwoTurnStrategy:
        def __init__(self):
            self.inputs = []

        async def stream_turn(self, _call_id, text, *, turn_id):
            self.inputs.append((turn_id, text))
            yield {"turn_id": turn_id, "index": 0, "text": f"reply to {text}"}

        def is_turn_stale(self, *_args, **_kwargs):
            return False

    class _FastTTS:
        async def synthesize(self, *_args, **_kwargs):
            for _ in range(300):
                yield b"\x7f" * 160

    class _NativePlaybackLifecycle:
        """Model AVA's active-stream reuse and delayed pacer cleanup."""

        def __init__(self):
            self.active_streams = {}
            self.started_queues = []
            self._sequence = 0

        def is_stream_active(self, call_id, stream_id=None):
            info = self.active_streams.get(call_id)
            if not info:
                return False
            if stream_id is not None and info["stream_id"] != stream_id:
                return False
            return not info["streaming_task"].done()

        async def start_streaming_playback(self, call_id, queue, **_kwargs):
            if self.is_stream_active(call_id):
                return self.active_streams[call_id]["stream_id"]

            self._sequence += 1
            stream_id = f"stream-{self._sequence}"
            self.started_queues.append(queue)

            async def consume():
                while await queue.get() is not None:
                    pass
                # AVA still owns the slot while its jitter buffer and pacer drain.
                await asyncio.sleep(0.05)

            task = asyncio.create_task(consume())
            self.active_streams[call_id] = {
                "stream_id": stream_id,
                "streaming_task": task,
            }
            return stream_id

        async def stop_streaming_playback(
            self,
            call_id,
            *,
            drain=False,
            drain_timeout=120.0,
        ):
            info = self.active_streams.get(call_id)
            if not info:
                return False
            task = info["streaming_task"]
            if drain:
                await asyncio.wait_for(asyncio.shield(task), timeout=drain_timeout)
            elif not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if self.active_streams.get(call_id) is info:
                self.active_streams.pop(call_id, None)
            return True

    strategy = _TwoTurnStrategy()
    manager = _NativePlaybackLifecycle()
    engine = object.__new__(Engine)
    engine.strategy_session_manager = strategy
    engine.streaming_playback_manager = manager
    engine._publish_transcript_to_voiceai = AsyncMock()
    engine._save_session = AsyncMock()
    session = CallSession(
        call_id=call_id,
        caller_channel_id=call_id,
        provider_name="pipeline",
    )
    pipeline = SimpleNamespace(
        tts_adapter=_FastTTS(),
        tts_options={"format": {"encoding": "mulaw", "sample_rate": 8000}},
    )
    history = []

    await asyncio.wait_for(
        engine._run_strategy_pipeline_turn(
            call_id,
            session,
            pipeline,
            {},
            "first question",
            history,
        ),
        timeout=1.0,
    )
    await asyncio.wait_for(
        engine._run_strategy_pipeline_turn(
            call_id,
            session,
            pipeline,
            {},
            "second question",
            history,
        ),
        timeout=1.0,
    )

    assert len(manager.started_queues) == 2
    assert [text for _, text in strategy.inputs] == ["first question", "second question"]
    assert not manager.is_stream_active(call_id)
