import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.config import BargeInConfig
from src.core.models import CallSession
from src.core.session_store import SessionStore
from src.core.streaming_playback_manager import StreamingPlaybackManager
from src.engine import Engine


class _DummyARI:
    pass


def test_greeting_protection_window_is_shared_by_native_provider_paths():
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=BargeInConfig())
    session = CallSession(call_id="native-greeting", caller_channel_id="native-greeting")
    session.conversation_state = "greeting"
    session.tts_playing = True
    session.tts_started_ts = 100.0

    assert engine._greeting_protection_remaining_ms(session, now=102.9) == 100
    assert engine._greeting_protection_remaining_ms(session, now=103.01) == 0

    session.conversation_state = "listening"
    assert engine._greeting_protection_remaining_ms(session, now=101.0) == 0


@pytest.mark.asyncio
async def test_pipeline_greeting_blocks_talkdetect_for_three_seconds():
    call_id = "greeting-protection"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_playing = True
    session.media_rx_confirmed = True
    session.conversation_state = "greeting"
    session.tts_started_ts = time.time()
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.config = SimpleNamespace(barge_in=BargeInConfig())
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    event = {"channel": {"id": call_id}}
    await engine._handle_channel_talking_started(event)
    engine._start_pipeline_barge_in_candidate.assert_not_awaited()

    session.tts_started_ts = time.time() - 2.9
    await engine._handle_channel_talking_started(event)
    engine._start_pipeline_barge_in_candidate.assert_not_awaited()

    session.tts_started_ts = time.time() - 3.01
    await engine._handle_channel_talking_started(event)
    engine._start_pipeline_barge_in_candidate.assert_awaited_once_with(
        session,
        source="talkdetect",
    )


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_combines_finals_before_one_commit():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put("啊，是的，是的。")
        await asyncio.sleep(0.01)
        await transcript_queue.put("是有什么事吗？")
        await asyncio.sleep(0.06)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "settled-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == ["啊，是的，是的。 是有什么事吗？"]
    assert engine._confirm_pipeline_barge_in_candidate.await_count == 2


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_waits_for_partial_continuation():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put({
            "text": "嗯，是的，是的。",
            "is_final": True,
            "is_partial": False,
        })
        # Mirrors the recorded call: continuation activity arrives just before
        # the settlement deadline, while its final arrives later.
        await asyncio.sleep(0.034)
        await transcript_queue.put({
            "text": "是",
            "is_final": False,
            "is_partial": True,
        })
        await asyncio.sleep(0.02)
        await transcript_queue.put({
            "text": "是什么",
            "is_final": False,
            "is_partial": True,
        })
        await asyncio.sleep(0.02)
        await transcript_queue.put({
            "text": "是什么事？",
            "is_final": True,
            "is_partial": False,
        })
        await asyncio.sleep(0.02)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "partial-continuation-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == ["嗯，是的，是的。 是什么事？"]
    assert engine._confirm_pipeline_barge_in_candidate.await_count == 2


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_waits_for_sparse_partial_final():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put({
            "text": "first",
            "is_final": True,
            "is_partial": False,
        })
        await asyncio.sleep(0.034)
        await transcript_queue.put({
            "text": "continu",
            "is_final": False,
            "is_partial": True,
        })
        # Qwen realtime can wait for server VAD before emitting the final, so
        # the final may arrive after the ordinary settlement window.
        await asyncio.sleep(0.05)
        await transcript_queue.put({
            "text": "continuation",
            "is_final": True,
            "is_partial": False,
        })
        await asyncio.sleep(0.05)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "sparse-partial-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == ["first continuation"]
    assert engine._confirm_pipeline_barge_in_candidate.await_count == 2


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_uses_latest_cumulative_final():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put("啊，是的，是的。")
        await asyncio.sleep(0.01)
        await transcript_queue.put("啊，是的，是的。 是有什么事吗？")
        await asyncio.sleep(0.06)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "cumulative-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == ["啊，是的，是的。 是有什么事吗？"]


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_keeps_repeated_text_in_separate_turns():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put("嗯。")
        await asyncio.sleep(0.05)
        await transcript_queue.put("嗯。")
        await asyncio.sleep(0.05)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "repeat-call",
            transcript_queue,
            settle_seconds=0.03,
        )
    ]
    await producer

    assert committed == ["嗯。", "嗯。"]


@pytest.mark.asyncio
async def test_pipeline_source_timeline_replaces_short_onset_and_merges_continuation():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put({
            "text": "Oh.",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-onset",
            "item_id": "item-1",
            "audio_start_ms": 0,
            "audio_end_ms": 160,
            "audio_duration_ms": 160,
            "received_at_ms": 160,
        })
        # The ASR has not emitted the revised/full text before the ordinary
        # settlement boundary, but the source stream is still in the same item.
        await asyncio.sleep(0.05)
        await transcript_queue.put({
            "text": "",
            "is_final": False,
            "is_partial": True,
            "source_activity": "speech_started",
            "item_id": "item-1",
            "audio_start_ms": 120,
            "received_at_ms": 200,
        })
        await transcript_queue.put({
            "text": "啊，我不知道。",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-revision",
            "item_id": "item-1",
            "audio_start_ms": 0,
            "audio_end_ms": 800,
            "audio_duration_ms": 800,
            "received_at_ms": 800,
        })
        await transcript_queue.put({
            "text": "那怎么办呢？",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-continuation",
            "item_id": "item-2",
            "audio_start_ms": 820,
            "audio_end_ms": 1500,
            "audio_duration_ms": 680,
            "received_at_ms": 1500,
        })
        await asyncio.sleep(0.05)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "source-timeline-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == ["啊，我不知道。 那怎么办呢？"]


@pytest.mark.asyncio
async def test_pipeline_settlement_waits_full_window_after_delayed_final_delivery():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put({
            "text": "呃，可能是我。",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-unemployment-prefix",
            "item_id": "item-1",
            "audio_start_ms": 0,
            "audio_end_ms": 2016,
            "audio_duration_ms": 2016,
            "received_at_ms": 2240,
        })
        # The next source activity reaches AVA 292ms after the final. Its
        # source-audio gap is only 184ms, so the 400ms settlement window must
        # start when AVA receives the final instead of subtracting old silence.
        await asyncio.sleep(0.292)
        await transcript_queue.put({
            "text": "",
            "is_final": False,
            "is_partial": True,
            "source_activity": "speech_started",
            "item_id": "item-2",
            "audio_start_ms": 2200,
            "received_at_ms": 2532,
        })
        await asyncio.sleep(0.01)
        await transcript_queue.put({
            "text": "主要是失业了吧。",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-unemployment-continuation",
            "item_id": "item-2",
            "audio_start_ms": 2200,
            "audio_end_ms": 3584,
            "audio_duration_ms": 1384,
            "received_at_ms": 2600,
        })
        await asyncio.sleep(0.42)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "delayed-final-delivery-call",
            transcript_queue,
            settle_seconds=0.4,
        )
    ]
    await producer

    assert committed == ["呃，可能是我。 主要是失业了吧。"]


@pytest.mark.asyncio
async def test_pipeline_source_timeline_splits_turns_when_audio_gap_exceeds_settlement():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    await transcript_queue.put({
        "text": "第一句话",
        "is_final": True,
        "is_partial": False,
        "event_id": "evt-1",
        "item_id": "item-1",
        "audio_start_ms": 0,
        "audio_end_ms": 100,
        "audio_duration_ms": 100,
        "received_at_ms": 100,
    })
    await transcript_queue.put({
        "text": "第二句话",
        "is_final": True,
        "is_partial": False,
        "event_id": "evt-2",
        "item_id": "item-2",
        "audio_start_ms": 180,
        "audio_end_ms": 280,
        "audio_duration_ms": 100,
        "received_at_ms": 280,
    })
    await transcript_queue.put(None)

    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "source-gap-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]

    assert committed == ["第一句话", "第二句话"]


@pytest.mark.asyncio
async def test_streaming_pacer_stays_paused_until_resumed():
    manager = StreamingPlaybackManager(
        session_store=SessionStore(),
        ari_client=_DummyARI(),
        conversation_coordinator=None,
        streaming_config={"chunk_size_ms": 20},
    )
    call_id = "paused-stream"
    stream_id = "stream-1"
    resume_event = asyncio.Event()
    manager.active_streams[call_id] = {
        "stream_id": stream_id,
        "streaming_task": asyncio.current_task(),
        "playback_resume_event": resume_event,
    }
    manager._drain_next_frame = AsyncMock(return_value="finished")

    pacer = asyncio.create_task(manager._pacer_loop(call_id, stream_id, asyncio.Queue()))
    await asyncio.sleep(0.05)
    manager._drain_next_frame.assert_not_awaited()

    assert await manager.resume_streaming_playback(call_id, stream_id=stream_id) is True
    await asyncio.wait_for(pacer, timeout=0.5)
    manager._drain_next_frame.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("playback_type", ["pipeline-tts", "pipeline-tts-greeting"])
async def test_pipeline_barge_candidate_confirms_only_after_valid_transcript(playback_type):
    call_id = "candidate-confirm"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.media_rx_confirmed = True
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_barge_in_candidates = {}
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={call_id: {"stream_id": "stream-1", "playback_type": playback_type}},
        pause_streaming_playback=AsyncMock(return_value=True),
        resume_streaming_playback=AsyncMock(return_value=True),
    )
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(talk_detect_transcript_confirmation_timeout_ms=10_000)
    )
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    assert await engine._start_pipeline_barge_in_candidate(session, source="talkdetect") is True
    assert session.audio_capture_enabled is True
    engine._apply_barge_in_action.assert_not_awaited()

    assert await engine._confirm_pipeline_barge_in_candidate(call_id, "……") is False
    engine._apply_barge_in_action.assert_not_awaited()

    assert await engine._confirm_pipeline_barge_in_candidate(call_id, "等一下") is True
    engine._apply_barge_in_action.assert_awaited_once_with(
        call_id,
        source="talkdetect",
        reason="confirmed_customer_transcript",
    )
    engine.streaming_playback_manager.resume_streaming_playback.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_stream_enqueue_stops_when_playback_is_removed():
    engine = Engine.__new__(Engine)
    active = {"value": True}
    engine.streaming_playback_manager = SimpleNamespace(
        is_stream_active=lambda call_id, stream_id=None: active["value"],
    )
    queue = asyncio.Queue(maxsize=1)
    queue.put_nowait(b"queued-audio")

    enqueue = asyncio.create_task(
        engine._enqueue_pipeline_stream_chunk(
            "interrupted-greeting",
            "stream-1",
            queue,
            b"next-audio",
            poll_seconds=0.01,
        )
    )
    await asyncio.sleep(0.03)
    active["value"] = False

    assert await asyncio.wait_for(enqueue, timeout=0.2) is False
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_pipeline_barge_candidate_resumes_when_no_transcript_arrives():
    call_id = "candidate-resume"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.media_rx_confirmed = True
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_barge_in_candidates = {}
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={call_id: {"stream_id": "stream-1", "playback_type": "pipeline-tts"}},
        pause_streaming_playback=AsyncMock(return_value=True),
        resume_streaming_playback=AsyncMock(return_value=True),
    )
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(talk_detect_transcript_confirmation_timeout_ms=100)
    )
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)

    assert await engine._start_pipeline_barge_in_candidate(session, source="talkdetect") is True
    await asyncio.sleep(0.14)

    engine.streaming_playback_manager.resume_streaming_playback.assert_awaited_once_with(
        call_id,
        stream_id="stream-1",
    )
    assert session.audio_capture_enabled is False
    assert call_id not in engine._pipeline_barge_in_candidates


@pytest.mark.asyncio
async def test_pipeline_barge_candidate_timeout_survives_diagnostic_save_failure():
    call_id = "candidate-save-failure"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_barge_in_candidates = {}
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={call_id: {"stream_id": "stream-1", "playback_type": "pipeline-tts"}},
        pause_streaming_playback=AsyncMock(return_value=True),
        resume_streaming_playback=AsyncMock(return_value=True),
    )
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(talk_detect_transcript_confirmation_timeout_ms=100)
    )
    engine._save_session = AsyncMock(side_effect=RuntimeError("session store unavailable"))

    assert await engine._start_pipeline_barge_in_candidate(session, source="talkdetect") is True
    await asyncio.sleep(0.14)

    engine.streaming_playback_manager.resume_streaming_playback.assert_awaited_once_with(
        call_id,
        stream_id="stream-1",
    )
    assert call_id not in engine._pipeline_barge_in_candidates


@pytest.mark.asyncio
async def test_channel_talking_started_waits_for_transcript_before_destructive_barge_in():
    call_id = "talk-detect-candidate"
    channel_id = "caller-channel"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=channel_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_playing = True
    session.tts_started_ts = 1.0
    session.last_barge_in_ts = 0.0
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(
            enabled=True,
            talk_detect_initial_protection_ms=800,
            greeting_protection_ms=0,
            cooldown_ms=500,
        )
    )
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.engine.time.time", lambda: 3.0)
        await engine._handle_channel_talking_started({"channel": {"id": channel_id}})

    engine._start_pipeline_barge_in_candidate.assert_awaited_once_with(
        session,
        source="talkdetect",
    )
    engine._apply_barge_in_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_transcript_event_id_is_preserved_in_publish_queue():
    engine = Engine.__new__(Engine)
    engine._voiceai_transcript_queue = asyncio.Queue()
    engine._resolve_voiceai_transcript_sink = lambda _call_id: "http://voiceai-backend"

    await engine._publish_transcript_to_voiceai(
        "event-call",
        "user",
        "同一条稳定后的客户话语",
        "local_pipeline_user",
        event_id="event-call:user:fixed-id",
    )

    event = engine._voiceai_transcript_queue.get_nowait()
    assert event["content"] == "同一条稳定后的客户话语"
    assert event["event_id"] == "event-call:user:fixed-id"


def test_tool_farewell_does_not_replay_the_same_assistant_text():
    assert Engine._should_play_tool_farewell(
        "好的，那我这边帮您登记待提交材料。再见。",
        "好的，那我这边帮您登记待提交材料。再见。",
    ) is False
    assert Engine._should_play_tool_farewell("再见。", "材料已经登记。") is True
    assert Engine._should_play_tool_farewell("", "材料已经登记。") is False
