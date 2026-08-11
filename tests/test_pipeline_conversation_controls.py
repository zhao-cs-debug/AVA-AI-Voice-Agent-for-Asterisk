import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.config import BargeInConfig
from src.core.models import CallSession
from src.core.session_store import SessionStore
from src.core.streaming_playback_manager import StreamingPlaybackManager
from src.core.turn_lifecycle import TurnLifecycleState, TurnLifecycleTracker
from src.engine import Engine


class _DummyARI:
    pass


@pytest.mark.asyncio
async def test_pipeline_dialog_latest_committed_turn_cancels_previous_turn():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()
    first_turn_started = asyncio.Event()
    release_first_turn = asyncio.Event()
    started_turns = []
    cancelled_turns = []

    async def run_turn(text, generation):
        started_turns.append((text, generation))
        if text == "first":
            first_turn_started.set()
            try:
                await release_first_turn.wait()
            except asyncio.CancelledError:
                cancelled_turns.append((text, generation))
                raise

    dialog_task = asyncio.create_task(
        engine._process_pipeline_dialog_turns(
            "dialog-latest-wins-call",
            transcript_queue,
            settle_seconds=0.03,
            run_turn=run_turn,
        )
    )
    await transcript_queue.put("first")
    await asyncio.wait_for(first_turn_started.wait(), timeout=0.2)

    await transcript_queue.put("second")
    await asyncio.sleep(0.05)
    await transcript_queue.put("third")
    await asyncio.sleep(0.05)

    assert transcript_queue.empty()
    await transcript_queue.put(None)
    await asyncio.wait_for(dialog_task, timeout=0.3)

    assert started_turns == [("first", 1), ("second", 2), ("third", 3)]
    assert cancelled_turns == [("first", 1)]


@pytest.mark.asyncio
async def test_pipeline_dialog_discards_late_result_from_superseded_turn():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()
    first_turn_started = asyncio.Event()
    release_old_turn = asyncio.Event()
    produced_answers = []

    async def run_turn(text, generation):
        if text == "first":
            first_turn_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await release_old_turn.wait()
        try:
            engine._ensure_pipeline_turn_current("dialog-late-result-call", generation)
        except asyncio.CancelledError:
            return
        produced_answers.append(text)

    dialog_task = asyncio.create_task(
        engine._process_pipeline_dialog_turns(
            "dialog-late-result-call",
            transcript_queue,
            settle_seconds=0.03,
            run_turn=run_turn,
        )
    )
    await transcript_queue.put("first")
    await asyncio.wait_for(first_turn_started.wait(), timeout=0.2)
    await transcript_queue.put("latest")
    await asyncio.sleep(0.05)
    release_old_turn.set()
    await transcript_queue.put(None)
    await asyncio.wait_for(dialog_task, timeout=0.3)

    assert produced_answers == ["latest"]


def test_pipeline_turn_generation_rejects_late_old_generation():
    engine = Engine.__new__(Engine)
    engine._pipeline_turn_generations = {"latest-wins-call": 2}

    with pytest.raises(asyncio.CancelledError):
        engine._ensure_pipeline_turn_current("latest-wins-call", 1)

    engine._ensure_pipeline_turn_current("latest-wins-call", 2)


@pytest.mark.parametrize("text", ["呃。", "额……", "Uh.", "um", "Oh."])
def test_customer_transcript_rejects_hesitation_only_final(text):
    assert Engine._is_valid_customer_transcript(text) is False


@pytest.mark.parametrize("text", ["嗯。", "是的", "等一下", "啊？"])
def test_customer_transcript_keeps_meaningful_short_answer(text):
    assert Engine._is_valid_customer_transcript(text) is True


@pytest.mark.parametrize("text", ["嗯，是的，是的。", "是的，是的。", "嗯，对的。"])
def test_compound_acknowledgement_can_wait_for_follow_up(text):
    assert Engine._is_acknowledgement_only_transcript(text) is True


@pytest.mark.parametrize("text", ["是的。", "好的。", "嗯。", "没问题。", "我暂时失业了。"])
def test_standalone_answer_keeps_normal_settlement_window(text):
    assert Engine._is_acknowledgement_only_transcript(text) is False


@pytest.mark.asyncio
async def test_pipeline_settlement_drops_hesitation_before_llm_boundary():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()
    await transcript_queue.put(
        {
            "text": "呃。",
            "is_final": True,
            "is_partial": False,
            "event_id": "hesitation-only",
        }
    )
    await transcript_queue.put(
        {
            "text": "可能是我最近失业了吧。",
            "is_final": True,
            "is_partial": False,
            "event_id": "meaningful-answer",
        }
    )
    await transcript_queue.put(None)

    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "hesitation-filter-call",
            transcript_queue,
            settle_seconds=0.0,
        )
    ]

    assert committed == ["可能是我最近失业了吧。"]
    engine._confirm_pipeline_barge_in_candidate.assert_awaited_once_with(
        "hesitation-filter-call", "可能是我最近失业了吧。"
    )


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
    engine._pipeline_forced = {call_id: True}
    engine._note_pipeline_talk_detect_hint = AsyncMock(return_value=True)
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
    engine._note_pipeline_talk_detect_hint.assert_awaited_once_with(session)
    engine._start_pipeline_barge_in_candidate.assert_not_awaited()


def _pipeline_energy_gate_config(**overrides):
    values = {
        "enabled": True,
        "pipeline_barge_energy_min_ms": 180,
        "pipeline_barge_energy_absolute_min": 450,
        "pipeline_barge_energy_noise_multiplier": 3.0,
        "pipeline_barge_energy_noise_margin": 200,
        "pipeline_barge_energy_gap_tolerance_ms": 40,
        "pipeline_talk_detect_enabled": True,
        "pipeline_barge_energy_hint_timeout_ms": 1200,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _pcm16_frame(amplitude: int, duration_ms: int = 20, sample_rate: int = 16000) -> bytes:
    samples = int(sample_rate * duration_ms / 1000)
    return int(amplitude).to_bytes(2, byteorder="little", signed=True) * samples


@pytest.mark.asyncio
async def test_pipeline_talkdetect_hint_alone_does_not_pause_playback():
    call_id = "talkdetect-hint-only"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_playing = True
    session.tts_started_ts = 1.0
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(
            enabled=True,
            talk_detect_initial_protection_ms=800,
            greeting_protection_ms=0,
            cooldown_ms=500,
            pipeline_talk_detect_hint_timeout_ms=1200,
        )
    )
    engine._pipeline_forced = {call_id: True}
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.engine.time.time", lambda: 3.0)
        await engine._handle_channel_talking_started({"channel": {"id": call_id}})

    gate = session.vad_state["pipeline_barge_energy"]
    assert gate["talk_detect_pending"] is True
    engine._start_pipeline_barge_in_candidate.assert_not_awaited()
    engine._apply_barge_in_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_energy_gate_rejects_low_energy_and_short_spike():
    session = CallSession(call_id="energy-reject", caller_channel_id="energy-reject", provider_name="pipeline")
    session.audio_capture_enabled = False
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    await engine._note_pipeline_talk_detect_hint(session)
    for _ in range(12):
        assert not await engine._maybe_start_pipeline_barge_in_from_pcm(
            session,
            _pcm16_frame(300),
            16000,
            source="audiosocket",
        )
    assert not await engine._maybe_start_pipeline_barge_in_from_pcm(
        session,
        _pcm16_frame(2200, duration_ms=20),
        16000,
        source="audiosocket",
    )

    engine._start_pipeline_barge_in_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_pipeline_energy_gate_starts_once_after_sustained_speech():
    session = CallSession(call_id="energy-sustained", caller_channel_id="energy-sustained", provider_name="pipeline")
    session.audio_capture_enabled = False
    session.vad_state["pipeline_talk_detect"] = {"enabled": True}
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    await engine._note_pipeline_talk_detect_hint(session)
    results = []
    for _ in range(10):
        results.append(await engine._maybe_start_pipeline_barge_in_from_pcm(
            session,
            _pcm16_frame(2200),
            16000,
            source="audiosocket",
        ))

    assert results[:8] == [False] * 8
    assert results[8] is True
    assert results[9] is False
    engine._start_pipeline_barge_in_candidate.assert_awaited_once_with(
        session,
        source="talkdetect_audiosocket_energy",
    )


@pytest.mark.asyncio
async def test_pipeline_energy_gate_adapts_noise_floor_per_call():
    quiet = CallSession(call_id="quiet-call", caller_channel_id="quiet-call", provider_name="pipeline")
    noisy = CallSession(call_id="noisy-call", caller_channel_id="noisy-call", provider_name="pipeline")
    quiet.audio_capture_enabled = True
    noisy.audio_capture_enabled = True
    quiet.vad_state["pipeline_talk_detect"] = {"enabled": True}
    noisy.vad_state["pipeline_talk_detect"] = {"enabled": True}
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    for _ in range(20):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            quiet, _pcm16_frame(100), 16000, source="audiosocket"
        )
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            noisy, _pcm16_frame(700), 16000, source="audiosocket"
        )

    quiet_gate = quiet.vad_state["pipeline_barge_energy"]
    noisy_gate = noisy.vad_state["pipeline_barge_energy"]
    assert quiet_gate["noise_floor"] < noisy_gate["noise_floor"]
    assert quiet_gate["threshold"] < noisy_gate["threshold"]


@pytest.mark.asyncio
async def test_pipeline_energy_gate_freezes_noise_floor_while_candidate_active():
    call_id = "candidate-freeze"
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = True
    session.vad_state["pipeline_talk_detect"] = {"enabled": True}
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    for _ in range(10):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            session, _pcm16_frame(120), 16000, source="rtp"
        )
    before = session.vad_state["pipeline_barge_energy"]["noise_floor"]
    engine._pipeline_barge_in_candidates[call_id] = {"stream_id": "stream-1"}

    for _ in range(10):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            session, _pcm16_frame(2600), 16000, source="rtp"
        )

    assert session.vad_state["pipeline_barge_energy"]["noise_floor"] == before


@pytest.mark.asyncio
async def test_pipeline_energy_gate_confirms_final_that_arrived_before_energy():
    call_id = "final-before-energy"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_started_ts = time.time() - 1.0
    session.vad_state["pipeline_talk_detect"] = {"enabled": True}
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}

    async def start_candidate(_session, *, source):
        engine._pipeline_barge_in_candidates[call_id] = {
            "stream_id": "stream-1",
            "source": source,
        }
        return True

    engine._start_pipeline_barge_in_candidate = AsyncMock(side_effect=start_candidate)
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    await engine._note_pipeline_talk_detect_hint(session)
    session.vad_state["pipeline_barge_energy"].update({
        "pending_transcript": "等一下，我还没说完。",
        "pending_transcript_at": time.monotonic(),
        "pending_transcript_wall_ts": time.time(),
    })
    for _ in range(9):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            session,
            _pcm16_frame(2200),
            16000,
            source="audiosocket",
        )

    engine._confirm_pipeline_barge_in_candidate.assert_awaited_once_with(
        call_id,
        "等一下，我还没说完。",
    )


@pytest.mark.parametrize("text", ["嗯。", "啊。", "哦。", "Ah."])
def test_isolated_interjection_cannot_confirm_pipeline_barge_in(text):
    assert Engine._is_valid_customer_transcript(text) is True
    assert Engine._is_valid_barge_in_transcript(text) is False


@pytest.mark.asyncio
async def test_pipeline_final_is_buffered_during_tts_before_talkdetect_hint():
    call_id = "final-before-talkdetect"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_playing = True
    session.tts_started_ts = time.time() - 1.0
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_forced = {call_id: True}
    engine._pipeline_barge_in_candidates = {}

    async def start_candidate(_session, *, source):
        engine._pipeline_barge_in_candidates[call_id] = {
            "stream_id": "stream-1",
            "source": source,
        }
        return True

    engine._start_pipeline_barge_in_candidate = AsyncMock(side_effect=start_candidate)
    original_confirm = engine._confirm_pipeline_barge_in_candidate
    confirmed = []

    async def observe_confirm(_call_id, text):
        confirmed.append(text)
        return True

    assert await engine._confirm_pipeline_barge_in_candidate(
        call_id,
        "等一下，我还没说完。",
    ) is False

    gate = session.vad_state["pipeline_barge_energy"]
    assert gate["pending_transcript"] == "等一下，我还没说完。"
    assert gate["pending_transcript_wall_ts"] >= session.tts_started_ts

    await engine._note_pipeline_talk_detect_hint(session)
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(side_effect=observe_confirm)
    for _ in range(9):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            session,
            _pcm16_frame(2200),
            16000,
            source="audiosocket",
        )

    assert confirmed == ["等一下，我还没说完。"]
    engine._confirm_pipeline_barge_in_candidate = original_confirm


@pytest.mark.asyncio
async def test_pipeline_final_is_not_buffered_while_listening():
    call_id = "final-while-listening"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = True
    session.tts_playing = False
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_forced = {call_id: True}
    engine._pipeline_barge_in_candidates = {}

    assert await engine._confirm_pipeline_barge_in_candidate(
        call_id,
        "这是正常听取阶段的一句话。",
    ) is False
    assert "pipeline_barge_energy" in session.vad_state
    assert "pending_transcript" not in session.vad_state["pipeline_barge_energy"]


@pytest.mark.asyncio
async def test_pipeline_energy_gate_falls_back_when_talkdetect_install_failed():
    session = CallSession(call_id="talkdetect-fallback", caller_channel_id="talkdetect-fallback", provider_name="pipeline")
    session.audio_capture_enabled = False
    session.vad_state["pipeline_talk_detect"] = {"enabled": False}
    engine = Engine.__new__(Engine)
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._pipeline_barge_in_candidates = {}
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)

    for _ in range(9):
        await engine._maybe_start_pipeline_barge_in_from_pcm(
            session,
            _pcm16_frame(2200),
            16000,
            source="rtp",
        )

    engine._start_pipeline_barge_in_candidate.assert_awaited_once_with(
        session,
        source="rtp_energy",
    )


@pytest.mark.asyncio
async def test_audiosocket_pipeline_keeps_streaming_stt_input_during_tts_gate():
    call_id = "pipeline-full-duplex-stt"
    conn_id = "audio-connection"
    pcm16 = b"\x01\x00" * 320
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.tts_playing = True
    session.media_rx_confirmed = True
    session.vad_state["pipeline_talk_detect"] = {"enabled": True}
    await session_store.upsert_call(session)

    pipeline_queue = asyncio.Queue()
    engine = Engine.__new__(Engine)
    engine.conn_to_channel = {conn_id: call_id}
    engine.audio_socket_server = None
    engine._audiosocket_frame_count = {}
    engine.session_store = session_store
    engine.config = SimpleNamespace(
        audio_transport="audiosocket",
        audiosocket=SimpleNamespace(format="slin16", sample_rate=16000),
        streaming=SimpleNamespace(sample_rate=16000),
        barge_in=SimpleNamespace(enabled=True),
    )
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._infer_transport_from_frame = lambda _size: ("slin16", 16000)
    engine._update_transport_profile = AsyncMock()
    engine._wire_to_pcm16 = lambda payload, *_args: (payload, 16000)
    engine._update_audio_diagnostics = lambda *_args: None
    engine.audio_capture = SimpleNamespace(append_pcm16=lambda *_args, **_kwargs: None)
    engine.customer_audio_capture = SimpleNamespace(
        append_pcm16=lambda *_args, **_kwargs: False
    )
    engine._consume_attended_transfer_screening_audio = lambda *_args: False
    engine._session_has_pending_attended_transfer = lambda *_args: False
    engine._pipeline_forced = {call_id: True}
    engine._pipeline_queues = {call_id: pipeline_queue}
    engine._resample_state_pipeline16k = {}
    engine._publish_audio_to_voiceai = AsyncMock()

    await engine._audiosocket_handle_audio(conn_id, pcm16)

    assert pipeline_queue.get_nowait() == pcm16
    assert session.audio_capture_enabled is False


@pytest.mark.asyncio
async def test_rtp_pipeline_queues_stt_before_running_interruption_gate():
    call_id = "rtp-full-duplex-stt"
    pcm16 = _pcm16_frame(1800)
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.media_rx_confirmed = True
    await session_store.upsert_call(session)

    pipeline_queue = asyncio.Queue()
    observed = []
    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine.rtp_server = SimpleNamespace(sample_rate=16000)
    engine._pipeline_forced = {call_id: True}
    engine._pipeline_queues = {call_id: pipeline_queue}
    engine.config = SimpleNamespace(barge_in=_pipeline_energy_gate_config())
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._consume_attended_transfer_screening_audio = lambda *_args: False
    engine._session_has_pending_attended_transfer = lambda *_args: False
    engine._publish_audio_to_voiceai = AsyncMock()

    async def observe_gate(*_args, **_kwargs):
        observed.append(pipeline_queue.get_nowait())
        return False

    engine._maybe_start_pipeline_barge_in_from_pcm = AsyncMock(side_effect=observe_gate)

    await engine._on_rtp_audio(call_id, 1234, pcm16)

    assert observed == [pcm16]
    engine._maybe_start_pipeline_barge_in_from_pcm.assert_awaited_once()


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
async def test_pipeline_settlement_keeps_short_acknowledgement_open_for_delayed_follow_up():
    """A delayed question after an acknowledgement is one customer turn.

    The production trace delivered the two finals about 1.55s apart.  The
    acknowledgement window must therefore be longer than the normal 400ms
    window and cover that real source-audio gap.
    """
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put({
            "text": "嗯，是的，是的。",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-ack",
            "item_id": "item-ack",
            "audio_start_ms": 0,
            "audio_end_ms": 1000,
            "audio_duration_ms": 1000,
        })
        await asyncio.sleep(1.55)
        await transcript_queue.put({
            "text": "是什么事？",
            "is_final": True,
            "is_partial": False,
            "event_id": "evt-question",
            "item_id": "item-question",
            "audio_start_ms": 2548,
            "audio_end_ms": 3400,
            "audio_duration_ms": 852,
        })
        await asyncio.sleep(0.05)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "acknowledgement-continuation-call",
            transcript_queue,
            settle_seconds=0.04,
            acknowledgement_continuation_seconds=1.8,
        )
    ]
    await producer

    assert committed == ["嗯，是的，是的。 是什么事？"]


@pytest.mark.asyncio
async def test_pipeline_transcript_settlement_drops_turn_when_call_terminates():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()

    async def produce_transcripts():
        await transcript_queue.put(
            {
                "text": "挂断前最后一句",
                "is_final": True,
                "is_partial": False,
                "event_id": "late-final",
            }
        )
        await asyncio.sleep(0.02)
        engine._pipeline_terminating_calls.add("terminating-settlement-call")
        await asyncio.sleep(0.04)
        await transcript_queue.put(None)

    producer = asyncio.create_task(produce_transcripts())
    committed = [
        text
        async for text in engine._iter_settled_pipeline_transcripts(
            "terminating-settlement-call",
            transcript_queue,
            settle_seconds=0.04,
        )
    ]
    await producer

    assert committed == []


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
async def test_pipeline_dialog_settles_new_speech_while_previous_turn_runs():
    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()
    engine._confirm_pipeline_barge_in_candidate = AsyncMock(return_value=False)
    transcript_queue = asyncio.Queue()
    first_turn_started = asyncio.Event()
    release_first_turn = asyncio.Event()
    handled_turns = []

    async def run_turn(text, generation):
        handled_turns.append(text)
        if len(handled_turns) == 1:
            first_turn_started.set()
            await release_first_turn.wait()

    dialog_task = asyncio.create_task(
        engine._process_pipeline_dialog_turns(
            "dialog-settlement-call",
            transcript_queue,
            settle_seconds=0.03,
            run_turn=run_turn,
        )
    )
    await transcript_queue.put("第一轮")
    await asyncio.wait_for(first_turn_started.wait(), timeout=0.2)

    await transcript_queue.put("第二轮")
    await asyncio.sleep(0.05)
    await transcript_queue.put("第三轮")
    await asyncio.sleep(0.05)

    assert transcript_queue.empty()
    release_first_turn.set()
    await transcript_queue.put(None)
    await asyncio.wait_for(dialog_task, timeout=0.3)

    assert handled_turns == ["第一轮", "第二轮", "第三轮"]


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


def test_runtime_prompt_rules_are_injected_once():
    prompt = "请围绕客户的材料情况进行简短沟通。"

    injected = Engine._append_runtime_prompt_rules(prompt)
    reinjected = Engine._append_runtime_prompt_rules(injected)

    assert reinjected == injected
    assert injected.count("<runtime_rules>") == 1
    assert injected.count("如果客户输入语义不完整") == 1
    assert injected.count("你的回复内容不可以是历史消息中已经出现过的重复内容") == 1


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
async def test_meaningful_asr_partial_keeps_barge_candidate_open_for_final():
    call_id = "candidate-partial-extension"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.audio_capture_enabled = False
    session.media_rx_confirmed = True
    await session_store.upsert_call(session)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_barge_in_candidates = {}
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={
            call_id: {"stream_id": "stream-1", "playback_type": "pipeline-tts"}
        },
        pause_streaming_playback=AsyncMock(return_value=True),
        resume_streaming_playback=AsyncMock(return_value=True),
    )
    engine.config = SimpleNamespace(
        barge_in=SimpleNamespace(talk_detect_transcript_confirmation_timeout_ms=40)
    )
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    assert await engine._start_pipeline_barge_in_candidate(session, source="talkdetect") is True
    await asyncio.sleep(0.06)
    assert engine._note_pipeline_barge_in_asr_activity(call_id, "呃。") is False
    assert engine._note_pipeline_barge_in_asr_activity(call_id, "我现在确实没有收入") is True
    await asyncio.sleep(0.06)

    assert call_id in engine._pipeline_barge_in_candidates
    assert await engine._confirm_pipeline_barge_in_candidate(call_id, "我现在确实没有收入。") is True
    engine._apply_barge_in_action.assert_awaited_once()
    engine.streaming_playback_manager.resume_streaming_playback.assert_not_awaited()


@pytest.mark.asyncio
async def test_confirmed_barge_in_interrupts_active_lifecycle_turn():
    call_id = "barge-lifecycle"
    session_store = SessionStore()
    session = CallSession(call_id=call_id, caller_channel_id=call_id, provider_name="pipeline")
    session.media_rx_confirmed = True
    await session_store.upsert_call(session)

    tracker = TurnLifecycleTracker(call_id)
    turn = tracker.commit_customer("please continue")
    turn.mark_ai_generated("This response is currently audible.")
    turn.mark_ai_playing("stream-1", started_at=10.0)

    engine = Engine.__new__(Engine)
    engine.session_store = session_store
    engine._pipeline_turn_trackers = {call_id: tracker}
    engine.strategy_session_manager = SimpleNamespace(mark_barge_in=lambda _call_id: None)
    engine.streaming_playback_manager = SimpleNamespace(
        active_streams={call_id: {"stream_id": "stream-1"}},
        stop_streaming_playback=AsyncMock(return_value=True),
    )
    engine._provider_stream_queues = {}
    engine._provider_stream_formats = {}
    engine._provider_coalesce_buf = {}
    engine.ari_client = SimpleNamespace(stop_playback=AsyncMock())
    engine.conversation_coordinator = None
    engine.config = SimpleNamespace(barge_in=SimpleNamespace(provider_output_suppress_ms=0))
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._call_providers = {}

    applied = await engine._apply_barge_in_action(
        call_id,
        source="talkdetect",
        reason="confirmed_customer_transcript",
    )

    assert applied is True
    assert turn.state is TurnLifecycleState.INTERRUPTED
    assert turn.interruption_reason == "confirmed_customer_transcript"
    engine.streaming_playback_manager.stop_streaming_playback.assert_awaited_once_with(call_id)


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
            pipeline_talk_detect_enabled=True,
            pipeline_barge_energy_hint_timeout_ms=1200,
        )
    )
    engine._pipeline_forced = {call_id: True}
    engine._save_session = AsyncMock(side_effect=session_store.upsert_call)
    engine._note_pipeline_talk_detect_hint = AsyncMock(return_value=True)
    engine._start_pipeline_barge_in_candidate = AsyncMock(return_value=True)
    engine._apply_barge_in_action = AsyncMock(return_value=True)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.engine.time.time", lambda: 3.0)
        await engine._handle_channel_talking_started({"channel": {"id": channel_id}})

    engine._note_pipeline_talk_detect_hint.assert_awaited_once_with(session)
    engine._start_pipeline_barge_in_candidate.assert_not_awaited()
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


@pytest.mark.asyncio
async def test_transcript_lifecycle_metadata_is_preserved_in_publish_queue():
    engine = Engine.__new__(Engine)
    engine._voiceai_transcript_queue = asyncio.Queue()
    engine._resolve_voiceai_transcript_sink = lambda _call_id: "http://voiceai-backend"

    await engine._publish_transcript_to_voiceai(
        "event-call",
        "assistant",
        "audible response",
        "local_pipeline_agent",
        event_id="event-call:turn:1:assistant",
        turn_id="event-call:turn:1",
        turn_index=1,
        turn_role_order=1,
        lifecycle_state="interrupted",
        started_at="2026-08-05T08:00:01+00:00",
        interrupted_at="2026-08-05T08:00:02+00:00",
        playback_id="stream-1",
        interruption_reason="confirmed_customer_transcript",
        audible_text_complete=False,
    )

    event = engine._voiceai_transcript_queue.get_nowait()
    assert event["turn_id"] == "event-call:turn:1"
    assert event["turn_index"] == 1
    assert event["turn_role_order"] == 1
    assert event["lifecycle_state"] == "interrupted"
    assert event["audible_text_complete"] is False


def test_tool_farewell_does_not_replay_the_same_assistant_text():
    assert Engine._should_play_tool_farewell(
        "好的，那我这边帮您登记待提交材料。再见。",
        "好的，那我这边帮您登记待提交材料。再见。",
    ) is False
    assert Engine._should_play_tool_farewell("再见。", "材料已经登记。") is True
    assert Engine._should_play_tool_farewell("", "材料已经登记。") is False


def test_external_strategy_provider_context_contains_only_demo_runtime_data():
    raw_context = {
        "external_strategy": {
            "network": {"external_id": "network-1"},
            "ai": {"title": "顾问", "gender": "女", "background": "负责沟通"},
            "human": {"title": "客户", "gender": "", "background": ""},
            "voice": {"value": "voice-1"},
            "audio": {
                "input_sample_rate": 16000,
                "output_sample_rate": 24000,
                "chunk_ms": 200,
            },
        },
        "greeting": "local greeting",
        "prompt": "local prompt",
        "instructions": "local instructions",
        "tools": ["local-tool"],
        "caller_name": "local caller",
        "caller_id": "10086",
    }

    assert Engine._external_strategy_provider_context(raw_context) == {
        "external_strategy": raw_context["external_strategy"],
    }


@pytest.mark.asyncio
async def test_external_strategy_skips_local_pre_and_post_call_tools():
    engine = Engine.__new__(Engine)
    engine._get_provider_kind = lambda _name: "external_strategy_agent"
    engine.config = SimpleNamespace(default_provider="external_strategy_agent")
    engine.transport_orchestrator = SimpleNamespace(get_context_config=Mock())
    session = CallSession(
        call_id="external-tools",
        caller_channel_id="external-tools",
        provider_name="external_strategy_agent",
    )

    assert await engine._execute_pre_call_tools(session.call_id, session) == {}
    assert await engine._execute_post_call_tools(session.call_id, session) is None
    engine.transport_orchestrator.get_context_config.assert_not_called()


@pytest.mark.asyncio
async def test_external_strategy_skips_local_context_background_music():
    engine = Engine.__new__(Engine)
    engine._get_provider_kind = lambda _name: "external_strategy_agent"
    engine.config = SimpleNamespace(default_provider="external_strategy_agent")
    engine.transport_orchestrator = SimpleNamespace(
        get_context_config=lambda _name: SimpleNamespace(background_music="local-moh")
    )
    engine._start_background_music = AsyncMock()
    session = CallSession(
        call_id="external-background",
        caller_channel_id="external-background",
        provider_name="external_strategy_agent",
    )

    await engine._start_context_background_music(session, "external-context")

    engine._start_background_music.assert_not_awaited()


def test_external_strategy_provider_ignores_local_prompt_and_voice_overrides():
    engine = Engine.__new__(Engine)
    engine._get_provider_kind = lambda _name: "external_strategy_agent"
    engine.config = SimpleNamespace(default_provider="external_strategy_agent")
    provider_config = SimpleNamespace(
        greeting="upstream greeting",
        prompt="upstream prompt",
        instructions="upstream instructions",
        default_voice={"voice_id": "upstream-voice"},
        target_encoding="",
        target_sample_rate_hz=0,
    )
    provider = SimpleNamespace(config=provider_config)
    session = CallSession(
        call_id="external-overrides",
        caller_channel_id="external-overrides",
        provider_name="external_strategy_agent",
    )
    session.provider_overrides = {
        "greeting": "local greeting",
        "prompt": "local prompt",
        "default_voice": {"voice_id": "local-voice"},
        "target_encoding": "slin16",
        "target_sample_rate_hz": 16000,
    }

    engine._apply_provider_overrides(provider, session)

    assert provider_config.greeting == "upstream greeting"
    assert provider_config.prompt == "upstream prompt"
    assert provider_config.instructions == "upstream instructions"
    assert provider_config.default_voice == {"voice_id": "upstream-voice"}
    assert provider_config.target_encoding == "slin16"
    assert provider_config.target_sample_rate_hz == 16000


@pytest.mark.asyncio
async def test_external_strategy_audio_profile_does_not_load_local_templates():
    engine = Engine.__new__(Engine)
    engine._get_provider_kind = lambda _name: "external_strategy_agent"
    engine.config = SimpleNamespace(default_provider="external_strategy_agent")
    engine._call_providers = {}
    engine._save_session = AsyncMock()
    engine.streaming_playback_manager = SimpleNamespace(
        audiosocket_format="",
        chunk_size_ms=0,
        idle_cutoff_ms=0,
    )

    async def read_channel_variable(_method, _path, *, params, **_kwargs):
        values = {
            "AI_PROVIDER": "external_strategy_agent",
            "AI_AUDIO_PROFILE": "telephony_ulaw_8k",
            "AI_CONTEXT": "external-context",
        }
        return {"value": values.get(params["variable"], "")}

    engine.ari_client = SimpleNamespace(send_command=AsyncMock(side_effect=read_channel_variable))
    context_config = SimpleNamespace(
        provider="external_strategy_agent",
        greeting="local greeting",
        prompt="local prompt",
        default_voice={"voice_id": "local-voice"},
    )
    transport = SimpleNamespace(
        profile_name="telephony_ulaw_8k",
        wire_encoding="slin16",
        wire_sample_rate=16000,
        chunk_ms=200,
        idle_cutoff_ms=500,
        context="external-context",
    )
    engine.transport_orchestrator = SimpleNamespace(
        get_context_config=Mock(return_value=context_config),
        resolve_transport=Mock(return_value=transport),
    )
    engine.providers = {
        "external_strategy_agent": SimpleNamespace(
            config=None,
            get_capabilities=Mock(return_value=None),
        )
    }
    engine._apply_prompt_template_substitution = Mock(side_effect=lambda value, _session: value)
    session = CallSession(
        call_id="external-audio-profile",
        caller_channel_id="external-audio-profile",
        provider_name="external_strategy_agent",
    )
    session.provider_overrides = {
        "greeting": "stale greeting",
        "prompt": "stale prompt",
        "default_voice": {"voice_id": "stale-voice"},
    }

    await engine._resolve_audio_profile(session, session.caller_channel_id)

    assert session.provider_overrides == {
        "target_encoding": "slin16",
        "target_sample_rate_hz": 16000,
    }
    engine.transport_orchestrator.get_context_config.assert_not_called()


@pytest.mark.asyncio
async def test_external_strategy_session_start_skips_local_context_assembly():
    engine = Engine.__new__(Engine)
    engine._get_provider_kind = lambda _name: "external_strategy_agent"
    engine.config = SimpleNamespace(
        default_provider="external_strategy_agent",
        audio_transport="externalmedia",
    )
    engine._call_providers = {}
    engine._execute_pre_call_tools = AsyncMock(return_value={})
    engine._save_session = AsyncMock()
    engine.conversation_coordinator = None
    session = CallSession(
        call_id="external-context-only",
        caller_channel_id="external-context-only",
        provider_name="external_strategy_agent",
        context_name="external-context",
    )
    external_strategy = {
        "network": {"external_id": "network-1"},
        "ai": {"title": "顾问", "gender": "女", "background": "负责沟通"},
        "human": {"title": "客户", "gender": "", "background": ""},
        "voice": {"value": "voice-1"},
        "audio": {
            "input_sample_rate": 16000,
            "output_sample_rate": 24000,
            "chunk_ms": 200,
        },
    }
    context_config = SimpleNamespace(
        external_strategy=external_strategy,
        greeting="local greeting",
        prompt="local prompt",
        tools=["local-tool"],
        in_call_http_tools=[],
        disable_global_in_call_tools=[],
    )
    engine.transport_orchestrator = SimpleNamespace(
        get_context_config=Mock(return_value=context_config)
    )
    engine.pipeline_orchestrator = SimpleNamespace(enabled=False)
    engine.session_store = SimpleNamespace(
        get_by_call_id=AsyncMock(return_value=session),
        upsert_call=AsyncMock(),
    )
    provider = SimpleNamespace(
        config=None,
        start_session=AsyncMock(),
        play_initial_greeting=AsyncMock(),
    )
    engine.provider_factories = {"external_strategy_agent": lambda: provider}

    await engine._start_provider_session(session.call_id)

    provider.start_session.assert_awaited_once_with(
        session.call_id,
        context={"external_strategy": external_strategy},
    )
    provider.play_initial_greeting.assert_not_awaited()
    engine.session_store.upsert_call.assert_not_awaited()
