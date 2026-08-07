import asyncio
import struct

import pytest

from src.engine import Engine, _PipelinePlaybackInterrupted
from src.core.turn_lifecycle import TurnLifecycleState, TurnLifecycleTracker


class _StreamOwnershipStub:
    def __init__(self, stream_id="stream-1"):
        self.stream_id = stream_id
        self.active = True

    def is_stream_active(self, call_id, stream_id=None):
        return self.active and stream_id == self.stream_id


class _ClosableTtsStream:
    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._chunks)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.closed = True


def _pcm16_chunk(amplitude: int, duration_ms: int = 100, sample_rate: int = 8000) -> bytes:
    sample_count = int(sample_rate * duration_ms / 1000)
    return struct.pack("<h", amplitude) * sample_count


@pytest.mark.asyncio
async def test_pipeline_tts_stream_preserves_audio_after_internal_silence():
    engine = Engine.__new__(Engine)
    engine.streaming_playback_manager = _StreamOwnershipStub()
    queue = asyncio.Queue()
    speech = _pcm16_chunk(1200)
    silence = _pcm16_chunk(0)
    stream = _ClosableTtsStream([speech] + ([silence] * 12) + [speech])
    stream_info = {}

    any_audio, audible_end, interrupted = await engine._stream_pipeline_tts_audio(
        "call-audible-guard",
        "stream-1",
        queue,
        stream,
        source_encoding="slin16",
        source_sample_rate=8000,
        stream_info=stream_info,
    )

    assert any_audio is True
    assert audible_end is False
    assert interrupted is False
    assert stream.closed is False
    assert "end_reason" not in stream_info
    assert queue.qsize() == 14
    assert [await queue.get() for _ in range(14)] == [speech] + ([silence] * 12) + [speech]


@pytest.mark.asyncio
async def test_audible_end_of_stream_is_not_reported_as_completed():
    engine = Engine.__new__(Engine)
    turn = TurnLifecycleTracker("call-audible-end").commit_customer("continue")
    turn.mark_ai_generated("full response")
    turn.mark_ai_playing("stream-audible", started_at=20.0)

    audible = await engine._finish_pipeline_stream_turn(
        turn,
        {
            "first_real_emit_ts": 20.0,
            "last_real_emit_ts": 21.0,
            "real_tx_bytes": 8000,
            "queued_target_total_bytes": 8000,
            "end_reason": "audible-end-of-stream",
        },
    )

    assert audible == "full response"
    assert turn.state is TurnLifecycleState.INTERRUPTED
    assert turn.interruption_reason == "audible-end-of-stream"
    assert turn.audible_text_complete is False


@pytest.mark.asyncio
async def test_pipeline_stream_put_exits_when_barge_in_stops_full_queue():
    engine = Engine.__new__(Engine)
    manager = _StreamOwnershipStub()
    engine.streaming_playback_manager = manager
    queue = asyncio.Queue(maxsize=1)
    queue.put_nowait(b"already-full")

    async def stop_stream():
        await asyncio.sleep(0.05)
        manager.active = False

    stopper = asyncio.create_task(stop_stream())
    with pytest.raises(_PipelinePlaybackInterrupted):
        await asyncio.wait_for(
            engine._put_pipeline_stream_chunk(
                "call-deadlock",
                "stream-1",
                queue,
                b"blocked",
                wait_slice_sec=0.02,
            ),
            timeout=0.5,
        )
    await stopper


@pytest.mark.asyncio
async def test_pipeline_stream_put_rejects_replaced_stream_owner():
    engine = Engine.__new__(Engine)
    engine.streaming_playback_manager = _StreamOwnershipStub(stream_id="new-stream")
    queue = asyncio.Queue(maxsize=1)

    with pytest.raises(_PipelinePlaybackInterrupted):
        await engine._put_pipeline_stream_chunk(
            "call-replaced",
            "old-stream",
            queue,
            b"stale",
        )
    assert queue.empty()


@pytest.mark.asyncio
async def test_pipeline_stream_put_allows_healthy_consumer():
    engine = Engine.__new__(Engine)
    engine.streaming_playback_manager = _StreamOwnershipStub()
    queue = asyncio.Queue(maxsize=1)

    await engine._put_pipeline_stream_chunk(
        "call-healthy",
        "stream-1",
        queue,
        b"audio",
    )
    assert await queue.get() == b"audio"


@pytest.mark.asyncio
async def test_pipeline_turn_does_not_complete_until_stream_task_finishes():
    engine = Engine.__new__(Engine)
    release = asyncio.Event()

    async def finish_physical_playback():
        await release.wait()

    stream_task = asyncio.create_task(finish_physical_playback())
    stream_info = {
        "streaming_task": stream_task,
        "first_real_emit_ts": 10.0,
        "last_real_emit_ts": 12.0,
        "tx_bytes": 8000,
        "queued_total_bytes": 8000,
        "end_reason": "end-of-stream",
    }
    turn = TurnLifecycleTracker("call-stream").commit_customer("continue")
    turn.mark_ai_generated("full response")
    turn.mark_ai_playing("stream-1", started_at=9.0)

    finish_task = asyncio.create_task(engine._finish_pipeline_stream_turn(turn, stream_info))
    await asyncio.sleep(0)

    assert not finish_task.done()
    assert turn.state is TurnLifecycleState.AI_PLAYING

    release.set()
    assert await finish_task == "full response"
    assert turn.state is TurnLifecycleState.COMPLETED
    assert turn.playback_started_at == 10.0


@pytest.mark.asyncio
async def test_pipeline_interrupted_stream_keeps_only_audible_prefix():
    engine = Engine.__new__(Engine)
    turn = TurnLifecycleTracker("call-interrupted").commit_customer("stop")
    turn.mark_ai_generated("first sentence. second sentence.")
    turn.mark_ai_playing("stream-2", started_at=20.0)
    turn.state = TurnLifecycleState.INTERRUPTED
    turn.interruption_reason = "confirmed_customer_transcript"
    turn.playback_interrupted_at = 21.0

    audible = await engine._finish_pipeline_stream_turn(
        turn,
        {
            "first_real_emit_ts": 20.0,
            "last_real_emit_ts": 21.0,
            "tx_bytes": 4000,
            "queued_total_bytes": 8000,
            "end_reason": "barge-in",
        },
    )

    assert audible
    assert audible != turn.ai_generated_text
    assert turn.state is TurnLifecycleState.INTERRUPTED


@pytest.mark.asyncio
async def test_transport_failure_is_interrupted_and_uses_comparable_audio_bytes():
    engine = Engine.__new__(Engine)
    turn = TurnLifecycleTracker("call-transport-failure").commit_customer("continue")
    turn.mark_ai_generated("abcdefghij")
    turn.mark_ai_playing("stream-failed", started_at=20.0)

    audible = await engine._finish_pipeline_stream_turn(
        turn,
        {
            "first_real_emit_ts": 20.0,
            "last_real_emit_ts": 21.0,
            "tx_bytes": 8000,
            "queued_total_bytes": 32000,
            "real_tx_bytes": 4000,
            "queued_target_total_bytes": 8000,
            "end_reason": "transport-failure",
        },
    )

    assert turn.state is TurnLifecycleState.INTERRUPTED
    assert turn.interruption_reason == "transport-failure"
    assert turn.audible_text_complete is False
    assert audible == "abcde"
