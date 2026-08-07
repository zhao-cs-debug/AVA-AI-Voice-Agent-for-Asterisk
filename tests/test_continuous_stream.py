import asyncio
import math
import pytest

from src.core.streaming_playback_manager import StreamingPlaybackManager


class Dummy:
    pass


def make_manager(**overrides):
    cfg = {
        'continuous_stream': True,
        'min_start_ms': 120,
        'low_watermark_ms': 80,
        'chunk_size_ms': 20,
        'sample_rate': 8000,
        'normalizer': {'enabled': True, 'target_rms': 1400, 'max_gain_db': 9.0},
    }
    cfg.update(overrides)
    return StreamingPlaybackManager(
        session_store=Dummy(),
        ari_client=Dummy(),
        conversation_coordinator=None,
        fallback_playback_manager=None,
        streaming_config=cfg,
        audio_transport="audiosocket",
    )


def test_continuous_stream_skips_warmup_for_non_first_segment():
    mgr = make_manager()
    call_id = "test-call-1"
    stream_id = "stream:resp:test-call-1:1"
    # Simulate active stream entry minimal fields
    mgr.active_streams[call_id] = {
        'stream_id': stream_id,
        'min_start_chunks': mgr.min_start_chunks,
    }
    mgr._startup_ready[call_id] = False

    # Non-first segment
    stream_info = {
        'segments_played': 1,
        'min_start_chunks': mgr.min_start_chunks,
    }
    jitter = asyncio.Queue()

    ready = mgr._ensure_startup_ready(call_id, stream_id, jitter, stream_info)
    assert ready is True
    assert mgr._startup_ready.get(call_id) is True
    assert stream_info.get('startup_ready') is True


def test_first_segment_requires_min_start_when_empty():
    mgr = make_manager()
    call_id = "test-call-2"
    stream_id = "stream:resp:test-call-2:1"
    mgr._startup_ready[call_id] = False
    stream_info = {
        'segments_played': 0,
        'min_start_chunks': 4,
    }
    jitter = asyncio.Queue()
    # empty jitter buffer -> available_frames = 0 < 4
    ready = mgr._ensure_startup_ready(call_id, stream_id, jitter, stream_info)
    assert ready is False
    assert mgr._startup_ready.get(call_id) is False


@pytest.mark.asyncio
async def test_mark_segment_boundary_increments_and_resets_attack():
    mgr = make_manager()
    call_id = "test-call-3"
    # Prepare active stream with sample rate and existing fields
    mgr.active_streams[call_id] = {
        'stream_id': "stream:resp:test-call-3:1",
        'target_sample_rate': 8000,
        'segments_played': 0,
    }
    # attack bytes expected: sr * (attack_ms/1000) * 2
    expected_attack = int(max(0, int(8000 * (mgr.attack_ms / 1000.0)) * 2))

    await mgr.mark_segment_boundary(call_id)

    info = mgr.active_streams[call_id]
    assert info['segments_played'] == 1
    assert info.get('attack_bytes_remaining') == expected_attack


@pytest.mark.asyncio
async def test_active_stream_rejects_a_different_producer_queue():
    mgr = make_manager()
    call_id = "test-call-queue-owner"
    original_queue = asyncio.Queue()
    mgr.active_streams[call_id] = {
        'stream_id': 'stream-owner',
        'audio_queue': original_queue,
    }

    stream_id = await mgr.start_streaming_playback(call_id, asyncio.Queue())

    assert stream_id is None


@pytest.mark.asyncio
async def test_first_real_audio_emit_timestamp_is_recorded_once(monkeypatch):
    mgr = make_manager()
    call_id = "test-call-first-emit"
    stream_id = "stream:first-emit"
    mgr.active_streams[call_id] = {
        'stream_id': stream_id,
        'frames_sent': 0,
        'first_real_emit_ts': None,
        'last_real_emit_ts': None,
    }

    async def gating_started(*_args, **_kwargs):
        return True

    async def send_ok(*_args, **_kwargs):
        return True

    monkeypatch.setattr(mgr, "_ensure_deferred_gating_started", gating_started)
    monkeypatch.setattr(mgr, "_send_audio_chunk", send_ok)

    assert await mgr._emit_frame(call_id, stream_id, b"first", "ulaw", 8000, filler=False) == "sent"
    first_timestamp = mgr.active_streams[call_id]["first_real_emit_ts"]
    await asyncio.sleep(0.01)
    assert await mgr._emit_frame(call_id, stream_id, b"second", "ulaw", 8000, filler=False) == "sent"

    assert first_timestamp is not None
    assert mgr.active_streams[call_id]["first_real_emit_ts"] == first_timestamp
    assert mgr.active_streams[call_id]["last_real_emit_ts"] >= first_timestamp


@pytest.mark.asyncio
async def test_stop_settles_all_tasks_when_paused_jitter_queue_is_full(monkeypatch):
    mgr = make_manager(fallback_timeout_ms=10_000)
    call_id = "test-call-stop-paused-full-jitter"
    stream_id = "stream:resp:test-call-stop-paused-full-jitter:1"
    audio_chunks = asyncio.Queue()
    audio_chunks.put_nowait(b"new-provider-audio")

    class ObservedQueue(asyncio.Queue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.put_started = asyncio.Event()

        async def put(self, item):
            self.put_started.set()
            await super().put(item)

    jitter_buffer = ObservedQueue(maxsize=1)
    jitter_buffer.put_nowait(b"queued-audio")
    resume_event = asyncio.Event()

    async def paused_pacer():
        await resume_event.wait()
        await asyncio.Event().wait()

    async def keepalive():
        await asyncio.Event().wait()

    producer = asyncio.create_task(
        mgr._stream_audio_loop(call_id, stream_id, audio_chunks, jitter_buffer)
    )
    pacer = asyncio.create_task(paused_pacer())
    keepalive_task = asyncio.create_task(keepalive())
    mgr.active_streams[call_id] = {
        "stream_id": stream_id,
        "streaming_task": producer,
        "pacer_task": pacer,
        "keepalive_task": keepalive_task,
        "playback_resume_event": resume_event,
        "stop_event": asyncio.Event(),
        "end_reason": "barge-in",
    }
    mgr.jitter_buffers[call_id] = jitter_buffer
    mgr.keepalive_tasks[call_id] = keepalive_task

    async def cleanup(_call_id, _stream_id):
        mgr.active_streams.pop(_call_id, None)
        mgr.jitter_buffers.pop(_call_id, None)

    monkeypatch.setattr(mgr, "_cleanup_stream", cleanup)
    await asyncio.wait_for(jitter_buffer.put_started.wait(), timeout=0.2)
    assert audio_chunks.empty()

    stop_task = asyncio.create_task(mgr.stop_streaming_playback(call_id))
    done, pending = await asyncio.wait({stop_task}, timeout=0.2)
    try:
        assert not pending
        assert stop_task.result() is True
        assert producer.done()
        assert pacer.done()
        assert keepalive_task.done()
        assert jitter_buffer.empty()
        assert call_id not in mgr.active_streams
    finally:
        while not jitter_buffer.empty():
            jitter_buffer.get_nowait()
        for task in (producer, pacer, keepalive_task):
            if not task.done():
                task.cancel()
        await asyncio.wait_for(
            asyncio.gather(producer, pacer, keepalive_task, stop_task, return_exceptions=True),
            timeout=1.0,
        )
