import asyncio

import pytest

from src.engine import Engine, _PipelinePlaybackInterrupted


class _StreamOwnershipStub:
    def __init__(self, stream_id="stream-1"):
        self.stream_id = stream_id
        self.active = True

    def is_stream_active(self, call_id, stream_id=None):
        return self.active and stream_id == self.stream_id


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
