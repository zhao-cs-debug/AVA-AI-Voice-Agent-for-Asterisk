import os

import numpy as np
import pytest

from src.audio.realtime_denoiser import DenoiseConfig, LibDFBackend, RealtimeDenoiserManager


class _FakeStream:
    def __init__(self, source_rate, target_rate, channels, dtype, quality):
        self.ratio = target_rate / source_rate

    def resample_chunk(self, samples, last=False):
        count = int(round(len(samples) * self.ratio))
        if count == 0:
            return np.empty(0, dtype=np.float32)
        positions = np.arange(count, dtype=np.float64) / self.ratio
        return np.interp(positions, np.arange(len(samples)), samples).astype(np.float32)


class _FakeBackend:
    frame_size = 480

    def __init__(self):
        self.closed = False

    def process_frame(self, samples):
        return samples * 0.5

    def close(self):
        self.closed = True


def _pcm(sample_count=320, value=12000):
    return np.full(sample_count, value, dtype=np.int16).tobytes()


def _manager(**overrides):
    config = DenoiseConfig(
        enabled=True,
        context_allowlist=("cenani_dev_*",),
        stt_allowlist=("local_stt",),
        max_processing_ratio=2.0,
        **overrides,
    )
    return RealtimeDenoiserManager(
        config,
        backend_factory=lambda _config: _FakeBackend(),
        resampler_factory=_FakeStream,
    )


def test_disabled_manager_returns_original_bytes_exactly():
    manager = RealtimeDenoiserManager(DenoiseConfig(enabled=False))
    audio = os.urandom(640)

    assert manager.process("call-1", audio) == audio
    assert manager.active_call_count == 0


def test_only_allowed_development_pipeline_opens_state():
    manager = _manager()
    prod_audio = _pcm()

    assert manager.open_call("dev", context_name="cenani_dev_agent_7", stt_key="local_stt")
    assert not manager.open_call("prod", context_name="cenani_prod_agent_19", stt_key="local_stt")
    assert not manager.open_call("wrong-stt", context_name="cenani_dev_agent_7", stt_key="openai_stt")
    assert manager.active_call_count == 1
    assert manager.has_call_result("prod") is True
    assert manager.is_enhancing("prod") is False
    assert manager.process("prod", prod_audio) == prod_audio


def test_missing_context_remains_retryable_until_development_context_is_available():
    now = [100.0]
    config = DenoiseConfig(
        enabled=True,
        context_allowlist=("cenani_dev_*",),
        stt_allowlist=("local_stt",),
        max_processing_ratio=2.0,
    )
    manager = RealtimeDenoiserManager(
        config,
        backend_factory=lambda _config: _FakeBackend(),
        resampler_factory=_FakeStream,
        clock=lambda: now[0],
    )

    assert not manager.open_call("late", context_name=None, stt_key="local_stt")
    assert manager.has_call_result("late") is False
    assert manager.should_attempt_open("late") is False
    now[0] += 1.0
    assert manager.should_attempt_open("late") is True
    assert manager.open_call(
        "late", context_name="cenani_dev_agent_7", stt_key="local_stt"
    )
    assert manager.should_attempt_open("late") is False


def test_call_states_are_isolated_and_cleanup_closes_backend():
    manager = _manager()
    assert manager.open_call("a", context_name="cenani_dev_agent_7", stt_key="local_stt")
    assert manager.open_call("b", context_name="cenani_dev_agent_8", stt_key="local_stt")
    state_a = manager._states["a"]
    state_b = manager._states["b"]

    assert state_a is not state_b
    assert state_a.backend is not state_b.backend

    manager.close_call("a")
    assert state_a.backend.closed
    assert "a" not in manager._states
    assert "b" in manager._states


def test_chunked_audio_is_denoised_without_changing_16k_duration():
    manager = _manager()
    assert manager.open_call("call", context_name="cenani_dev_agent_7", stt_key="local_stt")
    assert manager.is_enhancing("call") is True
    original = _pcm()

    first = manager.process("call", original)
    second = manager.process("call", original)

    assert abs(len(first) + len(second) - 2 * len(original)) <= 320
    assert np.abs(np.frombuffer(second, dtype=np.int16)).mean() < 9000


def test_backend_failure_returns_original_and_disables_only_that_call():
    class _FailingBackend(_FakeBackend):
        def process_frame(self, samples):
            raise RuntimeError("inference failed")

    manager = RealtimeDenoiserManager(
        DenoiseConfig(enabled=True, context_allowlist=("cenani_dev_*",)),
        backend_factory=lambda _config: _FailingBackend(),
        resampler_factory=_FakeStream,
    )
    assert manager.open_call("call", context_name="cenani_dev_agent_7", stt_key="local_stt")
    original = _pcm()

    assert manager.process("call", original) == original
    assert manager.process("call", original) == original
    assert manager.stats("call")["fallback_count"] == 1
    assert manager.stats("call")["disabled"] is True
    assert manager.is_enhancing("call") is False
    assert manager.take_unreported_failure("call") == "inference failed"
    assert manager.take_unreported_failure("call") is None


def test_close_returns_final_metrics_and_forgets_call_result():
    manager = _manager()
    assert manager.open_call("call", context_name="cenani_dev_agent_7", stt_key="local_stt")
    manager.process("call", _pcm())

    stats = manager.close_call("call")

    assert stats["processed_chunks"] == 1
    assert manager.active_call_count == 0
    assert manager.has_call_result("call") is False


def test_model_checksum_is_validated_before_loading_native_library(tmp_path):
    library = tmp_path / "libdf.so"
    model = tmp_path / "model.tar.gz"
    library.write_bytes(b"library" * 256)
    model.write_bytes(b"model" * 256)
    config = DenoiseConfig(
        enabled=True,
        library_path=str(library),
        library_sha256="",
        model_path=str(model),
        model_sha256="0" * 64,
    )

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        LibDFBackend(config)


def test_library_checksum_is_validated_before_loading_native_code(tmp_path):
    library = tmp_path / "libdf.so"
    model = tmp_path / "model.tar.gz"
    library.write_bytes(b"library" * 256)
    model.write_bytes(b"model" * 256)
    config = DenoiseConfig(
        enabled=True,
        library_path=str(library),
        library_sha256="0" * 64,
        model_path=str(model),
        model_sha256="",
    )

    with pytest.raises(RuntimeError, match="library checksum mismatch"):
        LibDFBackend(config)
