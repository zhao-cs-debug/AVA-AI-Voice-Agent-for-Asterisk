"""Opt-in per-call DeepFilterNet3 streaming denoising for pipeline STT."""

from __future__ import annotations

import ctypes
import fnmatch
import hashlib
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Sequence

import numpy as np


def _env_bool(value: str, default: bool = False) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return default
    return token in {"1", "true", "yes", "on"}


def _env_csv(value: str, default: Sequence[str]) -> tuple[str, ...]:
    values = tuple(item.strip() for item in str(value or "").split(",") if item.strip())
    return values or tuple(default)


@dataclass(frozen=True)
class DenoiseConfig:
    enabled: bool = False
    provider: str = "deepfilternet3_libdf"
    library_path: str = "/app/models/libdf.so"
    library_sha256: str = "96977141cfa48bd58ed0f90ecd576c5be868b4a1d0f554c8fd6219e9e0c088db"
    model_path: str = "/app/models/DeepFilterNet3_onnx.tar.gz"
    model_sha256: str = "c94d91f70911001c946e0fabb4aa9adc37045f45a03b56008cb0c8244cb63616"
    context_allowlist: tuple[str, ...] = ("cenani_dev_*",)
    stt_allowlist: tuple[str, ...] = ("local_stt",)
    input_sample_rate: int = 16000
    model_sample_rate: int = 48000
    attenuation_limit_db: float = 18.0
    resample_quality: str = "LQ"
    max_processing_ratio: float = 0.75

    @classmethod
    def from_environment(cls, environment=None) -> "DenoiseConfig":
        env = environment if environment is not None else os.environ
        return cls(
            enabled=_env_bool(env.get("VOICEAI_DENOISE_ENABLED", "false")),
            provider=str(env.get("VOICEAI_DENOISE_PROVIDER", "deepfilternet3_libdf")),
            library_path=str(env.get("VOICEAI_DENOISE_LIBRARY_PATH", "/app/models/libdf.so")),
            library_sha256=str(
                env.get(
                    "VOICEAI_DENOISE_LIBRARY_SHA256",
                    "96977141cfa48bd58ed0f90ecd576c5be868b4a1d0f554c8fd6219e9e0c088db",
                )
            ).strip().lower(),
            model_path=str(
                env.get("VOICEAI_DENOISE_MODEL_PATH", "/app/models/DeepFilterNet3_onnx.tar.gz")
            ),
            model_sha256=str(
                env.get(
                    "VOICEAI_DENOISE_MODEL_SHA256",
                    "c94d91f70911001c946e0fabb4aa9adc37045f45a03b56008cb0c8244cb63616",
                )
            ).strip().lower(),
            context_allowlist=_env_csv(
                env.get("VOICEAI_DENOISE_CONTEXT_ALLOWLIST", "cenani_dev_*"),
                ("cenani_dev_*",),
            ),
            stt_allowlist=_env_csv(
                env.get("VOICEAI_DENOISE_STT_ALLOWLIST", "local_stt"),
                ("local_stt",),
            ),
            attenuation_limit_db=float(env.get("VOICEAI_DENOISE_ATTEN_LIMIT_DB", "18")),
            resample_quality=str(env.get("VOICEAI_DENOISE_RESAMPLE_QUALITY", "LQ")),
            max_processing_ratio=float(env.get("VOICEAI_DENOISE_MAX_PROCESSING_RATIO", "0.75")),
        )

    def allows(self, context_name: Optional[str], stt_key: Optional[str]) -> bool:
        context = str(context_name or "").strip()
        component = str(stt_key or "").strip()
        return bool(
            self.enabled
            and self.provider == "deepfilternet3_libdf"
            and any(fnmatch.fnmatchcase(context, pattern) for pattern in self.context_allowlist)
            and component in self.stt_allowlist
        )


class LibDFBackend:
    """Thin wrapper around DeepFilterNet v0.5.6's official streaming C API."""

    def __init__(self, config: DenoiseConfig):
        if not os.path.isfile(config.library_path):
            raise FileNotFoundError(f"DeepFilterNet library not found: {config.library_path}")
        if not os.path.isfile(config.model_path):
            raise FileNotFoundError(f"DeepFilterNet model not found: {config.model_path}")
        if os.path.getsize(config.library_path) < 1024 or os.path.getsize(config.model_path) < 1024:
            raise RuntimeError("DeepFilterNet runtime artifact is incomplete")
        self._verify_sha256(
            config.library_path,
            config.library_sha256,
            "DeepFilterNet library",
        )
        self._verify_sha256(
            config.model_path,
            config.model_sha256,
            "DeepFilterNet model",
        )

        self._library = ctypes.CDLL(config.library_path)
        self._library.df_create.argtypes = [ctypes.c_char_p, ctypes.c_float]
        self._library.df_create.restype = ctypes.c_void_p
        self._library.df_get_frame_length.argtypes = [ctypes.c_void_p]
        self._library.df_get_frame_length.restype = ctypes.c_size_t
        self._library.df_process_frame.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_float),
            ctypes.POINTER(ctypes.c_float),
        ]
        self._library.df_process_frame.restype = ctypes.c_float
        self._library.df_free.argtypes = [ctypes.c_void_p]

        self._state = self._library.df_create(
            os.fsencode(config.model_path), ctypes.c_float(config.attenuation_limit_db)
        )
        if not self._state:
            raise RuntimeError("DeepFilterNet state creation failed")
        self.frame_size = int(self._library.df_get_frame_length(self._state))
        if self.frame_size != 480:
            self.close()
            raise RuntimeError(f"Unexpected DeepFilterNet3 frame size: {self.frame_size}")

    @staticmethod
    def _verify_sha256(path: str, expected: str, label: str) -> None:
        if not expected:
            return
        digest = hashlib.sha256()
        with open(path, "rb") as artifact:
            for block in iter(lambda: artifact.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected:
            raise RuntimeError(f"{label} checksum mismatch")

    def process_frame(self, samples: np.ndarray) -> np.ndarray:
        if not self._state:
            raise RuntimeError("DeepFilterNet state is closed")
        source = np.ascontiguousarray(samples, dtype=np.float32)
        if len(source) != self.frame_size:
            raise ValueError(f"Expected {self.frame_size} samples, received {len(source)}")
        output = np.empty(self.frame_size, dtype=np.float32)
        self._library.df_process_frame(
            self._state,
            source.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            output.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        )
        if not np.isfinite(output).all():
            raise RuntimeError("DeepFilterNet returned non-finite audio")
        return output

    def close(self) -> None:
        state, self._state = self._state, None
        if state:
            self._library.df_free(state)


def _default_resampler(source_rate, target_rate, channels, dtype, quality):
    try:
        import soxr
    except ImportError as exc:
        raise RuntimeError("python-soxr is required for realtime denoising") from exc
    return soxr.ResampleStream(source_rate, target_rate, channels, dtype=dtype, quality=quality)


@dataclass
class _CallDenoiser:
    backend: LibDFBackend
    upsampler: object
    downsampler: object
    max_processing_ratio: float
    model_pending: np.ndarray = field(default_factory=lambda: np.empty(0, dtype=np.float32))
    disabled: bool = False
    fallback_count: int = 0
    processed_chunks: int = 0
    processing_ms_total: float = 0.0
    last_error: Optional[str] = None
    lock: threading.RLock = field(default_factory=threading.RLock)

    def process(self, pcm16: bytes, sample_rate: int) -> bytes:
        with self.lock:
            return self._process_locked(pcm16, sample_rate)

    def _process_locked(self, pcm16: bytes, sample_rate: int) -> bytes:
        if self.disabled or not pcm16:
            return pcm16
        if sample_rate != 16000:
            self._fail(f"unsupported input sample rate: {sample_rate}")
            return pcm16

        started = time.perf_counter()
        try:
            source_i16 = np.frombuffer(pcm16, dtype="<i2")
            source = source_i16.astype(np.float32) / 32768.0
            model_audio = self.upsampler.resample_chunk(source, last=False)
            if self.model_pending.size:
                model_audio = np.concatenate((self.model_pending, model_audio))

            frame_count = len(model_audio) // self.backend.frame_size
            frame_samples = frame_count * self.backend.frame_size
            self.model_pending = model_audio[frame_samples:].copy()
            if not frame_count:
                return b""

            enhanced = [
                self.backend.process_frame(
                    model_audio[index : index + self.backend.frame_size]
                )
                for index in range(0, frame_samples, self.backend.frame_size)
            ]
            output = self.downsampler.resample_chunk(np.concatenate(enhanced), last=False)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            audio_ms = len(source_i16) * 1000.0 / sample_rate
            if audio_ms and elapsed_ms / audio_ms > self.max_processing_ratio:
                self._fail(
                    f"processing ratio {elapsed_ms / audio_ms:.3f} exceeded "
                    f"{self.max_processing_ratio:.3f}"
                )
                return pcm16

            self.processed_chunks += 1
            self.processing_ms_total += elapsed_ms
            return np.clip(output * 32768.0, -32768, 32767).astype("<i2").tobytes()
        except Exception as exc:
            self._fail(str(exc))
            return pcm16

    def _fail(self, reason: str) -> None:
        self.disabled = True
        self.fallback_count += 1
        self.last_error = reason

    def close(self) -> None:
        with self.lock:
            self.backend.close()


class RealtimeDenoiserManager:
    """Owns independent stateful denoisers for eligible calls."""

    def __init__(
        self,
        config: DenoiseConfig,
        *,
        backend_factory: Callable[[DenoiseConfig], LibDFBackend] = LibDFBackend,
        resampler_factory: Callable = _default_resampler,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self._backend_factory = backend_factory
        self._resampler_factory = resampler_factory
        self._clock = clock
        self._states: Dict[str, _CallDenoiser] = {}
        self._failed_calls: Dict[str, str] = {}
        self._skipped_calls: set[str] = set()
        self._deferred_retry_at: Dict[str, float] = {}
        self._reported_failures: set[str] = set()
        self._lock = threading.RLock()

    @classmethod
    def from_environment(cls, environment=None) -> "RealtimeDenoiserManager":
        return cls(DenoiseConfig.from_environment(environment))

    @property
    def active_call_count(self) -> int:
        with self._lock:
            return len(self._states)

    def is_enhancing(self, call_id: str) -> bool:
        with self._lock:
            state = self._states.get(call_id)
            return bool(state and not state.disabled)

    def has_call_result(self, call_id: str) -> bool:
        """Return whether eligibility/initialization has already been resolved."""
        with self._lock:
            return (
                call_id in self._states
                or call_id in self._failed_calls
                or call_id in self._skipped_calls
            )

    def should_attempt_open(self, call_id: str) -> bool:
        with self._lock:
            if (
                call_id in self._states
                or call_id in self._failed_calls
                or call_id in self._skipped_calls
            ):
                return False
            return self._clock() >= self._deferred_retry_at.get(call_id, 0.0)

    def open_call(self, call_id: str, *, context_name: Optional[str], stt_key: Optional[str]) -> bool:
        if not self.config.allows(context_name, stt_key):
            # A fully resolved but ineligible call is a stable pass-through decision.
            # Missing context/component values may be filled later, so those remain retryable.
            if str(context_name or "").strip() and str(stt_key or "").strip():
                with self._lock:
                    self._skipped_calls.add(call_id)
                    self._deferred_retry_at.pop(call_id, None)
            else:
                with self._lock:
                    self._deferred_retry_at[call_id] = self._clock() + 1.0
            return False
        with self._lock:
            self._deferred_retry_at.pop(call_id, None)
            if call_id in self._states:
                return True
            if call_id in self._failed_calls:
                return False
            try:
                self._states[call_id] = _CallDenoiser(
                    backend=self._backend_factory(self.config),
                    upsampler=self._resampler_factory(
                        self.config.input_sample_rate,
                        self.config.model_sample_rate,
                        1,
                        "float32",
                        self.config.resample_quality,
                    ),
                    downsampler=self._resampler_factory(
                        self.config.model_sample_rate,
                        self.config.input_sample_rate,
                        1,
                        "float32",
                        self.config.resample_quality,
                    ),
                    max_processing_ratio=self.config.max_processing_ratio,
                )
            except Exception as exc:
                self._failed_calls[call_id] = str(exc)
                return False
            return True

    def process(self, call_id: str, pcm16: bytes, sample_rate: int = 16000) -> bytes:
        with self._lock:
            state = self._states.get(call_id)
        return state.process(pcm16, sample_rate) if state else pcm16

    def close_call(self, call_id: str) -> Dict[str, object]:
        with self._lock:
            state = self._states.pop(call_id, None)
            initialization_error = self._failed_calls.pop(call_id, None)
            self._skipped_calls.discard(call_id)
            self._deferred_retry_at.pop(call_id, None)
            self._reported_failures.discard(call_id)
            snapshot = self._state_stats(state) if state else (
                {"disabled": True, "last_error": initialization_error}
                if initialization_error
                else {}
            )
        if state:
            state.close()
        return snapshot

    def take_unreported_failure(self, call_id: str) -> Optional[str]:
        """Return a call failure once so the realtime path never logs per frame."""
        with self._lock:
            state = self._states.get(call_id)
            reason = state.last_error if state else self._failed_calls.get(call_id)
            if not reason or call_id in self._reported_failures:
                return None
            self._reported_failures.add(call_id)
            return reason

    @staticmethod
    def _state_stats(state: _CallDenoiser) -> Dict[str, object]:
        return {
            "disabled": state.disabled,
            "fallback_count": state.fallback_count,
            "processed_chunks": state.processed_chunks,
            "processing_ms_total": state.processing_ms_total,
            "pending_model_samples": int(state.model_pending.size),
            "last_error": state.last_error,
        }

    def stats(self, call_id: str) -> Dict[str, object]:
        with self._lock:
            state = self._states.get(call_id)
            if not state:
                error = self._failed_calls.get(call_id)
                return {"disabled": True, "last_error": error} if error else {}
            return self._state_stats(state)


__all__ = ["DenoiseConfig", "LibDFBackend", "RealtimeDenoiserManager"]
