import os
import re
import wave
import threading
from typing import Dict, FrozenSet, Mapping, Optional, Tuple

import audioop


class AudioCaptureManager:
    """Utility for capturing per-call audio streams to WAV files."""

    def __init__(self, base_dir: str = "/tmp/ai-engine-captures", keep_files: bool = False):
        self.base_dir = base_dir
        self.keep_files = keep_files
        self._lock = threading.Lock()
        # key -> (wave.Wave_write, sample_rate)
        self._handles: Dict[Tuple[str, str], Tuple[wave.Wave_write, int]] = {}
        try:
            os.makedirs(self.base_dir, mode=0o700, exist_ok=True)
            try:
                os.chmod(self.base_dir, 0o700)
            except Exception:
                pass
        except Exception:
            pass


    def _open_handle(self, call_id: str, stream_name: str, sample_rate: int) -> wave.Wave_write:
        path = os.path.join(self.base_dir, call_id, f"{stream_name}.wav")
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, mode=0o700, exist_ok=True)
        try:
            os.chmod(dir_path, 0o700)
        except Exception:
            pass
        wf = wave.open(path, "wb")
        wf.setnchannels(1)
        wf.setsampwidth(2)  # PCM16
        wf.setframerate(sample_rate)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
        return wf

    def append_pcm16(self, call_id: str, stream_name: str, pcm16: bytes, sample_rate: int) -> None:
        if not pcm16:
            return
        key = (call_id, stream_name)
        with self._lock:
            handle = self._handles.get(key)
            if handle is None:
                wf = self._open_handle(call_id, stream_name, sample_rate)
                self._handles[key] = (wf, sample_rate)
            else:
                wf, existing_rate = handle
                if existing_rate != sample_rate:
                    # Close and reopen with new rate to avoid inconsistent headers
                    try:
                        wf.close()
                    except Exception:
                        pass
                    wf = self._open_handle(call_id, stream_name, sample_rate)
                    self._handles[key] = (wf, sample_rate)
            wf = self._handles[key][0]
            try:
                wf.writeframes(pcm16)
            except Exception:
                # On write failure, close the handle to avoid corrupted files
                try:
                    wf.close()
                except Exception:
                    pass
                self._handles.pop(key, None)

    def append_encoded(
        self,
        call_id: str,
        stream_name: str,
        payload: bytes,
        encoding: str,
        sample_rate: int,
    ) -> None:
        if not payload:
            return
        encoding = (encoding or "").lower()
        try:
            if encoding in ("ulaw", "mulaw", "g711_ulaw", "mu-law"):
                pcm16 = audioop.ulaw2lin(payload, 2)
                rate = sample_rate or 8000
            elif encoding in ("slin16", "linear16", "pcm16"):
                pcm16 = payload
                rate = sample_rate or 16000
            else:
                # Fallback: treat as PCM16
                pcm16 = payload
                rate = sample_rate or 16000
            self.append_pcm16(call_id, stream_name, pcm16, rate)
        except Exception as e:
            # Log capture failures for debugging but don't break call flow
            import structlog
            logger = structlog.get_logger(__name__)
            logger.warning(
                "Audio capture failed",
                call_id=call_id,
                stream_name=stream_name,
                encoding=encoding,
                sample_rate=sample_rate,
                payload_len=len(payload) if payload else 0,
                error=str(e),
                exc_info=True,
            )

    def close_call(self, call_id: str) -> None:
        keys_to_close = []
        with self._lock:
            for key, (wf, _rate) in list(self._handles.items()):
                if key[0] == call_id:
                    try:
                        wf.close()
                    except Exception:
                        pass
                    keys_to_close.append(key)
            for key in keys_to_close:
                self._handles.pop(key, None)
        # Only delete files if not in diagnostic/keep mode
        if self.keep_files:
            return
        # After closing wave handles, remove captured files and call directory
        try:
            call_dir = os.path.join(self.base_dir, call_id)
            if os.path.isdir(call_dir):
                try:
                    for name in os.listdir(call_dir):
                        fpath = os.path.join(call_dir, name)
                        try:
                            if os.path.isfile(fpath):
                                os.remove(fpath)
                        except Exception:
                            pass
                    os.rmdir(call_dir)
                except Exception:
                    pass
        except Exception:
            pass


class CustomerAudioCaptureManager:
    """Opt-in capture of caller-side PCM for repeatable development tests."""

    DEFAULT_BASE_DIR = "/tmp/ai-engine-customer-captures"

    def __init__(
        self,
        *,
        enabled: bool,
        caller_allowlist,
        base_dir: str = DEFAULT_BASE_DIR,
    ):
        normalized_callers = {
            str(value or "").strip()
            for value in (caller_allowlist or set())
            if str(value or "").strip()
        }
        # Capturing without an explicit allowlist is intentionally forbidden.
        self.enabled = bool(enabled and normalized_callers)
        self.caller_allowlist: FrozenSet[str] = frozenset(normalized_callers)
        self.base_dir = str(base_dir or self.DEFAULT_BASE_DIR)
        self._capture = (
            AudioCaptureManager(base_dir=self.base_dir, keep_files=True)
            if self.enabled
            else None
        )

    @classmethod
    def from_environment(
        cls,
        environment: Optional[Mapping[str, str]] = None,
    ) -> "CustomerAudioCaptureManager":
        env = environment if environment is not None else os.environ
        enabled = str(env.get("VOICEAI_CUSTOMER_CAPTURE_ENABLED", "false")).strip().lower()
        raw_callers = str(env.get("VOICEAI_CUSTOMER_CAPTURE_CALLERS", ""))
        callers = {
            value.strip()
            for value in re.split(r"[,;\s]+", raw_callers)
            if value.strip()
        }
        return cls(
            enabled=enabled in {"1", "true", "yes", "on"},
            caller_allowlist=callers,
            base_dir=str(
                env.get("VOICEAI_CUSTOMER_CAPTURE_DIR", cls.DEFAULT_BASE_DIR)
                or cls.DEFAULT_BASE_DIR
            ),
        )

    def append_pcm16(
        self,
        *,
        call_id: str,
        caller_number: Optional[str],
        called_number: Optional[str],
        pcm16: bytes,
        sample_rate: int,
    ) -> bool:
        if not self.enabled or self._capture is None or not pcm16:
            return False
        identifiers = {
            str(caller_number or "").strip(),
            str(called_number or "").strip(),
        }
        if not self.caller_allowlist.intersection(identifiers):
            return False
        self._capture.append_pcm16(call_id, "customer", pcm16, sample_rate)
        return True

    def close_call(self, call_id: str) -> None:
        if self._capture is not None:
            self._capture.close_call(call_id)
