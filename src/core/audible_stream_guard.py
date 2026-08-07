from __future__ import annotations

import audioop
from typing import List, Tuple


class PipelineAudibleStreamGuard:
    """Detect a generated TTS stream's audible end before gain processing."""

    def __init__(
        self,
        source_encoding: str,
        sample_rate: int,
        *,
        trailing_silence_ms: int = 1200,
        tail_keep_ms: int = 200,
        silence_rms_threshold: int = 80,
    ) -> None:
        self.source_encoding = self._canonical_encoding(source_encoding)
        self.sample_rate = max(1, int(sample_rate or 8000))
        self.trailing_silence_ms = max(100, int(trailing_silence_ms))
        self.tail_keep_ms = max(0, min(int(tail_keep_ms), self.trailing_silence_ms))
        self.silence_rms_threshold = max(0, int(silence_rms_threshold))
        self.seen_speech = False
        self.reached_end = False
        self._pending_silence: List[Tuple[bytes, float]] = []
        self._pending_silence_ms = 0.0

    @staticmethod
    def _canonical_encoding(value: str) -> str:
        return str(value or "").strip().lower().replace("-", "").replace("_", "")

    def _decode_pcm16(self, chunk: bytes) -> bytes | None:
        encoding = self.source_encoding
        try:
            if encoding in {"ulaw", "mulaw", "pcmu", "g711ulaw", "mulaw8"}:
                return audioop.ulaw2lin(chunk, 2)
            if encoding in {"alaw", "pcma", "g711alaw", "alaw8"}:
                return audioop.alaw2lin(chunk, 2)
            if encoding in {
                "pcm",
                "pcm16",
                "pcm16le",
                "pcms16le",
                "linear16",
                "slin",
                "slin16",
                "s16le",
                "raw",
            }:
                usable = len(chunk) - (len(chunk) % 2)
                return chunk[:usable]
            if encoding in {"pcm16be", "pcms16be", "s16be"}:
                usable = len(chunk) - (len(chunk) % 2)
                return audioop.byteswap(chunk[:usable], 2)
        except (audioop.error, ValueError):
            return None
        return None

    def _duration_ms(self, pcm16: bytes) -> float:
        return (len(pcm16) / 2.0) * 1000.0 / float(self.sample_rate)

    def _kept_tail(self) -> List[bytes]:
        kept: List[bytes] = []
        kept_ms = 0.0
        for chunk, duration_ms in self._pending_silence:
            if kept_ms >= self.tail_keep_ms:
                break
            kept.append(chunk)
            kept_ms += duration_ms
        return kept

    def feed(self, chunk: bytes) -> tuple[List[bytes], bool]:
        if self.reached_end or not chunk:
            return [], self.reached_end

        raw = bytes(chunk)
        pcm16 = self._decode_pcm16(raw)
        if not pcm16:
            pending = [item[0] for item in self._pending_silence]
            self._pending_silence.clear()
            self._pending_silence_ms = 0.0
            return pending + [raw], False

        try:
            rms = audioop.rms(pcm16, 2)
        except audioop.error:
            rms = self.silence_rms_threshold + 1

        if rms > self.silence_rms_threshold:
            self.seen_speech = True
            pending = [item[0] for item in self._pending_silence]
            self._pending_silence.clear()
            self._pending_silence_ms = 0.0
            return pending + [raw], False

        if not self.seen_speech:
            return [raw], False

        duration_ms = self._duration_ms(pcm16)
        self._pending_silence.append((raw, duration_ms))
        self._pending_silence_ms += duration_ms
        if self._pending_silence_ms + 0.001 < self.trailing_silence_ms:
            return [], False

        kept = self._kept_tail()
        self._pending_silence.clear()
        self._pending_silence_ms = 0.0
        self.reached_end = True
        return kept, True

    def finish(self) -> List[bytes]:
        if self.reached_end:
            return []
        pending = [item[0] for item in self._pending_silence]
        self._pending_silence.clear()
        self._pending_silence_ms = 0.0
        return pending
