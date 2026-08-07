from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import time
from typing import Any, Dict, Optional


class TurnLifecycleState(str, Enum):
    CUSTOMER_COMMITTED = "customer_committed"
    AI_GENERATING = "ai_generating"
    AI_GENERATED = "ai_generated"
    AI_PLAYING = "ai_playing"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    FAILED = "failed"


def _now() -> float:
    return time.time()


def _iso_timestamp(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()


def _audible_prefix(text: str, ratio: float) -> str:
    text = str(text or "").strip()
    if not text or ratio <= 0:
        return ""
    if ratio >= 0.98:
        return text
    length = max(1, min(len(text) - 1, int(round(len(text) * ratio))))
    prefix = text[:length].rstrip()
    if " " in prefix and length < len(text) and not text[length].isspace():
        whole_word = prefix.rsplit(" ", 1)[0].rstrip()
        if whole_word:
            prefix = whole_word
    return prefix


@dataclass
class TurnLifecycle:
    call_id: str
    turn_index: int
    customer_text: str = ""
    customer_committed_at: Optional[float] = None
    ai_generated_text: str = ""
    ai_generated_at: Optional[float] = None
    playback_id: Optional[str] = None
    playback_started_at: Optional[float] = None
    playback_completed_at: Optional[float] = None
    playback_interrupted_at: Optional[float] = None
    interruption_reason: Optional[str] = None
    failure_reason: Optional[str] = None
    audible_text: str = ""
    audible_text_complete: bool = False
    state: TurnLifecycleState = TurnLifecycleState.CUSTOMER_COMMITTED

    @property
    def turn_id(self) -> str:
        return f"{self.call_id}:turn:{self.turn_index}"

    @property
    def customer_event_id(self) -> str:
        return f"{self.turn_id}:user"

    @property
    def assistant_event_id(self) -> str:
        return f"{self.turn_id}:assistant"

    def mark_ai_generating(self) -> None:
        self.state = TurnLifecycleState.AI_GENERATING

    def reset_ai_attempt(self) -> None:
        """Prepare the same committed customer turn for a retry before audio was emitted."""
        self.ai_generated_text = ""
        self.ai_generated_at = None
        self.playback_id = None
        self.playback_started_at = None
        self.playback_completed_at = None
        self.playback_interrupted_at = None
        self.interruption_reason = None
        self.failure_reason = None
        self.audible_text = ""
        self.audible_text_complete = False
        self.state = TurnLifecycleState.CUSTOMER_COMMITTED

    def mark_ai_generated(self, text: str, *, generated_at: Optional[float] = None) -> None:
        self.ai_generated_text = str(text or "").strip()
        self.ai_generated_at = generated_at if generated_at is not None else _now()
        if self.state not in {TurnLifecycleState.AI_PLAYING, TurnLifecycleState.INTERRUPTED}:
            self.state = TurnLifecycleState.AI_GENERATED

    def mark_ai_playing(self, playback_id: str, *, started_at: Optional[float] = None) -> None:
        self.playback_id = str(playback_id or "").strip() or None
        self.playback_started_at = started_at if started_at is not None else _now()
        self.state = TurnLifecycleState.AI_PLAYING

    def mark_completed(
        self,
        *,
        completed_at: Optional[float] = None,
        started_at: Optional[float] = None,
    ) -> str:
        if started_at is not None:
            self.playback_started_at = started_at
        self.playback_completed_at = completed_at if completed_at is not None else _now()
        self.playback_interrupted_at = None
        self.interruption_reason = None
        self.audible_text = self.ai_generated_text
        self.audible_text_complete = True
        self.state = TurnLifecycleState.COMPLETED
        return self.audible_text

    def mark_interrupted(
        self,
        *,
        reason: str,
        interrupted_at: Optional[float] = None,
        emitted_audio_bytes: Optional[int] = None,
        total_audio_bytes: Optional[int] = None,
        expected_audio_seconds: Optional[float] = None,
    ) -> str:
        when = interrupted_at if interrupted_at is not None else _now()
        ratio = 0.0
        if emitted_audio_bytes is not None and total_audio_bytes:
            ratio = float(emitted_audio_bytes) / float(max(1, total_audio_bytes))
        elif expected_audio_seconds and self.playback_started_at is not None:
            elapsed = max(0.0, when - self.playback_started_at)
            ratio = elapsed / max(0.001, float(expected_audio_seconds))
        self.playback_interrupted_at = when
        self.playback_completed_at = None
        self.interruption_reason = str(reason or "interrupted")
        self.audible_text = _audible_prefix(self.ai_generated_text, max(0.0, min(1.0, ratio)))
        self.audible_text_complete = False
        self.state = TurnLifecycleState.INTERRUPTED
        return self.audible_text

    def mark_failed(self, reason: str, *, failed_at: Optional[float] = None) -> None:
        self.failure_reason = str(reason or "failed")
        self.playback_completed_at = failed_at if failed_at is not None else _now()
        self.audible_text = ""
        self.audible_text_complete = False
        self.state = TurnLifecycleState.FAILED

    def customer_metadata(self) -> Dict[str, Any]:
        return {
            "event_id": self.customer_event_id,
            "turn_id": self.turn_id,
            "turn_index": self.turn_index,
            "turn_role_order": 0,
            "lifecycle_state": TurnLifecycleState.CUSTOMER_COMMITTED.value,
            "completed_at": _iso_timestamp(self.customer_committed_at),
        }

    def assistant_metadata(self) -> Dict[str, Any]:
        if self.state not in {TurnLifecycleState.COMPLETED, TurnLifecycleState.INTERRUPTED}:
            raise ValueError("Assistant transcript is only available after playback has ended")
        return {
            "event_id": self.assistant_event_id,
            "turn_id": self.turn_id,
            "turn_index": self.turn_index,
            "turn_role_order": 1,
            "lifecycle_state": self.state.value,
            "started_at": _iso_timestamp(self.playback_started_at),
            "completed_at": _iso_timestamp(self.playback_completed_at),
            "interrupted_at": _iso_timestamp(self.playback_interrupted_at),
            "playback_id": self.playback_id,
            "interruption_reason": self.interruption_reason,
            "audible_text_complete": self.audible_text_complete,
        }


class TurnLifecycleTracker:
    def __init__(self, call_id: str) -> None:
        self.call_id = str(call_id)
        self._next_turn_index = 1
        self.active_turn: Optional[TurnLifecycle] = None
        self.greeting_turn: Optional[TurnLifecycle] = None

    def begin_greeting(self, text: str, *, committed_at: Optional[float] = None) -> TurnLifecycle:
        turn = TurnLifecycle(
            call_id=self.call_id,
            turn_index=0,
            ai_generated_text=str(text or "").strip(),
            ai_generated_at=committed_at if committed_at is not None else _now(),
            state=TurnLifecycleState.AI_GENERATED,
        )
        self.greeting_turn = turn
        self.active_turn = turn
        return turn

    def commit_customer(self, text: str, *, committed_at: Optional[float] = None) -> TurnLifecycle:
        turn = TurnLifecycle(
            call_id=self.call_id,
            turn_index=self._next_turn_index,
            customer_text=str(text or "").strip(),
            customer_committed_at=committed_at if committed_at is not None else _now(),
        )
        self._next_turn_index += 1
        self.active_turn = turn
        return turn

    def interrupt_active(self, *, reason: str, interrupted_at: Optional[float] = None) -> None:
        turn = self.active_turn
        if turn is None or turn.state is not TurnLifecycleState.AI_PLAYING:
            return
        turn.playback_interrupted_at = interrupted_at if interrupted_at is not None else _now()
        turn.interruption_reason = str(reason or "interrupted")
        turn.state = TurnLifecycleState.INTERRUPTED

    def clear(self) -> None:
        self.active_turn = None
        self.greeting_turn = None
