import pytest

from src.core.turn_lifecycle import (
    TurnLifecycleState,
    TurnLifecycleTracker,
)


def test_tracker_assigns_stable_role_order_and_turn_indices():
    tracker = TurnLifecycleTracker("call-1")

    greeting = tracker.begin_greeting("您好")
    first = tracker.commit_customer("我是本人")
    second = tracker.commit_customer("暂时失业了")

    assert greeting.turn_index == 0
    assert greeting.assistant_event_id == "call-1:turn:0:assistant"
    assert first.turn_index == 1
    assert first.customer_event_id == "call-1:turn:1:user"
    assert first.assistant_event_id == "call-1:turn:1:assistant"
    assert second.turn_index == 2
    assert first.customer_metadata()["turn_role_order"] == 0
    first.mark_ai_generated("response")
    first.mark_ai_playing("stream-1")
    first.mark_completed()
    assert first.assistant_metadata()["turn_role_order"] == 1


def test_customer_commit_is_available_before_ai_generation_finishes():
    tracker = TurnLifecycleTracker("call-2")

    turn = tracker.commit_customer("我不知道怎么办")

    assert turn.state is TurnLifecycleState.CUSTOMER_COMMITTED
    assert turn.customer_text == "我不知道怎么办"
    assert turn.customer_committed_at is not None
    assert turn.customer_metadata()["lifecycle_state"] == "customer_committed"


def test_completed_playback_reports_full_audible_text():
    tracker = TurnLifecycleTracker("call-3")
    turn = tracker.commit_customer("请继续")
    turn.mark_ai_generating()
    turn.mark_ai_generated("好的，我继续为您说明。")
    turn.mark_ai_playing("stream-1", started_at=10.0)

    audible = turn.mark_completed(completed_at=12.0)

    assert turn.state is TurnLifecycleState.COMPLETED
    assert audible == "好的，我继续为您说明。"
    assert turn.assistant_metadata()["audible_text_complete"] is True
    assert turn.assistant_metadata()["playback_id"] == "stream-1"


def test_interrupted_stream_reports_only_estimated_audible_prefix():
    tracker = TurnLifecycleTracker("call-4")
    turn = tracker.commit_customer("先停一下")
    turn.mark_ai_generating()
    turn.mark_ai_generated("第一句已经播放。第二句还没有播放。")
    turn.mark_ai_playing("stream-2", started_at=20.0)

    audible = turn.mark_interrupted(
        reason="confirmed_customer_transcript",
        interrupted_at=21.0,
        emitted_audio_bytes=4000,
        total_audio_bytes=8000,
    )

    assert turn.state is TurnLifecycleState.INTERRUPTED
    assert audible
    assert audible != turn.ai_generated_text
    metadata = turn.assistant_metadata()
    assert metadata["lifecycle_state"] == "interrupted"
    assert metadata["interruption_reason"] == "confirmed_customer_transcript"
    assert metadata["audible_text_complete"] is False


def test_failed_turn_does_not_claim_assistant_audio_was_heard():
    tracker = TurnLifecycleTracker("call-5")
    turn = tracker.commit_customer("还有人在吗")
    turn.mark_ai_generating()

    turn.mark_failed("llm_failed", failed_at=30.0)

    assert turn.state is TurnLifecycleState.FAILED
    assert turn.audible_text == ""
    with pytest.raises(ValueError):
        turn.assistant_metadata()
