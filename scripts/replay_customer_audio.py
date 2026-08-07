#!/usr/bin/env python3
"""Replay caller-only WAV audio through the configured 2.0 STT settlement path."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import LocalProviderConfig, load_config
from src.engine import Engine
from src.logging_config import configure_logging
from src.pipelines.local import LocalSTTAdapter


@dataclass(frozen=True)
class AudioClip:
    pcm16: bytes
    sample_rate: int
    start_sec: float
    end_sec: float

    @property
    def duration_sec(self) -> float:
        return len(self.pcm16) / float(self.sample_rate * 2)


def read_audio_clip(
    path: Path,
    *,
    start_sec: float = 0.0,
    end_sec: Optional[float] = None,
) -> AudioClip:
    with wave.open(str(path), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        total_frames = wav_file.getnframes()
        if channels != 1 or sample_width != 2:
            raise ValueError("WAV must be mono PCM16 audio")
        if sample_rate not in {8000, 16000}:
            raise ValueError("WAV sample rate must be 8000 or 16000 Hz")

        start_frame = min(total_frames, max(0, int(start_sec * sample_rate)))
        requested_end = total_frames if end_sec is None else int(end_sec * sample_rate)
        end_frame = min(total_frames, max(start_frame, requested_end))
        wav_file.setpos(start_frame)
        pcm16 = wav_file.readframes(end_frame - start_frame)

    if not pcm16:
        raise ValueError("Selected WAV segment is empty")
    return AudioClip(
        pcm16=pcm16,
        sample_rate=sample_rate,
        start_sec=start_frame / float(sample_rate),
        end_sec=end_frame / float(sample_rate),
    )


def _local_provider_config(
    app_config,
    component: str = "local_stt",
) -> LocalProviderConfig:
    providers = getattr(app_config, "providers", {}) or {}
    raw_provider = providers.get(component)
    if raw_provider is None and component != "local":
        raw_provider = providers.get("local")
    if isinstance(raw_provider, LocalProviderConfig):
        return raw_provider
    if not isinstance(raw_provider, dict):
        raise RuntimeError("Configured 2.0 pipeline has no local STT provider")
    return LocalProviderConfig(
        **{key: value for key, value in raw_provider.items() if value is not None}
    )


def _pipeline_stt_options(app_config, pipeline_name: str) -> tuple[str, Dict[str, Any]]:
    pipeline = (getattr(app_config, "pipelines", {}) or {}).get(pipeline_name)
    if pipeline is None:
        raise RuntimeError(f"Pipeline '{pipeline_name}' does not exist")
    component = str(getattr(pipeline, "stt", "") or "")
    if component != "local_stt":
        raise RuntimeError(
            f"Pipeline '{pipeline_name}' uses '{component}', not the 2.0 local_stt adapter"
        )
    options = dict((getattr(pipeline, "options", {}) or {}).get("stt") or {})
    options.update({"mode": "stt", "streaming": True})
    return component, options


async def replay_audio(
    clip: AudioClip,
    *,
    config_path: str,
    pipeline_name: str,
    settle_seconds: float,
    frame_ms: int,
    tail_silence_seconds: float,
    result_wait_seconds: float,
) -> Dict[str, Any]:
    app_config = load_config(config_path, merge_external_contexts=False)
    component, stt_options = _pipeline_stt_options(app_config, pipeline_name)
    provider_config = _local_provider_config(app_config, component)
    adapter = LocalSTTAdapter(component, app_config, provider_config, stt_options)
    call_id = f"replay-{uuid.uuid4()}"
    stream_format = f"pcm16_{clip.sample_rate // 1000}k"
    event_queue: asyncio.Queue = asyncio.Queue()
    raw_events: List[Dict[str, Any]] = []
    committed_turns: List[Dict[str, Any]] = []
    started_at = time.monotonic()

    engine = Engine.__new__(Engine)
    engine._pipeline_barge_in_candidates = {}
    engine._pipeline_terminating_calls = set()

    async def ignore_barge_in(_call_id: str, _text: str) -> bool:
        return False

    engine._confirm_pipeline_barge_in_candidate = ignore_barge_in

    async def collect_asr_events() -> None:
        try:
            async for event in adapter.iter_events(call_id):
                received_ms = round((time.monotonic() - started_at) * 1000.0, 1)
                normalized = {
                    "text": str(event.get("text") or ""),
                    "is_partial": bool(event.get("is_partial", False)),
                    "is_final": bool(event.get("is_final", False)),
                }
                for field in (
                    "event_id",
                    "item_id",
                    "audio_start_ms",
                    "audio_end_ms",
                    "audio_duration_ms",
                    "received_at_ms",
                    "raw_event_type",
                    "source_activity",
                ):
                    if event.get(field) is not None:
                        normalized[field] = event[field]
                raw_events.append({"at_ms": received_ms, **normalized})
                await event_queue.put(normalized)
        finally:
            await event_queue.put(None)

    async def collect_committed_turns() -> None:
        async for text in engine._iter_settled_pipeline_transcripts(
            call_id,
            event_queue,
            settle_seconds=settle_seconds,
        ):
            committed_turns.append(
                {
                    "at_ms": round((time.monotonic() - started_at) * 1000.0, 1),
                    "text": text,
                }
            )

    await adapter.start()
    await adapter.open_call(call_id, stt_options)
    await adapter.start_stream(call_id, stt_options)
    event_task = asyncio.create_task(collect_asr_events())
    settlement_task = asyncio.create_task(collect_committed_turns())

    try:
        bytes_per_frame = max(2, int(clip.sample_rate * 2 * frame_ms / 1000))
        frame_seconds = frame_ms / 1000.0
        for offset in range(0, len(clip.pcm16), bytes_per_frame):
            await adapter.send_audio(
                call_id,
                clip.pcm16[offset : offset + bytes_per_frame],
                fmt=stream_format,
            )
            await asyncio.sleep(frame_seconds)

        silence_frames = max(0, int(tail_silence_seconds / frame_seconds))
        silence = b"\x00" * bytes_per_frame
        for _ in range(silence_frames):
            await adapter.send_audio(call_id, silence, fmt=stream_format)
            await asyncio.sleep(frame_seconds)
        await asyncio.sleep(max(0.0, result_wait_seconds))
    finally:
        await adapter.stop_stream(call_id)
        await asyncio.wait_for(event_task, timeout=5.0)
        await asyncio.wait_for(settlement_task, timeout=max(5.0, settle_seconds + 2.0))
        await adapter.close_call(call_id)
        await adapter.stop()

    finals = [event for event in raw_events if event["is_final"]]
    final_intervals_ms = [
        round(current["at_ms"] - previous["at_ms"], 1)
        for previous, current in zip(finals, finals[1:])
    ]
    return {
        "call_id": call_id,
        "pipeline": pipeline_name,
        "stt_component": component,
        "audio": {
            "sample_rate": clip.sample_rate,
            "start_sec": round(clip.start_sec, 3),
            "end_sec": round(clip.end_sec, 3),
            "duration_sec": round(clip.duration_sec, 3),
        },
        "settle_ms": round(settle_seconds * 1000.0),
        "raw_events": raw_events,
        "partial_event_count": sum(1 for event in raw_events if event["is_partial"]),
        "raw_final_count": len(finals),
        "final_intervals_ms": final_intervals_ms,
        "committed_turns": committed_turns,
        "expected_llm_calls": len(committed_turns),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wav", type=Path, help="Caller-only mono PCM16 WAV")
    parser.add_argument("--config", default="config/ai-agent.yaml")
    parser.add_argument("--pipeline", default="local_hybrid")
    parser.add_argument("--start-sec", type=float, default=0.0)
    parser.add_argument("--end-sec", type=float)
    parser.add_argument("--settle-ms", type=int, default=400)
    parser.add_argument("--frame-ms", type=int, default=160)
    parser.add_argument("--tail-silence-sec", type=float, default=1.5)
    parser.add_argument("--result-wait-sec", type=float, default=2.0)
    parser.add_argument(
        "--expected-turns",
        type=int,
        help="Exit non-zero unless this many customer turns reach the LLM boundary",
    )
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--verbose", action="store_true", help="Show adapter debug logs")
    return parser


async def async_main(args: argparse.Namespace) -> int:
    log_level = "INFO" if args.verbose else "WARNING"
    os.environ["LOG_LEVEL"] = log_level
    configure_logging(log_level, service_name="customer-audio-replay")
    if args.start_sec < 0 or (args.end_sec is not None and args.end_sec <= args.start_sec):
        raise ValueError("Audio time range is invalid")
    if args.frame_ms <= 0 or args.settle_ms < 0:
        raise ValueError("Frame and settlement durations must be non-negative")

    clip = read_audio_clip(
        args.wav,
        start_sec=args.start_sec,
        end_sec=args.end_sec,
    )
    report = await replay_audio(
        clip,
        config_path=args.config,
        pipeline_name=args.pipeline,
        settle_seconds=args.settle_ms / 1000.0,
        frame_ms=args.frame_ms,
        tail_silence_seconds=args.tail_silence_sec,
        result_wait_seconds=args.result_wait_sec,
    )
    if args.expected_turns is not None:
        report["expected_turns"] = args.expected_turns
        report["verdict"] = (
            "pass"
            if report["expected_llm_calls"] == args.expected_turns
            else "fail"
        )
    else:
        report["verdict"] = "observed"

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output_json:
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    return 1 if report["verdict"] == "fail" else 0


def main() -> int:
    try:
        return asyncio.run(async_main(build_parser().parse_args()))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"replay failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
