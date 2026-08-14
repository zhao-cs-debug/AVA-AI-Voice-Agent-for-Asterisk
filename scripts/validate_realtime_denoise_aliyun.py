#!/usr/bin/env python3
"""Replay PCM16 WAV files through the configured Aliyun realtime ASR backend."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import wave
from pathlib import Path


def _load_backend_types():
    local_server_dir = Path(__file__).resolve().parents[1] / "local_ai_server"
    sys.path.insert(0, str(local_server_dir))
    from audio_processor import AudioProcessor
    from stt_backends import AliyunQwenASRBackend

    return AudioProcessor, AliyunQwenASRBackend


def _backend_config() -> dict[str, object]:
    model = os.getenv("ALIYUN_ASR_MODEL", "qwen3-asr-flash-realtime")
    return {
        "provider": "aliyun_model_studio",
        "backend": "aliyun_qwen",
        "model": model,
        "language": os.getenv("ALIYUN_ASR_LANGUAGE", "zh"),
        "sample_rate": int(os.getenv("ALIYUN_ASR_SAMPLE_RATE", "8000")),
        "audio_format": os.getenv("ALIYUN_ASR_FORMAT", "pcm"),
        "turn_detection": os.getenv("ALIYUN_ASR_TURN_DETECTION", "server_vad"),
        "silence_duration_ms": int(
            os.getenv("ALIYUN_ASR_SILENCE_DURATION_MS", "400")
        ),
        "url": os.getenv("ALIYUN_ASR_URL")
        or f"wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model={model}",
    }


def _read_pcm16(path: Path) -> tuple[bytes, int]:
    with wave.open(str(path), "rb") as wav_file:
        if wav_file.getnchannels() != 1 or wav_file.getsampwidth() != 2:
            raise ValueError(f"{path}: expected mono PCM16 WAV")
        sample_rate = wav_file.getframerate()
        if sample_rate != 16000:
            raise ValueError(f"{path}: expected 16 kHz audio, received {sample_rate}")
        return wav_file.readframes(wav_file.getnframes()), sample_rate


async def transcribe(path: Path, *, frame_ms: int, tail_silence_ms: int) -> dict:
    AudioProcessor, AliyunQwenASRBackend = _load_backend_types()
    config = _backend_config()
    api_key = os.getenv("ALIYUN_ASR_API_KEY") or os.getenv("DASHSCOPE_API_KEY", "")
    if not api_key:
        raise RuntimeError("ALIYUN_ASR_API_KEY or DASHSCOPE_API_KEY is required")

    backend = AliyunQwenASRBackend(
        config["url"],
        api_key,
        model=config["model"],
        language=config["language"],
        sample_rate=config["sample_rate"],
        audio_format=config["audio_format"],
        turn_detection=config["turn_detection"],
        silence_duration_ms=config["silence_duration_ms"],
    )
    pcm16, input_rate = _read_pcm16(path)
    websocket = await backend.connect()
    events: list[dict] = []
    started = time.monotonic()
    receiver_done = asyncio.Event()

    async def receive_events() -> None:
        while not receiver_done.is_set():
            batch = await backend.receive_events(websocket, timeout=0.08, max_events=16)
            observed_at_ms = round((time.monotonic() - started) * 1000.0, 1)
            events.extend(
                {"observed_at_ms": observed_at_ms, **event} for event in batch
            )
            if not batch:
                await asyncio.sleep(0.01)

    receiver = asyncio.create_task(receive_events())
    frame_bytes = input_rate * 2 * frame_ms // 1000
    try:
        for offset in range(0, len(pcm16), frame_bytes):
            frame = pcm16[offset : offset + frame_bytes]
            asr_frame = AudioProcessor.resample_audio(
                frame, input_rate, config["sample_rate"]
            )
            await backend.send_audio(websocket, asr_frame)
            await asyncio.sleep(frame_ms / 1000.0)

        silence_frame = bytes(frame_bytes)
        for _ in range(max(1, tail_silence_ms // frame_ms)):
            asr_frame = AudioProcessor.resample_audio(
                silence_frame, input_rate, config["sample_rate"]
            )
            await backend.send_audio(websocket, asr_frame)
            await asyncio.sleep(frame_ms / 1000.0)
        await asyncio.sleep(2.0)
    finally:
        receiver_done.set()
        await receiver
        await backend.close(websocket)

    finals = [event for event in events if event.get("is_final")]
    report_config = {key: value for key, value in config.items() if key != "url"}
    return {
        "audio": {
            "path": str(path),
            "input_sample_rate": input_rate,
            "duration_sec": round(len(pcm16) / float(input_rate * 2), 6),
            "frame_ms": frame_ms,
        },
        "asr": report_config,
        "event_count": len(events),
        "partial_count": sum(bool(event.get("is_partial")) for event in events),
        "final_count": len(finals),
        "finals": finals,
        "events": events,
    }


async def async_main(args: argparse.Namespace) -> int:
    for path in args.wav:
        report = await transcribe(
            path,
            frame_ms=args.frame_ms,
            tail_silence_ms=args.tail_silence_ms,
        )
        output = path.with_name(f"{path.stem}-aliyun-asr.json")
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(
            json.dumps(
                {
                    "audio": path.name,
                    "report": output.name,
                    "asr": report["asr"],
                    "final_count": report["final_count"],
                    "texts": [event.get("text") for event in report["finals"]],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wav", nargs="+", type=Path)
    parser.add_argument("--frame-ms", type=int, default=160)
    parser.add_argument("--tail-silence-ms", type=int, default=1920)
    args = parser.parse_args()
    if args.frame_ms <= 0 or args.tail_silence_ms < 0:
        parser.error("frame and tail silence durations must be non-negative")
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
