"""Streaming VoxCPM2 TTS through vLLM-Omni's OpenAI-compatible API."""

from __future__ import annotations

import asyncio
import base64
import time
import uuid
from typing import Any, AsyncIterator, Callable, Dict, Optional
from urllib.parse import urlparse

import aiohttp

from ..audio import convert_pcm16le_to_target_format, resample_audio
from ..config import AppConfig, VllmOmniTTSProviderConfig
from ..logging_config import get_logger
from .base import TTSComponent

logger = get_logger(__name__)


def _bytes_per_sample(encoding: str) -> int:
    return 1 if str(encoding or "").lower() in {"ulaw", "mulaw", "mu-law"} else 2


class VllmOmniTTSAdapter(TTSComponent):
    """Yield telephony-ready frames while vLLM-Omni is still synthesizing."""

    downstream_mode_override = "stream"

    def __init__(
        self,
        component_key: str,
        app_config: AppConfig,
        provider_config: VllmOmniTTSProviderConfig,
        options: Optional[Dict[str, Any]] = None,
        *,
        session_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
    ) -> None:
        self.component_key = component_key
        self._app_config = app_config
        self._provider_defaults = provider_config
        self._pipeline_defaults = options or {}
        self._session_factory = session_factory
        self._session: Optional[aiohttp.ClientSession] = None
        self._active_responses: Dict[str, aiohttp.ClientResponse] = {}
        self._reference_cache: Dict[str, tuple[float, str, str]] = {}

    async def start(self) -> None:
        logger.info(
            "vLLM-Omni TTS adapter initialized",
            component=self.component_key,
            model=self._provider_defaults.tts_model,
        )

    async def stop(self) -> None:
        for response in list(self._active_responses.values()):
            response.close()
        self._active_responses.clear()
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def open_call(self, call_id: str, options: Dict[str, Any]) -> None:
        await self._ensure_session()

    async def close_call(self, call_id: str) -> None:
        response = self._active_responses.pop(call_id, None)
        if response is not None:
            response.close()

    async def validate_connectivity(self, options: Dict[str, Any]) -> Dict[str, Any]:
        merged = self._compose_options(options)
        parsed = urlparse(str(merged.get("tts_base_url") or ""))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return {
                "healthy": False,
                "error": "Invalid vLLM-Omni TTS URL",
                "details": {"endpoint": merged.get("tts_base_url")},
            }

        await self._ensure_session()
        assert self._session is not None
        health_url = f"{parsed.scheme}://{parsed.netloc}/health"
        headers = {}
        if merged.get("api_key"):
            headers["Authorization"] = f"Bearer {merged['api_key']}"
        timeout = aiohttp.ClientTimeout(
            total=float(merged["connect_timeout_sec"]),
            connect=float(merged["connect_timeout_sec"]),
        )
        try:
            async with self._session.get(
                health_url,
                headers=headers,
                timeout=timeout,
            ) as response:
                body = (await response.read()).decode("utf-8", errors="ignore")[:256]
                if response.status >= 400:
                    return {
                        "healthy": False,
                        "error": f"vLLM-Omni health check failed: HTTP {response.status}",
                        "details": {"endpoint": health_url, "response": body},
                    }
                return {
                    "healthy": True,
                    "error": None,
                    "details": {"endpoint": health_url, "status": response.status},
                }
        except Exception as exc:
            return {
                "healthy": False,
                "error": f"vLLM-Omni health check failed: {exc}",
                "details": {"endpoint": health_url},
            }

    async def synthesize(
        self,
        call_id: str,
        text: str,
        options: Dict[str, Any],
    ) -> AsyncIterator[bytes]:
        if not text:
            return
            yield

        await self._ensure_session()
        assert self._session is not None
        merged = self._compose_options(options)
        ref_audio, ref_text = await self._resolve_reference(call_id, merged)

        payload: Dict[str, Any] = {
            "model": merged["model"],
            "input": text,
            "voice": merged["voice"],
            "response_format": "pcm",
            "stream": True,
            "stream_format": "audio",
        }
        if ref_audio:
            payload["ref_audio"] = ref_audio
        if ref_text:
            payload["ref_text"] = ref_text

        headers = {"Content-Type": "application/json", "User-Agent": "Asterisk-AI-Voice-Agent/1.0"}
        if merged.get("api_key"):
            headers["Authorization"] = f"Bearer {merged['api_key']}"

        request_id = f"vllm-omni-{uuid.uuid4().hex[:12]}"
        started_at = time.perf_counter()
        first_audio_at: Optional[float] = None
        output_bytes = 0
        timeout = aiohttp.ClientTimeout(
            total=float(merged["request_timeout_sec"]),
            connect=float(merged["connect_timeout_sec"]),
        )
        logger.info(
            "vLLM-Omni TTS synthesis started",
            call_id=call_id,
            request_id=request_id,
            model=payload["model"],
            has_reference=bool(ref_audio),
            text_preview=text[:64],
        )

        try:
            async with self._session.post(
                merged["tts_base_url"],
                json=payload,
                headers=headers,
                timeout=timeout,
            ) as response:
                self._active_responses[call_id] = response
                if response.status >= 400:
                    body = (await response.read()).decode("utf-8", errors="ignore")[:256]
                    raise RuntimeError(
                        f"vLLM-Omni TTS request failed: HTTP {response.status}: {body}"
                    )

                source_rate = int(merged["source_sample_rate_hz"])
                target_rate = int(merged["format"]["sample_rate"])
                target_encoding = str(merged["format"]["encoding"])
                chunk_size = max(
                    _bytes_per_sample(target_encoding),
                    int(target_rate * float(merged["chunk_size_ms"]) / 1000.0)
                    * _bytes_per_sample(target_encoding),
                )
                pending_source = b""
                pending_output = bytearray()
                resample_state = None

                async for raw_chunk in response.content.iter_any():
                    if not raw_chunk:
                        continue
                    if first_audio_at is None:
                        first_audio_at = time.perf_counter()
                    source_chunk = pending_source + bytes(raw_chunk)
                    if len(source_chunk) % 2:
                        pending_source = source_chunk[-1:]
                        source_chunk = source_chunk[:-1]
                    else:
                        pending_source = b""
                    if not source_chunk:
                        continue
                    if source_rate != target_rate:
                        source_chunk, resample_state = resample_audio(
                            source_chunk,
                            source_rate,
                            target_rate,
                            state=resample_state,
                        )
                    converted = convert_pcm16le_to_target_format(source_chunk, target_encoding)
                    pending_output.extend(converted)
                    while len(pending_output) >= chunk_size:
                        frame = bytes(pending_output[:chunk_size])
                        del pending_output[:chunk_size]
                        output_bytes += len(frame)
                        yield frame

                if pending_source:
                    logger.warning(
                        "vLLM-Omni TTS stream ended with an incomplete PCM sample",
                        call_id=call_id,
                        request_id=request_id,
                    )
                if pending_output:
                    frame = bytes(pending_output)
                    output_bytes += len(frame)
                    yield frame
        except asyncio.CancelledError:
            logger.info(
                "vLLM-Omni TTS synthesis cancelled",
                call_id=call_id,
                request_id=request_id,
            )
            raise
        finally:
            self._active_responses.pop(call_id, None)

        logger.info(
            "vLLM-Omni TTS synthesis completed",
            call_id=call_id,
            request_id=request_id,
            first_audio_ms=(
                round((first_audio_at - started_at) * 1000.0, 2)
                if first_audio_at is not None
                else None
            ),
            total_ms=round((time.perf_counter() - started_at) * 1000.0, 2),
            output_bytes=output_bytes,
        )

    async def _resolve_reference(self, call_id: str, merged: Dict[str, Any]) -> tuple[str, str]:
        ref_audio = str(merged.get("ref_audio") or "").strip()
        ref_text = str(merged.get("ref_text") or "").strip()
        if ref_audio:
            return ref_audio, ref_text

        voice_config = merged.get("default_voice")
        voice_id = str(voice_config.get("voice_id") or "").strip() if isinstance(voice_config, dict) else ""
        if not voice_id:
            return "", ref_text

        now = time.monotonic()
        cached = self._reference_cache.get(voice_id)
        if cached and now - cached[0] <= float(merged["reference_cache_ttl_sec"]):
            return cached[1], cached[2]

        reference_base_url = str(merged.get("reference_base_url") or "").rstrip("/")
        if not reference_base_url:
            raise RuntimeError(
                f"vLLM-Omni voice '{voice_id}' requires reference_base_url"
            )
        assert self._session is not None
        url = (
            f"{reference_base_url}/api/v1/outbound-ai/voice-library/"
            f"{voice_id}/runtime-reference"
        )
        headers = {}
        if merged.get("reference_auth_token"):
            headers["X-AI-Engine-Token"] = str(merged["reference_auth_token"])
        timeout = aiohttp.ClientTimeout(total=float(merged["reference_timeout_sec"]))
        async with self._session.get(url, headers=headers, timeout=timeout) as response:
            if response.status >= 400:
                body = (await response.read()).decode("utf-8", errors="ignore")[:256]
                raise RuntimeError(
                    f"vLLM-Omni reference lookup failed for '{voice_id}': "
                    f"HTTP {response.status}: {body}"
                )
            data = await response.json()
        resolved_audio = str(data.get("ref_audio") or "").strip()
        resolved_text = str(data.get("ref_text") or "").strip()
        if not resolved_audio or not resolved_text:
            raise RuntimeError(
                f"vLLM-Omni reference lookup for '{voice_id}' returned incomplete data"
            )
        self._reference_cache[voice_id] = (now, resolved_audio, resolved_text)
        logger.info(
            "vLLM-Omni voice reference resolved",
            call_id=call_id,
            voice_id=voice_id,
        )
        return resolved_audio, resolved_text

    async def _ensure_session(self) -> None:
        if self._session and not self._session.closed:
            return
        factory = self._session_factory or aiohttp.ClientSession
        self._session = factory()

    def _compose_options(self, runtime_options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        runtime_options = runtime_options or {}
        defaults = self._pipeline_defaults
        provider = self._provider_defaults
        runtime_format = runtime_options.get("format") or {}
        default_format = defaults.get("format") or {}
        return {
            "api_key": runtime_options.get("api_key", defaults.get("api_key", provider.api_key)),
            "tts_base_url": runtime_options.get(
                "tts_base_url",
                runtime_options.get("base_url", defaults.get("tts_base_url", provider.tts_base_url)),
            ),
            "model": runtime_options.get(
                "model",
                runtime_options.get("tts_model", defaults.get("model", defaults.get("tts_model", provider.tts_model))),
            ),
            "voice": runtime_options.get("voice", defaults.get("voice", provider.voice)),
            "source_sample_rate_hz": int(
                runtime_options.get(
                    "source_sample_rate_hz",
                    defaults.get("source_sample_rate_hz", provider.source_sample_rate_hz),
                )
            ),
            "chunk_size_ms": int(runtime_options.get("chunk_size_ms", defaults.get("chunk_size_ms", provider.chunk_size_ms))),
            "connect_timeout_sec": float(runtime_options.get("connect_timeout_sec", defaults.get("connect_timeout_sec", provider.connect_timeout_sec))),
            "request_timeout_sec": float(runtime_options.get("request_timeout_sec", defaults.get("request_timeout_sec", provider.request_timeout_sec))),
            "reference_base_url": runtime_options.get("reference_base_url", defaults.get("reference_base_url", provider.reference_base_url)),
            "reference_auth_token": runtime_options.get("reference_auth_token", defaults.get("reference_auth_token", provider.reference_auth_token)),
            "reference_timeout_sec": float(runtime_options.get("reference_timeout_sec", defaults.get("reference_timeout_sec", provider.reference_timeout_sec))),
            "reference_cache_ttl_sec": float(runtime_options.get("reference_cache_ttl_sec", defaults.get("reference_cache_ttl_sec", provider.reference_cache_ttl_sec))),
            "ref_audio": runtime_options.get("ref_audio", defaults.get("ref_audio")),
            "ref_text": runtime_options.get("ref_text", defaults.get("ref_text")),
            "default_voice": runtime_options.get("default_voice", defaults.get("default_voice")),
            "format": {
                "encoding": runtime_format.get("encoding", default_format.get("encoding", provider.target_encoding)),
                "sample_rate": int(runtime_format.get("sample_rate", default_format.get("sample_rate", provider.target_sample_rate_hz))),
            },
        }


__all__ = ["VllmOmniTTSAdapter"]
