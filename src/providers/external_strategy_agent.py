from __future__ import annotations

import asyncio
import audioop
import base64
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote, urljoin, urlparse

import aiohttp

from ..audio.resampler import resample_audio
from .base import AIProviderInterface, ProviderCapabilities, ProviderCapabilitiesMixin
from .external_strategy_config import ExternalStrategyAgentConfig


logger = logging.getLogger(__name__)


MAX_INPUT_FRAME_BYTES = 256 * 1024


class ExternalStrategyProtocolError(RuntimeError):
    pass


class ExternalStrategyAgentProvider(AIProviderInterface, ProviderCapabilitiesMixin):
    """Full voice provider for the public external strategy-agent protocol."""

    def __init__(
        self,
        config: ExternalStrategyAgentConfig,
        on_event: Callable[[Dict[str, Any]], Any],
    ) -> None:
        super().__init__(on_event)
        self.set_provider_identity(
            provider_key="external_strategy_agent",
            provider_kind="external_strategy_agent",
        )
        self.config = config
        self._http: Optional[aiohttp.ClientSession] = None
        self._ws: Any = None
        self._receive_task: Optional[asyncio.Task] = None
        self._startup: Optional[asyncio.Future] = None
        self._ended = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._call_id: Optional[str] = None
        self._session_id: Optional[str] = None
        self._connected = False
        self._closing = False
        self._remote_ended = False
        self._input_rate = int(config.provider_input_sample_rate_hz)
        self._output_rate = int(config.output_sample_rate_hz)
        self._chunk_ms = int(config.audio_chunk_ms)
        self._input_buffer = bytearray()
        self._resample_state_in = None
        self._seen_chunk_ids: set[str] = set()
        self._blocked_response_ids: set[str] = set()
        self._started_response_ids: set[str] = set()
        self._completed_response_ids: set[str] = set()
        self._response_sample_rates: dict[str, int] = {}
        self._played_text: dict[str, str] = {}
        self._playback_sequence = 0

    @property
    def supported_codecs(self) -> List[str]:
        return ["linear16", "pcm16", "slin", "slin16", "ulaw", "alaw"]

    def is_ready(self) -> bool:
        # The public protocol explicitly supports deployments with authentication
        # disabled. A missing key is therefore valid; authenticated services will
        # reject the first request with their normal 401 response.
        return bool(self.config.enabled and self.config.base_url.strip())

    def get_capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            input_encodings=["linear16", "pcm16", "slin", "slin16", "ulaw", "alaw"],
            input_sample_rates_hz=[8000, 16000, 24000, 32000, 48000],
            output_encodings=["linear16", "pcm16"],
            output_sample_rates_hz=[8000, 16000, 24000, 32000, 48000],
            preferred_chunk_ms=max(20, int(self.config.audio_chunk_ms)),
            can_negotiate=True,
            is_full_agent=True,
            has_native_vad=True,
            has_native_barge_in=True,
            requires_continuous_audio=True,
        )

    async def start_session(
        self,
        call_id: str,
        on_event: Optional[Callable] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.is_ready():
            raise ValueError("External strategy agent URL or API key is not configured")
        if on_event:
            self.on_event = on_event
        await self._reset_session(call_id)
        external = self._validated_context(context or {})
        audio = external["audio"]
        self._input_rate = int(audio["input_sample_rate"])
        self._output_rate = int(audio["output_sample_rate"])
        self._chunk_ms = int(audio["chunk_ms"])

        try:
            created = await self._request_json(
                "POST",
                "/api/v1/external/sessions",
                payload={
                    "mode": "voice",
                    "audio": {
                        "input": {
                            "encoding": "pcm_s16le",
                            "sample_rate": self._input_rate,
                            "channels": 1,
                        },
                        "output": {
                            "encoding": "pcm_s16le",
                            "sample_rate": self._output_rate,
                            "channels": 1,
                        },
                    },
                },
            )
            self._session_id = self._required_string(created, "session_id")
            settings_url = self._required_string(created, "settings_url")
            websocket_path = self._required_string(created, "websocket_path")

            confirmed = await self._request_json(
                "PUT",
                settings_url,
                payload=self._settings_payload(external, context or {}),
                timeout=self.config.settings_timeout_sec,
            )
            if confirmed.get("settings_confirmed") is not True:
                raise ExternalStrategyProtocolError("External strategy settings were not confirmed")
            confirmed_settings = confirmed.get("settings")
            if not isinstance(confirmed_settings, dict):
                raise ExternalStrategyProtocolError("Settings confirmation omitted the complete settings object")
            websocket_path = str(confirmed.get("websocket_path") or websocket_path).strip()

            catalog = await self._request_json(
                "GET",
                f"/api/tts/voices?conn_id={quote(self._session_id, safe='')}",
            )
            selection = self._select_voice(
                catalog,
                external.get("voice") or {},
                ai_gender=str((external.get("ai") or {}).get("gender") or ""),
            )
            if selection:
                saved_settings = dict(confirmed_settings)
                saved_settings[selection["value_field"]] = selection["value"]
                saved = await self._request_json(
                    "POST",
                    "/api/settings/save",
                    payload={"conn_id": self._session_id, "settings": saved_settings},
                    timeout=self.config.settings_timeout_sec,
                )
                persisted = saved.get("settings")
                if not isinstance(persisted, dict) or str(
                    persisted.get(selection["value_field"]) or ""
                ) != selection["value"]:
                    raise ExternalStrategyProtocolError("External strategy service did not persist the selected voice")

            websocket_url = self._websocket_url(websocket_path)
            self._ws = await self._connect_websocket(websocket_url)
            self._connected = True
            self._receive_task = asyncio.create_task(
                self._receive_loop(),
                name=f"external-strategy-recv-{call_id}",
            )
            await asyncio.wait_for(
                asyncio.shield(self._startup),
                timeout=float(self.config.session_start_timeout_sec),
            )
            await self.on_event({
                "type": "session_started",
                "call_id": call_id,
                "provider": self.provider_event_name(),
                "external_session_id": self._session_id,
            })
        except Exception:
            self._closing = True
            await self._close_resources(send_end=False)
            raise

    async def send_audio(
        self,
        audio_chunk: bytes,
        sample_rate: Optional[int] = None,
        encoding: Optional[str] = None,
    ) -> None:
        if not audio_chunk or not self._connected or self._closing or self._ws is None:
            return
        pcm = self._to_pcm16(audio_chunk, encoding or self.config.input_encoding)
        source_rate = int(sample_rate or self.config.input_sample_rate_hz)
        if source_rate != self._input_rate:
            pcm, self._resample_state_in = resample_audio(
                pcm,
                source_rate,
                self._input_rate,
                state=self._resample_state_in,
            )
        if len(pcm) % 2:
            pcm = pcm[:-1]
        self._input_buffer.extend(pcm)
        frame_bytes = max(2, int(self._input_rate * 2 * self._chunk_ms / 1000))
        frame_bytes -= frame_bytes % 2
        while len(self._input_buffer) >= frame_bytes:
            frame = bytes(self._input_buffer[:frame_bytes])
            del self._input_buffer[:frame_bytes]
            async with self._send_lock:
                await self._ws.send_bytes(frame)

    async def stop_session(self) -> None:
        if self._closing:
            return
        self._closing = True
        try:
            if self._connected and self._ws is not None:
                if self._input_buffer:
                    final_frame = bytes(self._input_buffer[: len(self._input_buffer) // 2 * 2])
                    self._input_buffer.clear()
                    if final_frame:
                        async with self._send_lock:
                            await self._ws.send_bytes(final_frame)
                await self._send_json({"type": "session.end"})
                try:
                    await asyncio.wait_for(
                        self._ended.wait(),
                        timeout=max(0.01, float(self.config.close_timeout_sec)),
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            await self._close_resources(send_end=False)
            try:
                await self.on_event({
                    "type": "session_ended",
                    "call_id": self._call_id,
                    "provider": self.provider_event_name(),
                    "external_session_id": self._session_id,
                })
            except Exception:
                logger.debug("Failed to emit external strategy session_ended", exc_info=True)

    async def _reset_session(self, call_id: str) -> None:
        if self._connected or self._http is not None or self._ws is not None:
            await self._close_resources(send_end=False)
        self._call_id = call_id
        self._session_id = None
        self._closing = False
        self._connected = False
        self._remote_ended = False
        self._ended = asyncio.Event()
        self._startup = asyncio.get_running_loop().create_future()
        self._input_buffer.clear()
        self._resample_state_in = None
        self._seen_chunk_ids.clear()
        self._blocked_response_ids.clear()
        self._started_response_ids.clear()
        self._completed_response_ids.clear()
        self._response_sample_rates.clear()
        self._played_text.clear()
        self._playback_sequence = 0

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        session = await self._http_session()
        url = self._http_url(path)
        try:
            async with session.request(
                method,
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout or self.config.request_timeout_sec),
                allow_redirects=False,
            ) as response:
                raw = await response.text()
                if response.status >= 400:
                    raise ExternalStrategyProtocolError(
                        f"External strategy HTTP {response.status}: {self._public_error(raw)}"
                    )
        except asyncio.TimeoutError as exc:
            raise ExternalStrategyProtocolError("External strategy request timed out") from exc
        except aiohttp.ClientError as exc:
            raise ExternalStrategyProtocolError("External strategy connection failed") from exc
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExternalStrategyProtocolError("External strategy response was not JSON") from exc
        if not isinstance(decoded, dict):
            raise ExternalStrategyProtocolError("External strategy response must be an object")
        return decoded

    async def _http_session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            headers = {"Content-Type": "application/json"}
            if self.config.api_key.strip():
                headers["Authorization"] = f"Bearer {self.config.api_key.strip()}"
            self._http = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.config.request_timeout_sec),
            )
        return self._http

    async def _connect_websocket(self, url: str) -> Any:
        session = await self._http_session()
        try:
            return await session.ws_connect(
                url,
                heartbeat=20,
                max_msg_size=max(1024, int(self.config.max_message_bytes)),
                timeout=float(self.config.connect_timeout_sec),
                receive_timeout=None,
            )
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            raise ExternalStrategyProtocolError("External strategy WebSocket connection failed") from exc

    async def _receive_loop(self) -> None:
        disconnect_reason = "websocket_closed"
        try:
            async for message in self._ws:
                if self._closing:
                    break
                if isinstance(message, dict):
                    event = message
                elif getattr(message, "type", None) == aiohttp.WSMsgType.TEXT:
                    try:
                        event = json.loads(message.data)
                    except json.JSONDecodeError:
                        raise ExternalStrategyProtocolError("External strategy WebSocket returned invalid JSON")
                elif getattr(message, "type", None) in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                }:
                    disconnect_reason = "websocket_closed"
                    break
                elif getattr(message, "type", None) == aiohttp.WSMsgType.ERROR:
                    disconnect_reason = "websocket_error"
                    break
                else:
                    raise ExternalStrategyProtocolError("External strategy WebSocket returned an unexpected binary frame")
                if not isinstance(event, dict):
                    raise ExternalStrategyProtocolError("External strategy WebSocket event must be an object")
                await self._handle_event(event)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            disconnect_reason = type(exc).__name__
            if self._startup is not None and not self._startup.done():
                self._startup.set_exception(exc)
            logger.error("External strategy receive loop failed", exc_info=True)
        finally:
            self._connected = False
            self._ended.set()
            if self._startup is not None and not self._startup.done():
                self._startup.set_exception(
                    ExternalStrategyProtocolError(
                        "External strategy WebSocket closed before session.started"
                    )
                )
            if not self._closing and not self._remote_ended:
                await self.on_event({
                    "type": "ProviderDisconnected",
                    "call_id": self._call_id,
                    "provider": self.provider_event_name(),
                    "reason": disconnect_reason,
                })

    async def _handle_event(self, event: Dict[str, Any]) -> None:
        event_type = str(event.get("type") or "")
        if event_type == "session.ready":
            self._apply_ready_audio_contract(event)
            await self._send_json({
                "type": "session.start",
                "input_audio_sample_rate": self._input_rate,
            })
        elif event_type == "session.started":
            if self._startup is not None and not self._startup.done():
                self._startup.set_result(None)
        elif event_type == "input.transcript":
            if event.get("final") is True:
                text = str(event.get("text") or "").strip()
                if text:
                    await self.on_event({
                        "type": "transcript",
                        "call_id": self._call_id,
                        "text": text,
                        "role": "user",
                        "is_final": True,
                        "event_id": self._event_id("input", event),
                        "turn_id": event.get("log_id") or event.get("sequence"),
                    })
        elif event_type == "input.accepted":
            logger.debug(
                "External strategy input accepted: call=%s turn=%s",
                self._call_id,
                event.get("turn_id"),
            )
        elif event_type in {"response[0]", "response[1]"}:
            text = str(event.get("text") or "").strip()
            response_id = str(event.get("response_id") or "").strip()
            if text:
                await self.on_event({
                    "type": "agent_transcript",
                    "call_id": self._call_id,
                    "text": text,
                    "role": "assistant",
                    "event_id": self._event_id(event_type, event),
                    "segment_id": self._segment_id(response_id or event_type),
                    "response_id": response_id,
                    "response_index": 0 if event_type == "response[0]" else 1,
                })
        elif event_type == "response.audio.delta":
            await self._handle_audio_delta(event)
        elif event_type == "response.audio.probe":
            await self._handle_probe(event)
        elif event_type == "response.audio.interrupted":
            await self._handle_interrupted(event)
        elif event_type == "ping":
            await self._send_json({
                "type": "pong",
                "session_id": self._session_id,
                "timestamp_ms": int(time.time() * 1000),
                "client_timestamp_ms": event.get("timestamp_ms"),
            })
        elif event_type == "session.ended":
            reason = str(event.get("reason") or "session_ended")
            self._remote_ended = True
            self._connected = False
            self._ended.set()
            if self._startup is not None and not self._startup.done():
                self._startup.set_exception(
                    ExternalStrategyProtocolError(
                        f"External strategy session ended before startup: {reason}"
                    )
                )
            if not self._closing:
                await self.on_event({
                    "type": "ProviderDisconnected",
                    "call_id": self._call_id,
                    "provider": self.provider_event_name(),
                    "reason": reason,
                })
            if self._ws is not None:
                await self._ws.close()
        elif event_type == "error":
            code = str(event.get("code") or "external_error")
            message = str(event.get("message") or "External strategy session failed")
            error = ExternalStrategyProtocolError(f"{code}: {message}")
            if self._startup is not None and not self._startup.done():
                self._startup.set_exception(error)
            else:
                raise error

    async def _handle_audio_delta(self, event: Dict[str, Any]) -> None:
        response_id = str(event.get("response_id") or "").strip()
        chunk_id = str(event.get("chunk_id") or "").strip()
        if not response_id or not chunk_id:
            raise ExternalStrategyProtocolError("Audio delta omitted response_id or chunk_id")
        if response_id in self._blocked_response_ids or chunk_id in self._seen_chunk_ids:
            return
        if response_id in self._completed_response_ids:
            raise ExternalStrategyProtocolError("Audio delta arrived after the response final marker")
        try:
            channels = int(event.get("channels", 1))
        except (TypeError, ValueError) as exc:
            raise ExternalStrategyProtocolError("Audio delta channels were invalid") from exc
        if channels != 1:
            raise ExternalStrategyProtocolError("Audio delta must be mono")
        content_type = str(event.get("content_type") or "").lower().strip()
        if content_type and "pcm_s16le" not in content_type:
            raise ExternalStrategyProtocolError("Audio delta was not PCM16 little-endian audio")
        encoded = str(event.get("audio") or "")
        try:
            audio = base64.b64decode(encoded, validate=True) if encoded else b""
        except ValueError as exc:
            raise ExternalStrategyProtocolError("Audio delta contained invalid base64") from exc
        if len(audio) % 2:
            raise ExternalStrategyProtocolError("Audio delta PCM16 payload had an odd byte length")
        if not audio and event.get("final") is not True:
            raise ExternalStrategyProtocolError("Only a final audio marker may have an empty payload")
        expected_rate = self._response_sample_rates.get(response_id)
        try:
            sample_rate = int(event.get("sample_rate") or expected_rate or self._output_rate)
        except (TypeError, ValueError) as exc:
            raise ExternalStrategyProtocolError("Audio delta sample rate was invalid") from exc
        if sample_rate not in {8000, 16000, 24000, 32000, 48000}:
            raise ExternalStrategyProtocolError("Audio delta used an unsupported sample rate")
        if expected_rate is not None and sample_rate != expected_rate:
            raise ExternalStrategyProtocolError("Audio delta sample rate changed within one response")
        segment_id = self._segment_id(response_id)
        if audio:
            accepted = await self.on_event({
                "type": "AgentAudio",
                "call_id": self._call_id,
                "data": audio,
                "encoding": "linear16",
                "sample_rate": sample_rate,
                "segment_id": segment_id,
                "response_id": response_id,
                "chunk_id": chunk_id,
            })
            if accepted is not True:
                logger.warning(
                    "External strategy audio was rejected by the local playback path",
                    extra={"call_id": self._call_id, "response_id": response_id, "chunk_id": chunk_id},
                )
                return
            self._started_response_ids.add(response_id)
        self._response_sample_rates.setdefault(response_id, sample_rate)
        self._seen_chunk_ids.add(chunk_id)
        chunk_text = str(event.get("text") or "")
        if chunk_text:
            self._played_text[response_id] = self._played_text.get(response_id, "") + chunk_text
        self._playback_sequence += 1
        await self._send_json({
            "type": "response.audio.playback",
            "response_id": response_id,
            "chunk_id": chunk_id,
            "chunk_text": chunk_text,
            "last": bool(event.get("final")),
            "seq": self._playback_sequence,
            "client_ts_ms": int(time.time() * 1000),
        })
        if event.get("final") is True and response_id not in self._completed_response_ids:
            self._completed_response_ids.add(response_id)
            await self.on_event({
                "type": "AgentAudioDone",
                "call_id": self._call_id,
                "streaming_done": True,
                "segment_id": segment_id,
                "response_id": response_id,
            })

    def _apply_ready_audio_contract(self, event: Dict[str, Any]) -> None:
        mode = str(event.get("mode") or "voice").strip().lower()
        if mode != "voice":
            raise ExternalStrategyProtocolError("External strategy session was not prepared for voice mode")
        audio = event.get("audio")
        if not isinstance(audio, dict):
            raise ExternalStrategyProtocolError("session.ready omitted the audio contract")

        contracts: dict[str, dict[str, Any]] = {}
        for direction in ("input", "output"):
            contract = audio.get(direction)
            if not isinstance(contract, dict):
                raise ExternalStrategyProtocolError(
                    f"session.ready omitted the {direction} audio contract"
                )
            encoding = str(contract.get("encoding") or "").strip().lower()
            if encoding != "pcm_s16le":
                raise ExternalStrategyProtocolError(
                    f"session.ready {direction} audio was not PCM16 little-endian"
                )
            try:
                channels = int(contract.get("channels"))
                sample_rate = int(contract.get("sample_rate"))
            except (TypeError, ValueError) as exc:
                raise ExternalStrategyProtocolError(
                    f"session.ready {direction} audio contract was invalid"
                ) from exc
            if channels != 1:
                raise ExternalStrategyProtocolError(
                    f"session.ready {direction} audio must be mono"
                )
            contracts[direction] = {"sample_rate": sample_rate}

        input_rate = contracts["input"]["sample_rate"]
        output_rate = contracts["output"]["sample_rate"]
        if not 8000 <= input_rate <= 192000:
            raise ExternalStrategyProtocolError("session.ready input sample rate was invalid")
        if input_rate != self._input_rate:
            raise ExternalStrategyProtocolError("session.ready changed the input sample rate")
        if output_rate not in {8000, 16000, 24000, 32000, 48000}:
            raise ExternalStrategyProtocolError("session.ready output sample rate was invalid")
        self._output_rate = output_rate

    async def _handle_probe(self, event: Dict[str, Any]) -> None:
        response_id = str(event.get("response_id") or "").strip()
        chunk_id = str(event.get("chunk_id") or "").strip()
        await self._send_json({
            "type": "response.audio.probe_result",
            "probe_id": event.get("probe_id"),
            "response_id": response_id,
            "chunk_id": chunk_id,
            "entry_found": bool(response_id in self._played_text or chunk_id in self._seen_chunk_ids),
            "visible_text": self._played_text.get(response_id, ""),
            "client_ts_ms": int(time.time() * 1000),
        })

    async def _handle_interrupted(self, event: Dict[str, Any]) -> None:
        response_id = str(event.get("response_id") or "").strip()
        blocked = {response_id} if response_id else set()
        blocked.update(str(item) for item in (event.get("blocked_response_ids") or []) if str(item))
        self._blocked_response_ids.update(blocked)
        if response_id and event.get("truncated_text") is not None:
            self._played_text[response_id] = str(event.get("truncated_text") or "")
        if response_id in self._started_response_ids and response_id not in self._completed_response_ids:
            self._completed_response_ids.add(response_id)
            await self.on_event({
                "type": "AgentAudioDone",
                "call_id": self._call_id,
                "streaming_done": True,
                "segment_id": self._segment_id(response_id),
                "response_id": response_id,
            })
        await self.on_event({
            "type": "interruption",
            "call_id": self._call_id,
            "response_id": response_id,
            "blocked_response_ids": sorted(blocked),
            "truncated_text": event.get("truncated_text"),
        })
        await self._send_json({
            "type": "response.audio.interrupt_ack",
            "response_id": response_id,
            "chunk_id": event.get("chunk_id"),
            "client_ts_ms": int(time.time() * 1000),
        })

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        if self._ws is None:
            raise ExternalStrategyProtocolError("External strategy WebSocket is not connected")
        async with self._send_lock:
            await self._ws.send_json(payload)

    async def _close_resources(self, *, send_end: bool) -> None:
        if send_end and self._connected and self._ws is not None:
            try:
                await self._send_json({"type": "session.end"})
            except Exception:
                pass
        current = asyncio.current_task()
        if self._receive_task is not None and self._receive_task is not current:
            self._receive_task.cancel()
            await asyncio.gather(self._receive_task, return_exceptions=True)
        self._receive_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None
        if self._http is not None:
            await self._http.close()
        self._http = None
        self._connected = False
        self._ended.set()

    def _validated_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        external = context.get("external_strategy")
        if not isinstance(external, dict):
            raise ValueError("External strategy context is missing")
        network = external.get("network")
        ai = external.get("ai")
        if not isinstance(network, dict) or not str(network.get("external_id") or "").strip():
            raise ValueError("External strategy network is not configured")
        if not isinstance(ai, dict):
            raise ValueError("External strategy AI persona is not configured")
        for key in ("title", "gender", "background"):
            if not str(ai.get(key) or "").strip():
                raise ValueError(f"External strategy AI {key} is required")
        if str(ai.get("gender")) not in {"男", "女"}:
            raise ValueError("External strategy AI gender must be 男 or 女")
        human = external.get("human") if isinstance(external.get("human"), dict) else {}
        if str(human.get("gender") or "") not in {"", "男", "女"}:
            raise ValueError("External strategy human gender must be empty, 男 or 女")
        raw_audio = external.get("audio") if isinstance(external.get("audio"), dict) else {}
        input_rate = int(raw_audio.get("input_sample_rate") or self.config.provider_input_sample_rate_hz)
        output_rate = int(raw_audio.get("output_sample_rate") or self.config.output_sample_rate_hz)
        chunk_ms = int(raw_audio.get("chunk_ms") or self.config.audio_chunk_ms)
        if not 8000 <= input_rate <= 192000:
            raise ValueError("External strategy input sample rate is invalid")
        if output_rate not in {8000, 16000, 24000, 32000, 48000}:
            raise ValueError("External strategy output sample rate is invalid")
        if not 20 <= chunk_ms <= 1000:
            raise ValueError("External strategy audio chunk duration is invalid")
        frame_bytes = int(input_rate * 2 * chunk_ms / 1000)
        if frame_bytes > MAX_INPUT_FRAME_BYTES:
            raise ValueError("External strategy audio chunk exceeds the 256 KiB frame limit")
        normalized = dict(external)
        normalized["network"] = dict(network)
        normalized["ai"] = dict(ai)
        normalized["human"] = dict(human)
        normalized["audio"] = {
            "input_sample_rate": input_rate,
            "output_sample_rate": output_rate,
            "chunk_ms": chunk_ms,
        }
        return normalized

    def _settings_payload(self, external: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        human = dict(external.get("human") or {})
        template = str(human.pop("background_template", "") or human.get("background") or "")
        if template:
            values = {
                "caller_name": str(context.get("caller_name") or ""),
                "caller_id": str(context.get("caller_id") or ""),
            }
            try:
                human["background"] = template.format(**values)
            except (KeyError, ValueError):
                human["background"] = template
        return {
            "confirm": True,
            "network": {
                "mode": "existing",
                "id": str(external["network"]["external_id"]).strip(),
            },
            "ai": dict(external["ai"]),
            "human": human,
        }

    @staticmethod
    def _select_voice(
        catalog: Dict[str, Any],
        configured: Dict[str, Any],
        *,
        ai_gender: str = "",
    ) -> Optional[Dict[str, str]]:
        if catalog.get("enabled") is False:
            return None
        voices = [item for item in (catalog.get("voices") or []) if isinstance(item, dict)]
        if not voices:
            return None
        value_field = str(catalog.get("value_field") or "").strip()
        if not value_field:
            raise ExternalStrategyProtocolError("Voice catalog omitted value_field")
        options: dict[str, Dict[str, Any]] = {}
        for item in voices:
            value = str(item.get("value") or item.get("uuid") or item.get("voice_id") or "").strip()
            if value:
                options[value] = item
        requested = str(configured.get("value") or "").strip()
        if requested and requested not in options:
            raise ExternalStrategyProtocolError("Configured external strategy voice is no longer available")
        if not requested:
            for key in ("current_value", "suggested_value"):
                candidate = str(catalog.get(key) or "").strip()
                if candidate and (not options or candidate in options):
                    requested = candidate
                    break
        if not requested and ai_gender:
            requested = next(
                (
                    value
                    for value, item in options.items()
                    if str(item.get("gender") or "").strip() == ai_gender
                ),
                "",
            )
        if not requested and options:
            requested = next(iter(options))
        if not requested:
            return None
        return {"value_field": value_field, "value": requested}

    def _http_url(self, path: str) -> str:
        return self._bounded_url(path, websocket=False)

    def _websocket_url(self, path: str) -> str:
        return self._bounded_url(path, websocket=True)

    def _bounded_url(self, path: str, *, websocket: bool) -> str:
        base = self.config.base_url.rstrip("/") + "/"
        parsed_base = urlparse(base)
        base_scheme = parsed_base.scheme.lower()
        if base_scheme not in {"http", "https"} or not parsed_base.netloc:
            raise ExternalStrategyProtocolError("External strategy base URL is invalid")
        expected_scheme = (
            "wss" if base_scheme == "https" else "ws"
        ) if websocket else base_scheme
        value = str(path or "").strip()
        if websocket and not urlparse(value).scheme:
            websocket_base = f"{expected_scheme}://{parsed_base.netloc}{parsed_base.path}"
            value = urljoin(websocket_base, value)
        elif not websocket:
            value = urljoin(base, value)
        parsed = urlparse(value)
        if (
            parsed.scheme.lower() != expected_scheme
            or parsed.netloc.lower() != parsed_base.netloc.lower()
        ):
            raise ExternalStrategyProtocolError("External strategy service returned an unsafe URL")
        return value

    @staticmethod
    def _to_pcm16(audio: bytes, encoding: str) -> bytes:
        normalized = str(encoding or "").lower().strip()
        if normalized in {"linear16", "pcm16", "slin", "slin16", "pcm_s16le"}:
            return audio
        if normalized in {"ulaw", "mulaw", "g711_ulaw", "mu-law"}:
            return audioop.ulaw2lin(audio, 2)
        if normalized in {"alaw", "g711_alaw", "a-law"}:
            return audioop.alaw2lin(audio, 2)
        raise ValueError(f"Unsupported external strategy input encoding: {encoding}")

    def _segment_id(self, response_id: str) -> str:
        return f"{self._call_id}:assistant:{response_id}"

    def _event_id(self, prefix: str, event: Dict[str, Any]) -> str:
        identity = (
            event.get("response_id")
            or event.get("log_id")
            or event.get("sequence")
            or event.get("timestamp_ms")
            or int(time.time() * 1000)
        )
        return f"external:{self._session_id}:{prefix}:{identity}"

    @staticmethod
    def _required_string(payload: Dict[str, Any], key: str) -> str:
        value = str(payload.get(key) or "").strip()
        if not value:
            raise ExternalStrategyProtocolError(f"External strategy response omitted {key}")
        return value

    @staticmethod
    def _public_error(raw: str) -> str:
        try:
            payload = json.loads(raw)
            error = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error, dict):
                return str(error.get("message") or error.get("code") or "request failed")[:300]
        except (TypeError, json.JSONDecodeError):
            pass
        return "request failed"
