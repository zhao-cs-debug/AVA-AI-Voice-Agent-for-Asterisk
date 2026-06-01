import asyncio
import json
import os
import uuid
from typing import Any, Dict, Optional

import websockets
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()


class CreateVoiceRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)
    wav_base64: str = Field(min_length=1)
    wav_format: str = "wav"
    prompt_text: str = Field(min_length=1, max_length=500)
    source_filename: Optional[str] = Field(default=None, max_length=255)
    language: str = "zh-CN"
    tags: list[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class UpdateVoiceRequest(BaseModel):
    display_name: Optional[str] = Field(default=None, min_length=1, max_length=80)
    tags: Optional[list[str]] = None
    metadata: Optional[Dict[str, Any]] = None


class PreviewVoiceRequest(BaseModel):
    target_text: str = Field(min_length=1, max_length=500)
    response_audio_format: str = "mp3_base64"


def _voice_library_ws_url() -> str:
    return (
        os.getenv("VOICE_LIBRARY_WS_URL")
        or os.getenv("LOCAL_WS_URL")
        or os.getenv("LOCAL_WS_BASE_URL")
        or ""
    ).strip()


def _auth_token() -> str:
    return (os.getenv("LOCAL_WS_AUTH_TOKEN") or "").strip()


def _error_detail(response: Dict[str, Any]) -> str:
    error = response.get("error")
    if isinstance(error, dict):
        if error.get("code") == "audio_decode_failed":
            return "音频生成成功但解码失败，请检查 B 服务器 ffmpeg/音频转码环境。"
        return str(error.get("message") or error.get("code") or "Voice library request failed")
    return "Voice library request failed"


async def _send_voice_library_request(
    message_type: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    timeout_sec: float = 60.0,
) -> Dict[str, Any]:
    ws_url = _voice_library_ws_url()
    if not ws_url:
        raise HTTPException(
            status_code=503,
            detail="VOICE_LIBRARY_WS_URL or LOCAL_WS_URL is not configured",
        )

    request_id = f"vl-{uuid.uuid4().hex}"
    message: Dict[str, Any] = {"type": message_type, "request_id": request_id}
    if payload is not None:
        message["payload"] = payload

    try:
        async with websockets.connect(ws_url, ping_interval=None, max_size=None) as websocket:
            token = _auth_token()
            if token:
                await websocket.send(json.dumps({"type": "auth", "auth_token": token}))
                while True:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                    if isinstance(raw, (bytes, bytearray)):
                        continue
                    data = json.loads(raw)
                    if data.get("type") != "auth_response":
                        continue
                    if data.get("status") != "ok":
                        raise HTTPException(status_code=502, detail=data.get("message") or "Local AI auth failed")
                    break

            await websocket.send(json.dumps(message))
            while True:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout_sec)
                if isinstance(raw, (bytes, bytearray)):
                    continue
                data = json.loads(raw)
                if data.get("request_id") != request_id:
                    continue
                if data.get("ok") is False:
                    raise HTTPException(status_code=502, detail=_error_detail(data))
                payload_data = data.get("payload")
                return payload_data if isinstance(payload_data, dict) else {}
    except HTTPException:
        raise
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timed out waiting for voice library response")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Voice library WebSocket request failed: {exc}")


@router.post("/voice-library")
async def create_voice(req: CreateVoiceRequest):
    payload = req.model_dump(exclude_none=True)
    payload["wav_format"] = payload.get("wav_format") or "wav"
    return await _send_voice_library_request("voice_library.create", payload, timeout_sec=120.0)


@router.get("/voice-library")
async def list_voices(limit: int = 50, cursor: Optional[str] = None, include_deleted: bool = False):
    payload: Dict[str, Any] = {
        "limit": max(1, min(int(limit or 50), 200)),
        "include_deleted": bool(include_deleted),
    }
    if cursor:
        payload["cursor"] = cursor
    return await _send_voice_library_request("voice_library.list", payload)


@router.patch("/voice-library/{voice_id}")
async def update_voice(voice_id: str, req: UpdateVoiceRequest):
    payload = {"voice_id": voice_id, **req.model_dump(exclude_none=True)}
    return await _send_voice_library_request("voice_library.update", payload)


@router.delete("/voice-library/{voice_id}")
async def delete_voice(voice_id: str, hard_delete: bool = False):
    return await _send_voice_library_request(
        "voice_library.delete",
        {"voice_id": voice_id, "hard_delete": bool(hard_delete)},
    )


@router.post("/voice-library/{voice_id}/preview")
async def preview_voice(voice_id: str, req: PreviewVoiceRequest):
    payload = {"voice_id": voice_id, **req.model_dump(exclude_none=True)}
    return await _send_voice_library_request("voice_library.preview", payload, timeout_sec=120.0)
