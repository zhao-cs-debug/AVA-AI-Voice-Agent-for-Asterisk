# B 服务器任务单：local_ai_server 音色库与 WS 协议扩展

面向团队：B 服务器 `local_ai_server` 后端开发。
依赖规范：`api_docs/AB_C_HIFI_VOICE_LIBRARY_SPEC.md`、`api_docs/HIFI_STREAMING_API.md`。

---

## 1. 目标

B 服务器新增音色库能力：

- 接收 A 通过 WebSocket 上传的参考音频。
- 持久化参考音频与元数据。
- 调用 C `/add_hifi` 创建 HiFi 缓存并保存 `hifi_id`。
- 在 TTS WebSocket 请求中根据 `voice_id` 调用 C `/generate`。
- 保持现有 A/B WebSocket TTS 协议向后兼容。

---

## 2. B 侧职责

1. 扩展现有 WebSocket handler，识别 `voice_library.*` 消息。
2. 扩展 `tts_request`，支持可选 `voice` 字段。
3. 维护本地音色库：
   - `voice_id`
   - `voice_revision_id`
   - 参考音频文件
   - `prompt_text`
   - `hifi_id`
   - C 返回的 `prompt_id/reference_id`
4. 调用 C：
   - 创建：`POST /add_hifi`
   - 生成：`POST /generate`
   - 删除：`DELETE /hifi/{hifi_id}`
5. C 返回 MP3 时，B 解码并转为当前 WS TTS 输出格式。
6. C `hifi_id` 失效时，B 自动重建并继续生成。

---

## 3. 配置项

建议新增环境变量：

```env
VOICE_LIBRARY_ENABLED=true
VOICE_LIBRARY_DATA_DIR=/app/data/voice_library
VOICE_LIBRARY_MAX_AUDIO_MB=20
VOICE_LIBRARY_CREATE_HIFI_ON_UPLOAD=true
VOICE_LIBRARY_REFRESH_HIFI_ON_404=true

HIFI_TTS_BASE_URL=http://<c-server-ip>:8000
HIFI_TTS_CONNECT_TIMEOUT_SEC=5
HIFI_TTS_RESPONSE_TIMEOUT_SEC=120
HIFI_TTS_MAX_GENERATE_LENGTH=2000
HIFI_TTS_TEMPERATURE=1.0
HIFI_TTS_CFG_VALUE=2.0
HIFI_TTS_FFMPEG_BIN=ffmpeg
```

容器卷建议：

```yaml
volumes:
  - ./data/voice_library:/app/data/voice_library
```

---

## 4. 存储结构建议

```text
/app/data/voice_library/
  voices.jsonl 或 voices.sqlite
  audio/
    <voice_id>/
      <voice_revision_id>.wav
```

最小可用版本可以先用 SQLite；后续再接对象存储。

---

## 5. WebSocket 消息实现清单

B 必须实现以下消息：

- `voice_library.create`
- `voice_library.list`
- `voice_library.update`
- `voice_library.delete`
- `voice_library.preview`
- `tts_request` 可选 `voice`

完整 schema 见 `AB_C_HIFI_VOICE_LIBRARY_SPEC.md`。

---

## 6. B 内部数据模型 Schema

### 6.1 VoiceRecord

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "BVoiceRecord",
  "type": "object",
  "required": ["voice_id", "display_name", "active_revision_id", "status", "created_at", "updated_at"],
  "properties": {
    "voice_id": { "type": "string" },
    "display_name": { "type": "string" },
    "active_revision_id": { "type": "string" },
    "status": { "type": "string", "enum": ["ready", "degraded", "deleted"] },
    "tags": { "type": "array", "items": { "type": "string" } },
    "metadata": { "type": "object", "additionalProperties": true },
    "created_at": { "type": "string", "format": "date-time" },
    "updated_at": { "type": "string", "format": "date-time" },
    "deleted_at": { "type": ["string", "null"], "format": "date-time" }
  },
  "additionalProperties": false
}
```

### 6.2 VoiceRevisionRecord

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "BVoiceRevisionRecord",
  "type": "object",
  "required": [
    "voice_revision_id",
    "voice_id",
    "reference_audio_path",
    "reference_audio_sha256",
    "prompt_text",
    "wav_format",
    "created_at"
  ],
  "properties": {
    "voice_revision_id": { "type": "string" },
    "voice_id": { "type": "string" },
    "reference_audio_path": { "type": "string" },
    "reference_audio_sha256": { "type": "string" },
    "prompt_text": { "type": "string" },
    "wav_format": { "type": "string", "enum": ["wav"] },
    "hifi_id": { "type": ["string", "null"] },
    "prompt_id": { "type": ["string", "null"] },
    "reference_id": { "type": ["string", "null"] },
    "sample_rate": { "type": ["integer", "null"] },
    "channels": { "type": ["integer", "null"] },
    "created_at": { "type": "string", "format": "date-time" },
    "hifi_created_at": { "type": ["string", "null"], "format": "date-time" },
    "hifi_last_checked_at": { "type": ["string", "null"], "format": "date-time" }
  },
  "additionalProperties": false
}
```

---

## 7. `voice_library.create` 处理流程

1. 校验 `wav_base64` 不为空且不是 data URI。
2. base64 解码。
3. 校验文件大小与 WAV 头。
4. 计算 `reference_audio_sha256`。
5. 生成 `voice_id` 和 `voice_revision_id`。
6. 保存音频文件。
7. 调用 C `POST /add_hifi`。
8. 保存 C 返回的 `hifi_id/prompt_id/reference_id`。
9. 返回 `voice_library.created`。

失败策略：

- C 不可用时，如果 `VOICE_LIBRARY_CREATE_HIFI_ON_UPLOAD=true`，返回错误。
- 如果以后允许离线创建，可返回 `status=degraded`，但当前版本建议先要求 C 可用。

---

## 8. `tts_request` 处理流程

### 8.1 无 `voice`

保持原逻辑，走默认 TTS。

### 8.2 有 `voice.voice_id`

1. 查找 `voice_id`。
2. 如果指定 `voice_revision_id`，使用指定版本；否则使用 active revision。
3. 如果请求包含 `hifi_id`，可以尝试使用，但必须以 B 本地 revision 为准。
4. 调用 C `POST /generate`。
5. 如果 C 返回 404：
   - 调用 C `POST /add_hifi` 重建 `hifi_id`。
   - 更新 revision。
   - 重试 `/generate` 一次。
6. C 返回 MP3 后，B 解码为 PCM，再转为现有 TTS 输出格式。
7. 用现有 WS 响应把音频发回 A。

---

## 9. B→C API 调用 Schema

### 9.1 `/add_hifi` Request

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "BToCAddHifiRequest",
  "type": "object",
  "required": ["wav_base64", "wav_format", "prompt_text"],
  "properties": {
    "wav_base64": { "type": "string", "contentEncoding": "base64" },
    "wav_format": { "type": "string", "enum": ["wav"] },
    "prompt_text": { "type": "string", "minLength": 1, "maxLength": 500 }
  },
  "additionalProperties": false
}
```

### 9.2 `/add_hifi` Response

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "BToCAddHifiResponse",
  "type": "object",
  "required": ["hifi_id", "prompt_id", "reference_id"],
  "properties": {
    "hifi_id": { "type": "string" },
    "prompt_id": { "type": "string" },
    "reference_id": { "type": "string" },
    "feat_dim": { "type": "integer" },
    "sample_rate": { "type": "integer" },
    "channels": { "type": "integer" }
  },
  "additionalProperties": true
}
```

### 9.3 `/generate` Request

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "BToCGenerateRequest",
  "type": "object",
  "required": ["target_text", "hifi_id"],
  "properties": {
    "target_text": { "type": "string", "minLength": 1 },
    "hifi_id": { "type": "string" },
    "max_generate_length": { "type": "integer", "default": 2000 },
    "temperature": { "type": "number", "default": 1.0 },
    "cfg_value": { "type": "number", "default": 2.0 }
  },
  "additionalProperties": false
}
```

---

## 10. 错误处理要求

B 对 A 返回错误时使用统一错误 envelope：

- `invalid_request`
- `audio_too_large`
- `unsupported_audio_format`
- `voice_not_found`
- `revision_not_found`
- `hifi_backend_unavailable`
- `hifi_cache_invalid`
- `internal_error`

C 调用失败时：

- 连接失败：`hifi_backend_unavailable`, `retryable=true`
- `/generate` 404：内部刷新一次，仍失败再返回 `hifi_cache_invalid`
- MP3 解码失败：`audio_decode_failed`

---

## 11. B 侧验收标准

1. B 重启后仍能列出已创建音色。
2. B 创建音色时能调用 C 并返回 `hifi_id`。
3. B 能处理带 `voice_id` 的 `tts_request`。
4. B 能处理不带 `voice` 的旧 `tts_request`。
5. C 清缓存或重启后，B 能自动刷新 `hifi_id`。
6. B 能返回 Admin UI 试听音频。
7. 所有新增 WS 消息都有 `request_id` 对应响应。
