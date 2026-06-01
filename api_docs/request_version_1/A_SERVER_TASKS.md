# A 服务器任务单：Admin UI / ai_engine 音色库接入

面向团队：A 服务器后端与 Admin UI 前端开发。
依赖规范：`api_docs/AB_C_HIFI_VOICE_LIBRARY_SPEC.md`。

---

## 1. 目标

A 服务器负责提供用户自助音色管理入口，并在通话 TTS 请求中把选中的 `voice_id` 通过现有 WebSocket 传给 B。

必须保持：

- A 与 B 仍通过现有 WebSocket 通信。
- 不新增额外鉴权。
- 旧通话流程不选音色时不受影响。

---

## 2. A 侧职责

1. Admin UI 增加“音色库”页面。
2. 支持上传参考音频 `wav`、填写 `prompt_text`、`display_name`。
3. 后端通过 A→B WebSocket 发送 `voice_library.create`。
4. 保存 B 返回的 `voice_id`、`voice_revision_id`、`hifi_id`。
5. 支持查询、删除、更新音色元数据。
6. 支持试听音色。
7. 通话配置支持选择默认 `voice_id`。
8. ai_engine 发起 TTS 时在 `tts_request` 中追加可选 `voice` 字段。

---

## 3. Admin UI 页面需求

### 3.1 音色列表

字段：

- `display_name`
- `voice_id`
- `status`
- `prompt_text`
- `latest_hifi_id`
- `created_at`
- `updated_at`

操作：

- 新建音色
- 编辑显示名称 / tags / metadata
- 删除音色
- 试听音色
- 设为默认通话音色

### 3.2 新建音色表单

必填：

- `display_name`
- `reference_audio_file`：仅支持 `.wav`
- `prompt_text`：必须与参考音频内容一致

选填：

- `language`
- `tags`
- `metadata`

前端限制建议：

- 文件大小默认不超过 20MB。
- 上传前校验扩展名与 MIME。
- 上传时显示“正在创建 HiFi 缓存”状态。

---

## 4. A 后端内部 API 建议

这些 API 是 Admin UI 调 A 后端，不暴露给 B/C。

### 4.1 创建音色

`POST /api/voice-library`

#### Request JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "AServerCreateVoiceRequest",
  "type": "object",
  "required": ["display_name", "wav_base64", "prompt_text"],
  "properties": {
    "display_name": { "type": "string", "minLength": 1, "maxLength": 80 },
    "wav_base64": { "type": "string", "contentEncoding": "base64" },
    "wav_format": { "type": "string", "enum": ["wav"], "default": "wav" },
    "prompt_text": { "type": "string", "minLength": 1, "maxLength": 500 },
    "source_filename": { "type": "string", "maxLength": 255 },
    "language": { "type": "string", "default": "zh-CN" },
    "tags": { "type": "array", "items": { "type": "string" }, "default": [] },
    "metadata": { "type": "object", "additionalProperties": true }
  },
  "additionalProperties": false
}
```

#### Response JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "AServerCreateVoiceResponse",
  "type": "object",
  "required": ["voice_id", "voice_revision_id", "hifi_id", "status"],
  "properties": {
    "voice_id": { "type": "string" },
    "voice_revision_id": { "type": "string" },
    "hifi_id": { "type": "string" },
    "display_name": { "type": "string" },
    "status": { "type": "string", "enum": ["ready"] },
    "reference_audio_sha256": { "type": "string" },
    "created_at": { "type": "string", "format": "date-time" }
  },
  "additionalProperties": false
}
```

### 4.2 查询音色列表

`GET /api/voice-library`

Response 直接映射 B 的 `voice_library.list_result.payload`。

### 4.3 更新音色

`PATCH /api/voice-library/{voice_id}`

#### Request JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "AServerUpdateVoiceRequest",
  "type": "object",
  "properties": {
    "display_name": { "type": "string", "minLength": 1, "maxLength": 80 },
    "tags": { "type": "array", "items": { "type": "string" } },
    "metadata": { "type": "object", "additionalProperties": true }
  },
  "additionalProperties": false
}
```

### 4.4 删除音色

`DELETE /api/voice-library/{voice_id}`

### 4.5 试听音色

`POST /api/voice-library/{voice_id}/preview`

#### Request JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "AServerPreviewVoiceRequest",
  "type": "object",
  "required": ["target_text"],
  "properties": {
    "target_text": { "type": "string", "minLength": 1, "maxLength": 500 },
    "response_audio_format": {
      "type": "string",
      "enum": ["mp3_base64", "mulaw_8000_base64"],
      "default": "mp3_base64"
    }
  },
  "additionalProperties": false
}
```

#### Response JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "AServerPreviewVoiceResponse",
  "type": "object",
  "required": ["voice_id", "hifi_id", "audio_base64", "audio_format"],
  "properties": {
    "voice_id": { "type": "string" },
    "voice_revision_id": { "type": "string" },
    "hifi_id": { "type": "string" },
    "audio_base64": { "type": "string", "contentEncoding": "base64" },
    "audio_format": { "type": "string", "enum": ["mp3", "mulaw_8000"] },
    "duration_ms": { "type": "integer" },
    "ttfb_ms": { "type": "number" },
    "total_ms": { "type": "number" }
  },
  "additionalProperties": false
}
```

---

## 5. A→B WebSocket 消息要求

A 后端只需要把 Admin UI 请求转换为规范中的 WS 消息：

- `voice_library.create`
- `voice_library.list`
- `voice_library.update`
- `voice_library.delete`
- `voice_library.preview`

每个请求必须生成 `request_id` 并等待匹配响应。

---

## 6. ai_engine TTS 请求改造

通话配置新增：

```json
{
  "default_voice": {
    "voice_id": "voice_01JABCDEF123456789",
    "voice_revision_id": "vrev_01JABCDEF123456789",
    "hifi_id": "optional-fast-path-cache-id"
  }
}
```

ai_engine 原 TTS 请求：

```json
{
  "type": "tts_request",
  "call_id": "call-001",
  "mode": "tts",
  "text": "您好，请问有什么可以帮您？"
}
```

改造后：

```json
{
  "type": "tts_request",
  "call_id": "call-001",
  "mode": "tts",
  "text": "您好，请问有什么可以帮您？",
  "voice": {
    "voice_id": "voice_01JABCDEF123456789",
    "voice_revision_id": "vrev_01JABCDEF123456789",
    "hifi_id": "optional-fast-path-cache-id"
  }
}
```

兼容要求：

- 未配置默认音色时不要发送 `voice` 字段。
- A 不负责判断 `hifi_id` 是否有效。
- B 返回 `hifi_refreshed=true` 时，A 可以异步更新本地保存的 `hifi_id`。

---

## 7. A 侧验收标准

1. Admin UI 可以创建、列表、更新、删除、试听音色。
2. A 后端所有音色库操作均通过 B 的 WebSocket 完成。
3. 通话时选择音色后，ai_engine 的 `tts_request` 带上 `voice.voice_id`。
4. 未选择音色时，旧 TTS 流程完全不变。
5. B/C 异常时 Admin UI 能展示明确错误，不把错误吞掉。
