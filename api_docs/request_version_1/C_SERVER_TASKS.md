# C 服务器任务单：HiFi TTS 服务配合要求

面向团队：C 服务器 HiFi TTS 后端开发。
依赖规范：`api_docs/HIFI_STREAMING_API.md`、`api_docs/AB_C_HIFI_VOICE_LIBRARY_SPEC.md`。

---

## 1. 目标

C 服务器继续作为纯 TTS 推理服务，不参与 A 的 Admin UI 业务逻辑，不维护 `voice_id`。

C 需要保证现有接口稳定：

- `GET /health`
- `GET /ready`
- `POST /add_hifi`
- `POST /generate`
- `DELETE /hifi/{hifi_id}`

重要确认：

- `/generate` 仍返回 `audio/mpeg`，当前版本不要求支持 wav/pcm_s16le。
- B 会负责 MP3 解码与 telephony 格式转换。

---

## 2. C 侧职责

1. 提供稳定的 `/ready` 健康检查。
2. `/add_hifi` 接收 B 上传的参考 wav，并返回可复用 `hifi_id`。
3. `/generate` 使用 `hifi_id` 生成 MP3 流。
4. `/hifi/{hifi_id}` 支持删除缓存。
5. 当 `hifi_id` 不存在时，明确返回 404，方便 B 自动刷新。
6. 返回必要响应头，便于 B 记录诊断信息。

---

## 3. API Schema

### 3.1 `GET /ready`

#### Response 200 JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "CReadyResponse",
  "type": "object",
  "properties": {
    "status": {
      "type": "string",
      "enum": ["ready"]
    },
    "model_loaded": {
      "type": "boolean"
    },
    "warmup_done": {
      "type": "boolean"
    }
  },
  "additionalProperties": true
}
```

如果现有实现只返回简单 JSON 或文本，也可以保持；B 只依赖 HTTP 2xx。

### 3.2 `POST /add_hifi`

#### Request JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "CAddHifiRequest",
  "type": "object",
  "required": ["wav_base64", "wav_format", "prompt_text"],
  "properties": {
    "wav_base64": {
      "type": "string",
      "contentEncoding": "base64",
      "description": "完整 wav 文件字节 base64，不允许 data URI"
    },
    "wav_format": {
      "type": "string",
      "enum": ["wav"]
    },
    "prompt_text": {
      "type": "string",
      "minLength": 1,
      "maxLength": 500
    }
  },
  "additionalProperties": false
}
```

#### Response JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "CAddHifiResponse",
  "type": "object",
  "required": ["hifi_id", "prompt_id", "reference_id", "sample_rate", "channels"],
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

### 3.3 `POST /generate`

#### Request JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "CGenerateRequest",
  "type": "object",
  "required": ["target_text", "hifi_id"],
  "properties": {
    "target_text": {
      "type": "string",
      "minLength": 1
    },
    "hifi_id": {
      "type": "string"
    },
    "max_generate_length": {
      "type": "integer",
      "default": 2000
    },
    "temperature": {
      "type": "number",
      "default": 1.0
    },
    "cfg_value": {
      "type": "number",
      "default": 2.0
    }
  },
  "additionalProperties": false
}
```

#### Response

```http
HTTP/1.1 200 OK
Content-Type: audio/mpeg
X-Audio-Sample-Rate: 48000
X-Audio-Channels: 1
```

响应体为 MP3 字节流。

### 3.4 `DELETE /hifi/{hifi_id}`

#### Response JSON Schema

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "CDeleteHifiResponse",
  "type": "object",
  "properties": {
    "deleted": {
      "type": "boolean"
    },
    "hifi_id": {
      "type": "string"
    }
  },
  "additionalProperties": true
}
```

如果当前实现无响应体，也可以保持；B 只依赖 2xx/404。

---

## 4. 错误语义要求

### 4.1 `/add_hifi`

- `400`：base64 无效、格式不支持、prompt_text 缺失
- `503`：模型未 ready
- `500`：内部错误

### 4.2 `/generate`

- `400`：target_text 缺失或参数错误
- `404`：`hifi_id` 不存在或已失效
- `503`：模型未 ready
- `500`：内部错误

404 非常重要：B 依赖此状态判断是否重新调用 `/add_hifi` 刷新缓存。

### 4.3 `/hifi/{hifi_id}`

- `204` 或 `200`：删除成功
- `404`：缓存不存在，B 可视为已删除

---

## 5. 性能与稳定性要求

1. `/ready` 必须能反映模型是否可接收生产流量。
2. `/generate` 首包 TTFB 需要可观测，建议暴露 `/metrics`。
3. C 重启后历史 `hifi_id` 可以失效，但必须通过 404 明确表达。
4. C 不需要理解 `voice_id`、`voice_revision_id`，这些是 B 的业务字段。
5. MP3 编码参数继续由 C 环境变量控制。

---

## 6. C 侧验收标准

1. B 调用 `/add_hifi` 能稳定获得 `hifi_id`。
2. B 调用 `/generate` 能获得 `audio/mpeg` 流。
3. 删除或失效的 `hifi_id` 调用 `/generate` 返回 404。
4. `/ready` 在模型未加载时不会误报 ready。
5. 并发试听与通话 TTS 时服务不会阻塞健康检查。
