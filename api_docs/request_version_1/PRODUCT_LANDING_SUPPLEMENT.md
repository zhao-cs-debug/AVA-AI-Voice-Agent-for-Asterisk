# HiFi 音色库 V1 落地补充与产品决策

本文档是 `AB_C_HIFI_VOICE_LIBRARY_SPEC.md`、`A_SERVER_TASKS.md`、`B_SERVER_TASKS.md`、`C_SERVER_TASKS.md` 的增量补充。原文档作为第一版规范保留不改；本文件用于解决当前 A/B/C 已完成开发后暴露出的落地问题，并给三端后续执行统一口径。

## 1. 当前结论

本轮 A/B/C 主链路已经具备产品落地条件：

- A 已接入 Admin 音色库、Context 默认音色、TTS 请求透传 `voice.voice_id`。
- B 已实现音色库持久化、A/B WebSocket 扩展、B/C HiFi 调用、404 自动重建 `hifi_id`、MP3 解码转电话输出格式。
- C 已收紧 HiFi API 边界，保持 `/generate` 404 语义，并补齐删除、ready、非法请求测试。

当前不建议推翻 V1 方案，也不建议把 C 做成业务音色库。产品侧只需要补齐下面三个口径：

1. `voice_library.list` 必须能让 Admin UI 预填可编辑字段。
2. `audio_decode_failed` 纳入 A/B 错误码口径。
3. A/B Docker 化部署需要形成固定验收清单，避免环境问题被误判为功能缺陷。

## 2. 产品决策

### 2.1 `voice_library.list` 返回 `tags/metadata`

决策：从 V1 落地版开始，B 的 `voice_library.list_result.payload.items[]` 需要返回 `tags`、`metadata`，并建议同时返回 `language`。

原因：

- A 的 Admin UI 有编辑 `tags/metadata` 的入口。
- 如果列表不返回这两个字段，编辑页只能写入新值，无法展示旧值，用户会误以为历史配置丢失。
- 这些字段属于 B 持久化的业务元数据，返回给 A 不会破坏 C 的纯推理边界。

兼容要求：

- B 返回 `tags` 时使用数组；无值返回 `[]`。
- B 返回 `metadata` 时使用对象；无值返回 `{}`。
- B 返回 `language` 时使用字符串；无值建议返回 `"zh-CN"`，或不返回。
- A 必须兼容旧 B 不返回这些字段的情况，当前输入新值再更新的行为可保留为降级路径。

有效增量 Schema：

```json
{
  "voice_id": "voice_01J...",
  "display_name": "客服女声",
  "status": "ready",
  "active_revision_id": "vrev_01J...",
  "latest_hifi_id": "hifi_...",
  "prompt_text": "您好，请问有什么可以帮您？",
  "reference_audio_sha256": "...",
  "language": "zh-CN",
  "tags": ["客服", "普通话"],
  "metadata": {
    "owner": "ops",
    "scenario": "outbound"
  },
  "created_at": "2026-04-29T00:00:00Z",
  "updated_at": "2026-04-29T00:00:00Z"
}
```

执行任务：

- B：补充 `voice_library.list` 返回字段，并加一条列表响应测试。
- A：收到 `tags/metadata/language` 时预填编辑表单；字段缺失时保持当前降级行为。
- 产品验收：创建带 `tags/metadata` 的音色，刷新页面后再次编辑，旧值必须可见。

### 2.2 `audio_decode_failed` 错误码

决策：`audio_decode_failed` 是 V1 落地版正式错误码，适用于 B 已拿到 C 返回的音频但无法解码或转为目标电话音频格式的场景。

适用范围：

- `tts_request` 走 HiFi TTS 时，B 解码 C 返回 MP3 失败，返回 `tts_error.error.code=audio_decode_failed`。
- `voice_library.preview` 试听时，如果 B 需要转码且转码失败，返回 `voice_library.error.error.code=audio_decode_failed`。

错误语义：

- 默认 `retryable=false`。
- 如果失败原因明确是临时资源不足、ffmpeg 进程异常等可恢复问题，B 可返回 `retryable=true`，但不要求 A 自动重试。
- A Admin UI 展示文案建议为：“音频生成成功但解码失败，请检查 B 服务器 ffmpeg/音频转码环境。”

执行任务：

- B：确保 `voice_library.error` 和 `tts_error` 都能返回 `audio_decode_failed`。
- A：把该错误码映射为可读提示，不展示原始堆栈。
- 部署：B 镜像必须包含 `ffmpeg`，并确认 `HIFI_TTS_FFMPEG_BIN=ffmpeg` 可执行。

### 2.3 Docker 部署与环境问题归类

决策：A/B 服务器均按 Docker 部署验收。宿主机本地 npm 或 node_modules 缺失，不作为产品功能阻塞；只有容器镜像构建或容器内运行失败才算部署阻塞。

A 服务器必须配置：

```env
VOICE_LIBRARY_ENABLED=true
VOICE_LIBRARY_WS_URL=ws://<b-server-ip>:8765
LOCAL_WS_AUTH_TOKEN=<same-token-as-b>
```

B 服务器必须配置：

```env
VOICE_LIBRARY_ENABLED=true
VOICE_LIBRARY_DATA_DIR=/app/data/voice_library
VOICE_LIBRARY_MAX_AUDIO_MB=20
VOICE_LIBRARY_CREATE_HIFI_ON_UPLOAD=true
VOICE_LIBRARY_REFRESH_HIFI_ON_404=true
HIFI_TTS_BASE_URL=http://<c-server-ip>:8000
HIFI_TTS_FFMPEG_BIN=ffmpeg
```

B 容器必须挂载持久化目录：

```yaml
volumes:
  - ./data/voice_library:/app/data/voice_library
```

执行任务：

- A：前端构建放到标准 Node/npm 环境或 Docker 构建环境中验证；不要用当前宿主机缺失 `semver` 的 npm 状态作为失败结论。
- B：重建 `local_ai_server` 镜像，确认镜像内 `ffmpeg -version` 可执行。
- 运维：把 A/B/C 的 IP、端口、token、数据卷路径整理到部署 `.env`，避免手工改 compose。

## 3. 联调验收清单

上线前按以下顺序验收，任一失败先定位到对应端，不扩大改动范围。

### 3.1 C 单体验收

1. `GET /ready`：ready 时返回 200，未 ready 时返回 503。
2. `POST /add_hifi`：只接受原始 base64 WAV、`wav_format="wav"`、`prompt_text`。
3. `POST /generate`：有效 `hifi_id` 返回 `audio/mpeg`。
4. `DELETE /hifi/{hifi_id}`：删除后再次 `/generate` 返回 404。

### 3.2 B 单体验收

1. 创建音色后，SQLite 和参考 WAV 均持久化到 `VOICE_LIBRARY_DATA_DIR`。
2. B 重启后，`voice_library.list` 仍能列出音色。
3. `voice_library.list` 返回 `tags/metadata`，Admin UI 可预填。
4. C 清缓存或重启后，B 对 HiFi TTS 自动刷新 `hifi_id` 并重试一次。
5. B 镜像内 `ffmpeg` 可用，HiFi MP3 能转为当前电话输出格式。

### 3.3 A/B 联调验收

1. Admin UI 创建音色成功，并显示 `voice_id`、`status`、`latest_hifi_id`。
2. Admin UI 编辑 `display_name/tags/metadata` 后刷新页面，旧值可见。
3. Admin UI 试听成功。
4. Context 设置默认音色后，新通话的 `tts_request` 带 `voice.voice_id`。
5. 不配置默认音色时，旧 TTS 流程保持不变。

### 3.4 A/B/C 端到端验收

1. 创建音色：A 上传 WAV，B 保存，C 返回 `hifi_id`。
2. 外呼通话：A 发送文本和 `voice_id`，B 调 C 生成，A 收到可播放电话音频。
3. C 重启：旧 `hifi_id` 失效后，B 自动 `/add_hifi` 刷新并继续生成。
4. 删除音色：A 删除后，B 删除业务记录并调用 C 删除缓存；后续使用该 `voice_id` 返回 `voice_not_found`。
5. 错误展示：C 不可用、音色不存在、解码失败时，A 展示产品化错误文案。

## 4. 发布分工

A 侧补充：

- 兼容并预填 `voice_library.list` 返回的 `tags/metadata/language`。
- 增加 `audio_decode_failed` 的 UI 错误文案。
- 用 Docker/标准 Node 环境完成前端 build 验证。

B 侧补充：

- 在 `voice_library.list` 响应中返回 `tags/metadata/language`。
- 将 `audio_decode_failed` 纳入 `voice_library.error` 的错误码枚举和测试。
- 确认 Docker 镜像包含 `ffmpeg`，并补充镜像内 smoke check。

C 侧补充：

- 暂无新增接口需求。
- 继续保持 `hifi_id` 失效返回 404，不接收 `voice_id` 等业务字段。

运维补充：

- 建议按 C -> B -> A 的顺序启动和验收。
- B 依赖 C 的 `/ready`，A 依赖 B 的 WebSocket。
- 发布前备份 `./data/voice_library`，避免重建容器时丢失音色资产。

## 5. V1 发布准入

满足以下条件后，可进入小流量试运行：

- A/B/C 单体验收全部通过。
- A/B/C 端到端验收全部通过。
- B 重启、C 重启两个恢复场景通过。
- 至少保留 1 条默认旧 TTS 回归用例，证明未选择音色时旧链路不受影响。
- 部署文档中已记录 A/B/C 实际 IP、端口、token、数据卷路径和镜像版本。

不阻塞 V1 的事项：

- A 宿主机本地 npm 缺少 `semver`。
- 旧版 B 不返回 `tags/metadata` 时，A 仍只能输入新值更新。
- C 不支持 wav/pcm_s16le 输出；V1 仍由 B 负责 MP3 解码和电话格式转换。
