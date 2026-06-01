# HiFi Streaming API 接入文档（实时 TTS）

本文档面向将 `nano-vllm-voxcpm-hifi` 接入实时 TTS 的客户端/网关服务开发者，重点覆盖：

- HiFi 流式接口定义
- 关键环境变量说明
- 推荐调用流程
- 可直接运行的 Python 示例代码

---

## 1. 服务启动与健康检查

在仓库根目录启动：

```bash
uv run fastapi run deployment/app/main.py --host 0.0.0.0 --port 8000
```

健康检查：

- `GET /health`：进程存活检查
- `GET /ready`：模型加载就绪检查（建议生产流量前检查）

---

## 2. 环境变量说明（HiFi/流式相关）

以下变量均在服务启动时读取。

### 2.1 模型与设备

- `NANOVLLM_MODEL_PATH`
  - 含义：模型目录路径
  - 默认：`~/VoxCPM1.5`
- `NANOVLLM_SERVERPOOL_DEVICES`
  - 含义：GPU 设备号，逗号分隔（如 `0`、`0,1`）
  - 默认：`0`
- `NANOVLLM_SERVERPOOL_GPU_MEMORY_UTILIZATION`
  - 含义：显存利用率上限
  - 默认：`0.95`
  - 取值范围：`(0, 1]`

### 2.2 并发与调度

- `NANOVLLM_SERVERPOOL_MAX_NUM_SEQS`
  - 含义：最大并发序列数，建议不小于你的目标并发
  - 默认：`16`
- `NANOVLLM_SERVERPOOL_MAX_NUM_BATCHED_TOKENS`
  - 含义：批处理 token 上限，影响吞吐和显存压力
  - 默认：`8192`
- `NANOVLLM_SERVERPOOL_MAX_MODEL_LEN`
  - 含义：模型上下文长度上限
  - 默认：`4096`
- `NANOVLLM_SERVERPOOL_INFERENCE_TIMESTEPS`
  - 含义：推理时间步参数
  - 默认：`10`
- `NANOVLLM_QUEUE_COALESCE_MS`
  - 含义：队列合批等待时间（毫秒），影响吞吐/时延平衡
  - README 推荐值（4090 HiFi）：`5`
- `NANOVLLM_SERVERPOOL_ENFORCE_EAGER`
  - 含义：是否强制 eager
  - 默认：`false`

### 2.3 MP3 流式编码

- `NANOVLLM_MP3_BITRATE_KBPS`
  - 含义：MP3 码率
  - 默认：`192`
- `NANOVLLM_MP3_QUALITY`
  - 含义：LAME 编码质量档（`0..2`，`0` 质量更高但更慢）
  - 默认：`2`

### 2.4 预热（推荐）

- `NANOVLLM_WARMUP_ENABLED`
  - 含义：是否启用服务内 warmup
  - 默认：`true`
- `NANOVLLM_WARMUP_TEXT`
  - 含义：warmup 文本
  - 默认：`你好`
- `NANOVLLM_WARMUP_MAX_GENERATE_LENGTH`
  - 含义：warmup 最长生成长度
  - 默认：`128`
- `NANOVLLM_WARMUP_DELAY_SEC`
  - 含义：服务 ready 后延迟 warmup 秒数
  - 默认：`1.0`

---

## 3. 核心接口（HiFi 流式）

## 3.1 创建 HiFi 缓存

- 方法：`POST /add_hifi`
- 用途：把同一份参考音频缓存为 `prompt_id + reference_id` 组合，返回可复用 `hifi_id`

请求体：

```json
{
  "wav_base64": "<base64音频文件字节>",
  "wav_format": "wav",
  "prompt_text": "参考音频对应文本"
}
```

响应体：

```json
{
  "hifi_id": "string",
  "prompt_id": "string",
  "reference_id": "string",
  "feat_dim": 64,
  "sample_rate": 16000,
  "channels": 1
}
```

---

## 3.2 流式生成（实时 TTS）

- 方法：`POST /generate`
- 返回：`Content-Type: audio/mpeg`（流式 MP3 字节流）
- 关键响应头：
  - `X-Audio-Sample-Rate`
  - `X-Audio-Channels`

HiFi 推荐请求体（使用缓存）：

```json
{
  "target_text": "要合成的文本",
  "hifi_id": "<来自 /add_hifi>",
  "max_generate_length": 2000,
  "temperature": 1.0,
  "cfg_value": 2.0
}
```

字段说明：

- `target_text`：必填，当前要合成的文本
- `hifi_id`：推荐填，用于复用 HiFi 缓存
- `max_generate_length`：默认 `2000`
- `temperature`：默认 `1.0`
- `cfg_value`：默认 `2.0`

约束说明：

- prompt 相关字段互斥（`prompt_wav_*` / `prompt_latents_base64` / `prompt_id` / `hifi_id`）
- reference 相关字段互斥（`ref_audio_wav_*` / `ref_audio_latents_base64` / `ref_audio_id`）

---

## 3.3 删除 HiFi 缓存

- 方法：`DELETE /hifi/{hifi_id}`
- 用途：释放该 HiFi 对应缓存（包含底层 prompt/reference）

---

## 4. 推荐调用流程（实时系统）

1. 服务启动后检查 `GET /ready`
2. 业务会话初始化时调用一次 `POST /add_hifi`，拿到 `hifi_id`
3. 每次实时合成调用 `POST /generate`（仅传 `target_text + hifi_id`）
4. 连接断开/会话结束时调用 `DELETE /hifi/{hifi_id}`

建议：

- `hifi_id` 做会话级缓存，避免每条文本重复上传参考音频
- 并发增大时优先调 `NANOVLLM_SERVERPOOL_MAX_NUM_SEQS` 与 `NANOVLLM_QUEUE_COALESCE_MS`
- 用 `GET /metrics` 观察 TTFB、总耗时、编码时延

---

## 5. 错误码与排查

- `400`：参数错误（字段互斥冲突、base64 无效、格式缺失等）
- `404`：`hifi_id` 不存在（可能已删除或服务重启）
- `503`：模型服务未就绪
- `500`：服务内部错误（如配置异常）

常见问题：

- 首包慢：检查是否做了 warmup，检查 `QUEUE_COALESCE_MS`
- 高并发吞吐下降：可能已过最优并发点，关注 `MAX_NUM_SEQS` 与显存上限
- 音频格式问题：确认输入是完整文件字节的 base64，而不是 data URI

---

## 6. Python 示例（完整可运行）

示例功能：

- 上传参考音频创建 `hifi_id`
- 调用 `/generate` 流式接收 MP3 并写入文件
- 输出首包时间（TTFB）
- 最后删除 `hifi_id`

```python
import base64
import time
from pathlib import Path

import requests


BASE_URL = "http://127.0.0.1:8000"
REF_WAV = Path("/path/to/reference.wav")
PROMPT_TEXT = "参考音频对应文本"
TARGET_TEXT = "你好，这是接入实时 TTS 的一段测试文本。"
OUT_MP3 = Path("out_stream.mp3")


def add_hifi() -> str:
    wav_bytes = REF_WAV.read_bytes()
    payload = {
        "wav_base64": base64.b64encode(wav_bytes).decode("ascii"),
        "wav_format": "wav",
        "prompt_text": PROMPT_TEXT,
    }
    r = requests.post(f"{BASE_URL}/add_hifi", json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["hifi_id"]


def stream_generate(hifi_id: str) -> None:
    payload = {
        "target_text": TARGET_TEXT,
        "hifi_id": hifi_id,
        "max_generate_length": 2000,
        "temperature": 1.0,
        "cfg_value": 2.0,
    }
    t0 = time.perf_counter()
    first_chunk_at = None
    total_bytes = 0

    with requests.post(
        f"{BASE_URL}/generate",
        json=payload,
        stream=True,
        timeout=(10, 180),
    ) as r:
        r.raise_for_status()
        sample_rate = r.headers.get("X-Audio-Sample-Rate")
        channels = r.headers.get("X-Audio-Channels")
        print("sample_rate=", sample_rate, "channels=", channels)

        with OUT_MP3.open("wb") as f:
            for chunk in r.iter_content(chunk_size=16384):
                if not chunk:
                    continue
                if first_chunk_at is None:
                    first_chunk_at = time.perf_counter()
                    print(f"TTFB={first_chunk_at - t0:.3f}s")
                f.write(chunk)
                total_bytes += len(chunk)

    total = time.perf_counter() - t0
    print(f"done total={total:.3f}s bytes={total_bytes} out={OUT_MP3}")


def delete_hifi(hifi_id: str) -> None:
    requests.delete(f"{BASE_URL}/hifi/{hifi_id}", timeout=30)


def main() -> None:
    # 可选：服务可用性检查
    requests.get(f"{BASE_URL}/ready", timeout=5).raise_for_status()

    hifi_id = add_hifi()
    try:
        stream_generate(hifi_id)
    finally:
        delete_hifi(hifi_id)


if __name__ == "__main__":
    main()
```

---

## 7. 实时播放接入提示

服务端输出是 MP3 流。对实时播放有两种常见方式：

- 方式 A：客户端边收边写 MP3，交给播放器边解码边播（实现简单）
- 方式 B：客户端边收边转码为 PCM，再推给实时音频设备（控制更细，延迟优化空间更大）

如果你有 WebSocket 网关，可将 `/generate` 的 chunk 直接转发给前端播放器。
