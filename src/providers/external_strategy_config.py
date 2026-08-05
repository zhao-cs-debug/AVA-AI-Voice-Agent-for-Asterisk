from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ExternalStrategyAgentConfig:
    """Configuration for the full-duplex external strategy voice agent."""

    base_url: str = "https://demo.shiwentech.com"
    api_key: str = ""
    api_key_file: str = ""
    api_key_env: str = "STRATEGY_NETWORK_API_KEY"
    type: str = "external_strategy_agent"
    enabled: bool = True
    capabilities: List[str] = field(default_factory=lambda: ["stt", "llm", "tts"])

    input_encoding: str = "linear16"
    input_sample_rate_hz: int = 16000
    provider_input_sample_rate_hz: int = 16000
    output_encoding: str = "linear16"
    output_sample_rate_hz: int = 24000
    audio_chunk_ms: int = 200

    connect_timeout_sec: float = 15.0
    request_timeout_sec: float = 30.0
    settings_timeout_sec: float = 600.0
    session_start_timeout_sec: float = 120.0
    close_timeout_sec: float = 2.0
    max_message_bytes: int = 2 * 1024 * 1024
    continuous_input: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExternalStrategyAgentConfig":
        values = dict(data or {})
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{key: value for key, value in values.items() if key in allowed})
