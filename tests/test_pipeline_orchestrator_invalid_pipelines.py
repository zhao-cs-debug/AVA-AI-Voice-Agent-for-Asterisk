import pytest

from src.config import AppConfig
from src.pipelines.orchestrator import PipelineOrchestrator, PipelineUnavailableError


def _build_app_config_with_one_invalid_pipeline() -> AppConfig:
    providers = {"openai": {"api_key": "test-key"}}
    pipelines = {
        "openai_stack": {
            "stt": "openai_stt",
            "llm": "openai_llm",
            "tts": "openai_tts",
        },
        # Missing GOOGLE_API_KEY by design; should be treated as invalid rather than
        # silently resolved to placeholder adapters.
        "google_stack": {
            "stt": "google_stt",
            "llm": "google_llm",
            "tts": "google_tts",
        },
    }
    return AppConfig(
        default_provider="openai",
        providers=providers,
        asterisk={"host": "127.0.0.1", "username": "ari", "password": "secret"},
        llm={"initial_greeting": "hi", "prompt": "prompt"},
        audio_transport="audiosocket",
        downstream_mode="stream",
        pipelines=pipelines,
        active_pipeline="openai_stack",
    )


@pytest.mark.asyncio
async def test_orchestrator_skips_invalid_pipelines_and_keeps_valid_ones(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    app_config = _build_app_config_with_one_invalid_pipeline()
    orchestrator = PipelineOrchestrator(app_config)
    await orchestrator.start()

    assert orchestrator.started
    assert "google_stack" in orchestrator._invalid_pipelines

    with pytest.raises(PipelineUnavailableError) as error:
        orchestrator.get_pipeline("call-1", "google_stack")

    assert error.value.pipeline_name == "google_stack"
    assert "unavailable" in str(error.value).lower()


@pytest.mark.asyncio
async def test_orchestrator_uses_default_only_when_pipeline_is_unspecified(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    orchestrator = PipelineOrchestrator(_build_app_config_with_one_invalid_pipeline())
    await orchestrator.start()

    resolution = orchestrator.get_pipeline("call-default")

    assert resolution is not None
    assert resolution.pipeline_name == "openai_stack"


@pytest.mark.asyncio
async def test_missing_explicit_pipeline_never_falls_back(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    orchestrator = PipelineOrchestrator(_build_app_config_with_one_invalid_pipeline())
    await orchestrator.start()

    with pytest.raises(PipelineUnavailableError) as error:
        orchestrator.get_pipeline("call-missing", "does-not-exist")

    assert error.value.pipeline_name == "does-not-exist"

