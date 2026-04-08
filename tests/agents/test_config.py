"""Tests for datasmith.agents.config — AgentConfig and configure_dspy."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from datasmith.agents.config import AgentConfig, configure_dspy


class TestAgentConfigFromEnv:
    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DSPY_MODEL", raising=False)
        monkeypatch.delenv("DSPY_FALLBACK_MODEL", raising=False)
        monkeypatch.delenv("DSPY_API_KEY", raising=False)
        monkeypatch.delenv("DSPY_API_BASE", raising=False)
        monkeypatch.delenv("DSPY_MAX_TOKENS", raising=False)
        monkeypatch.delenv("DSPY_TEMPERATURE", raising=False)
        monkeypatch.delenv("PORTKEY_API_KEY", raising=False)
        monkeypatch.delenv("PORTKEY_MODEL_NAME", raising=False)

        config = AgentConfig.from_env()
        assert config.primary_model == "openai/gpt-oss-120b"
        assert config.fallback_model == ""
        assert config.api_key == "local"
        assert config.api_base == "http://localhost:30001/v1"
        assert config.max_tokens == 16000
        assert config.temperature == 0.0
        assert config.portkey_api_key == ""
        assert config.portkey_model_name == ""

    def test_reads_dspy_temperature(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DSPY_TEMPERATURE", "0.7")
        config = AgentConfig.from_env()
        assert config.temperature == 0.7

    def test_reads_portkey_model_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PORTKEY_MODEL_NAME", "@openai/gpt-4o")
        config = AgentConfig.from_env()
        assert config.portkey_model_name == "@openai/gpt-4o"


class TestConfigureDspy:
    def test_direct_provider(self) -> None:
        mock_lm = MagicMock()
        mock_dspy = MagicMock()
        mock_dspy.LM.return_value = mock_lm

        config = AgentConfig(
            primary_model="openai/gpt-4o",
            api_key="sk-test",
            api_base="http://localhost:8000/v1",
            temperature=0.5,
            max_tokens=4096,
        )

        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            configure_dspy(config)

        mock_dspy.LM.assert_called_once_with(
            model="openai/gpt-4o",
            api_key="sk-test",
            api_base="http://localhost:8000/v1",
            temperature=0.5,
            max_tokens=4096,
        )
        mock_dspy.configure.assert_called_once_with(lm=mock_lm)

    def test_portkey_path(self) -> None:
        mock_lm = MagicMock()
        mock_dspy = MagicMock()
        mock_dspy.LM.return_value = mock_lm
        mock_portkey = MagicMock()
        mock_portkey.PORTKEY_GATEWAY_URL = "https://api.portkey.ai/v1"

        config = AgentConfig(
            portkey_api_key="pk-test",
            portkey_model_name="@anthropic/claude-3-5-sonnet-latest",
            temperature=0.3,
            max_tokens=8000,
        )

        with patch.dict("sys.modules", {"dspy": mock_dspy, "portkey_ai": mock_portkey}):
            configure_dspy(config)

        mock_dspy.LM.assert_called_once_with(
            model="@anthropic/claude-3-5-sonnet-latest",
            temperature=0.3,
            max_tokens=8000,
            api_base="https://api.portkey.ai/v1",
            api_key="unused-by-portkey",
            headers={
                "x-portkey-api-key": "pk-test",
                "x-portkey-provider": "anthropic",
            },
            custom_llm_provider="openai",
        )
        mock_dspy.configure.assert_called_once_with(lm=mock_lm)

    def test_direct_takes_priority_over_portkey(self) -> None:
        mock_lm = MagicMock()
        mock_dspy = MagicMock()
        mock_dspy.LM.return_value = mock_lm

        config = AgentConfig(
            primary_model="openai/gpt-4o",
            api_key="sk-direct",
            api_base="http://localhost:8000/v1",
            portkey_api_key="pk-test",
            portkey_model_name="@openai/gpt-4o",
        )

        with patch.dict("sys.modules", {"dspy": mock_dspy}):
            configure_dspy(config)

        # Should use direct provider, not portkey
        call_kwargs = mock_dspy.LM.call_args
        assert call_kwargs[1]["model"] == "openai/gpt-4o"
        assert call_kwargs[1]["api_base"] == "http://localhost:8000/v1"
        assert call_kwargs[1]["api_key"] == "sk-direct"

    def test_portkey_default_model(self) -> None:
        mock_lm = MagicMock()
        mock_dspy = MagicMock()
        mock_dspy.LM.return_value = mock_lm
        mock_portkey = MagicMock()
        mock_portkey.PORTKEY_GATEWAY_URL = "https://api.portkey.ai/v1"

        config = AgentConfig(
            portkey_api_key="pk-test",
            # portkey_model_name left empty -> should default
        )

        with patch.dict("sys.modules", {"dspy": mock_dspy, "portkey_ai": mock_portkey}):
            configure_dspy(config)

        call_kwargs = mock_dspy.LM.call_args
        assert call_kwargs[1]["model"] == "@anthropic/claude-3-5-sonnet-latest"

    def test_no_backend_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        config = AgentConfig(api_key="", primary_model="", portkey_api_key="")

        mock_dspy = MagicMock()
        with (
            patch.dict("sys.modules", {"dspy": mock_dspy}),
            caplog.at_level(logging.WARNING, logger="datasmith.agents.config"),
        ):
            configure_dspy(config)

        mock_dspy.LM.assert_not_called()
        mock_dspy.configure.assert_not_called()
        assert "No LM backend configured" in caplog.text
