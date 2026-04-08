from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.preflight import run_preflight


def _mock_httpx_request(url: str, **kwargs):
    """Build consistent mock httpx responses for both GET and POST."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    if "/models" in url:
        mock_resp.json.return_value = {"data": [{"id": "gpt-oss-120b"}]}
    elif "/chat/completions" in url:
        mock_resp.json.return_value = {"choices": [{"message": {"content": "OK"}, "finish_reason": "stop"}]}
    elif "rate_limit" in url:
        mock_resp.json.return_value = {"rate": {"remaining": 5000}}
    return mock_resp


class TestPreflight:
    def test_all_checks_pass(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        monkeypatch.setenv("DSPY_MODEL", "openai/gpt-oss-120b")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        mock_docker = MagicMock()

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=mock_docker),
            patch("httpx.get", side_effect=_mock_httpx_request),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is True

    def test_missing_supabase_url_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        monkeypatch.setenv("DSPY_MODEL", "openai/gpt-oss-120b")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        with (
            patch("datasmith.utils.db.get_client", side_effect=ValueError("SUPABASE_URL and SUPABASE_KEY must be set")),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_mock_httpx_request),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is False

    def test_missing_gh_tokens_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "")
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setenv("DSPY_MODEL", "openai/gpt-oss-120b")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_mock_httpx_request),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is False

    def test_missing_dspy_model_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        monkeypatch.setenv("DSPY_MODEL", "")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_mock_httpx_request),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is False

    def test_llm_server_unreachable_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        monkeypatch.setenv("DSPY_MODEL", "openai/gpt-oss-120b")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:99999/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        import httpx

        def _mock_get(url, **kwargs):
            if "rate_limit" in url:
                return _mock_httpx_request(url, **kwargs)
            raise httpx.ConnectError("Connection refused")

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_mock_get),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is False

    def test_multiple_gh_tokens_sums_remaining(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1,ghp_test2,ghp_test3")
        monkeypatch.setenv("DSPY_MODEL", "openai/gpt-oss-120b")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        rate_limit_calls = []

        def _side_effect(url, **kwargs):
            resp = MagicMock()
            resp.status_code = 200
            if "/models" in url:
                resp.json.return_value = {"data": [{"id": "gpt-oss-120b"}]}
            elif "rate_limit" in url:
                rate_limit_calls.append(kwargs.get("headers", {}))
                resp.json.return_value = {"rate": {"remaining": 1000}}
            else:
                resp.json.return_value = {}
            return resp

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_side_effect),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is True
        assert len(rate_limit_calls) == 3

    def test_model_not_found_on_server_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        monkeypatch.setenv("DSPY_MODEL", "openai/nonexistent-model")
        monkeypatch.setenv("DSPY_API_BASE", "http://localhost:30001/v1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.select.return_value = mock_table
        mock_table.limit.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        with (
            patch("datasmith.utils.db.get_client", return_value=mock_client),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get", side_effect=_mock_httpx_request),
            patch("httpx.post", side_effect=_mock_httpx_request),
        ):
            result = run_preflight()

        assert result is False
