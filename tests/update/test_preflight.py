from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.preflight import run_preflight


class TestPreflight:
    def test_all_checks_pass(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
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
            patch("httpx.get") as mock_httpx,
        ):
            mock_httpx.return_value = MagicMock(status_code=200, json=lambda: {"rate": {"remaining": 5000}})
            result = run_preflight()

        assert result is True

    def test_missing_supabase_url_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "ghp_test1")
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        monkeypatch.setenv("HF_TOKEN_PATH", str(token_file))

        with (
            patch("datasmith.utils.db.get_client", side_effect=ValueError("SUPABASE_URL and SUPABASE_KEY must be set")),
            patch("python_on_whales.DockerClient", return_value=MagicMock()),
            patch("httpx.get") as mock_httpx,
        ):
            mock_httpx.return_value = MagicMock(status_code=200, json=lambda: {"rate": {"remaining": 5000}})
            result = run_preflight()

        assert result is False

    def test_missing_gh_tokens_fails(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setenv("GH_TOKENS", "")
        monkeypatch.delenv("GH_TOKEN", raising=False)
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
            patch("httpx.get"),
        ):
            result = run_preflight()

        assert result is False
