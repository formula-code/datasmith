from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from datasmith.github.models import FormulaCodeRecord
from datasmith.publish.huggingface import HuggingFacePublisher


def _make_record(**kwargs):
    defaults = {
        "owner": "test-org",
        "repo": "test-repo",
        "issue_number": 42,
        "task_id": "test-org__test-repo-42",
        "gt_hash": "abc123",
    }
    defaults.update(kwargs)
    return FormulaCodeRecord(**defaults)


class TestHuggingFacePublisher:
    def test_reads_token_from_file(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("hf_test_token_123")
        pub = HuggingFacePublisher(token_path=str(token_file))
        assert pub._get_token() == "hf_test_token_123"

    def test_missing_token_raises(self, tmp_path):
        pub = HuggingFacePublisher(token_path=str(tmp_path / "nonexistent"))
        with patch.dict(os.environ, {}, clear=False):
            # Remove HF_TOKEN if present
            os.environ.pop("HF_TOKEN", None)
            with pytest.raises(ValueError, match="token not found"):
                pub._get_token()

    def test_publish_calls_upload(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")
        records = [_make_record()]

        with patch("huggingface_hub.HfApi") as MockApi:
            mock_api = MagicMock()
            MockApi.return_value = mock_api

            pub = HuggingFacePublisher(token_path=str(token_file))
            pub.publish(records, "formulacode@2024-01")

            mock_api.upload_file.assert_called_once()
            call_kwargs = mock_api.upload_file.call_args
            assert "data/formulacode@2024-01.parquet" in str(call_kwargs)

    def test_publish_empty_records(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("hf_test")

        pub = HuggingFacePublisher(token_path=str(token_file))
        # Should not raise, just warn
        pub.publish([], "formulacode@2024-01")

    def test_dataset_card_generated(self):
        pub = HuggingFacePublisher()
        card = pub.create_dataset_card("formulacode@2024-06")
        assert "formulacode@2024-06" in card
        assert "task_id" in card
        assert "Schema" in card
        assert "apache-2.0" in card
