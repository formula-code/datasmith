"""The stage 8 dry run must answer the question it is relied on to answer.

Publishing writes to DockerHub and stamps ``published_at``; there is no undo,
so the operating rule is "dry-run first, every time". That rule is only worth
following if the dry run reports the set the real run would publish.

It did not. The dry-run branch read ``candidate_containers`` whole -- no
window, no harbor evidence, no verification state -- and reported every
synthesized container ever built. On the corpus this branch was written
against it printed ``Items to process: 1875`` where the real run publishes a
handful. An operator following the rule exactly would have seen a number four
orders of magnitude too large and learned nothing about what was about to
become public.

So the dry run now goes through :func:`records_from_supabase`, the same
function the real run selects with.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from datasmith.update.pipeline import Pipeline


def _record(issue: int, container: str = "formulacode/o-r:1"):
    from datasmith.github.models import FormulaCodeRecord

    return FormulaCodeRecord(
        owner="o",
        repo="r",
        issue_number=issue,
        task_id=issue,
        gt_hash="sha" + str(issue),
        base_commit="base",
        date="2024-01-01T00:00:00Z",
        instructions="",
        classification="",
        difficulty="",
        container_name=container,
        patch="",
    )


@pytest.mark.asyncio
class TestPublishDryRunReportsWhatWouldPublish:
    async def test_it_selects_through_records_from_supabase(self) -> None:
        pipeline = Pipeline(dry_run=True)
        with patch("datasmith.publish.records.records_from_supabase") as mock_records:
            mock_records.return_value = [_record(1), _record(2)]
            with patch.object(pipeline, "_log_dry_run_summary") as mock_log:
                await pipeline._publish("2024-01-01", "2024-12-31")

        mock_records.assert_called_once()
        assert mock_records.call_args[1]["start_date"] == "2024-01-01"
        assert mock_records.call_args[1]["end_date"] == "2024-12-31"
        items = mock_log.call_args[0][1]
        assert len(items) == 2

    async def test_it_does_not_read_candidate_containers_wholesale(self) -> None:
        """The old branch's read is the bug; assert it is gone.

        ``fetch_all("candidate_containers")`` with no filters returns every row
        in the table. Nothing in the dry-run path should need that.
        """
        pipeline = Pipeline(dry_run=True)
        with patch("datasmith.publish.records.records_from_supabase", return_value=[]):
            with patch("datasmith.update.pipeline.fetch_all") as mock_fetch:
                with patch.object(pipeline, "_log_dry_run_summary"):
                    await pipeline._publish("2024-01-01", "2024-12-31")
        mock_fetch.assert_not_called()

    async def test_it_names_the_tags_and_the_gate(self) -> None:
        """An operator about to overwrite a public tag needs to see which tags.

        A count is not enough: the whole risk of this stage is that a push is
        silent from a consumer's point of view.
        """
        pipeline = Pipeline(dry_run=True)
        with patch("datasmith.publish.records.records_from_supabase") as mock_records:
            mock_records.return_value = [_record(7, "formulacode/o-r:7")]
            with patch.object(pipeline, "_log_dry_run_summary") as mock_log:
                await pipeline._publish("2024-01-01", "2024-12-31")

        extra = mock_log.call_args[1]["extra"]
        blob = " ".join(f"{k}: {v}" for k, v in extra.items())
        assert "formulacode/o-r:7" in blob
        assert "1.05" in blob
        assert "daytona" in blob

    async def test_a_dry_run_never_publishes(self) -> None:
        pipeline = Pipeline(dry_run=True)
        with patch("datasmith.publish.records.records_from_supabase", return_value=[_record(1)]):
            with patch("datasmith.publish.pipeline.publish_pipeline") as mock_publish:
                with patch.object(pipeline, "_log_dry_run_summary"):
                    await pipeline._publish("2024-01-01", "2024-12-31")
        mock_publish.assert_not_called()
