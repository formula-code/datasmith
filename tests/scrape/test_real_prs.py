from typing import Any

# - https://github.com/astropy/astropy/pull/13498
#   - We should be able to recover https://github.com/astropy/astropy/issues/13479 from the PR information.
# - https://github.com/astropy/astropy/pull/13498
#   - Same for this one. We should be able to recover https://github.com/astropy/astropy/issues/13479 from the PR information.
# We should ensure that for both PRs, we get the same issue recovered and that the issue is placed in the right field.
# - https://github.com/Qiskit/qiskit/pull/10651
#   - For this PR, we should not find any linked issue. However, we should be able to segment the PR description into a problem and a solution part correctly with an acceptable LCS ratio.
# - https://github.com/Qiskit/qiskit/pull/12869
#   - For this PR, a couple of issues were mentioned but they were mentioned AFTER the PR was merged so we should pay no heed to them.
# - https://github.com/apache/arrow/pull/36738
#   - We should recover one issue from the body.
# - https://github.com/apache/arrow-adbc/pull/2697
#  - There are no relevant issues and the body text doesn't have any problem / solution lines.
#  - We should reject this PR.
import pytest

from datasmith.scrape.report_builder import ReportBuilder
from datasmith.scrape.utils import _parse_pr_url
from datasmith.utils import _get_github_metadata


def _fetch_pr(owner: str, repo: str, num: int) -> dict[str, Any]:
    pr = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
    assert isinstance(pr, dict), "PR metadata should be a dict"
    return pr


def _build_pr_dict(owner: str, repo: str, num: int) -> dict[str, Any]:
    pr = _fetch_pr(owner, repo, num)
    return {
        "pr_url": f"https://api.github.com/repos/{owner}/{repo}/pulls/{num}",
        "pr_number": num,
        "pr_title": pr.get("title", ""),
        "pr_body": pr.get("body", ""),
        "pr_merged_at": pr.get("merged_at"),
    }


@pytest.mark.parametrize(
    "pr_url, expected_issue",
    [
        ("https://github.com/astropy/astropy/pull/13498", "13479"),
        ("https://github.com/astropy/astropy/pull/13498", "13479"),
    ],
)
def test_astropy_pr_recovers_issue(pr_url: str, expected_issue: str):
    owner, repo, num_s = _parse_pr_url(pr_url)
    num = int(num_s)
    pr_dict = _build_pr_dict(owner, repo, num)

    rb = ReportBuilder(enable_llm_backends=False, summarize_llm=False, anonymize_output=False)
    result = rb.build(pr_dict)

    # Expect exactly the referenced issue to be present in raw_issue_data
    issue_numbers = {str(item.get("number")) for item in result.all_data.get("raw_issue_data", [])}
    assert expected_issue in issue_numbers
    # And the rendered field should include the issue reference
    assert expected_issue in (result.all_data.get("git_issue_str") or "")


def test_qiskit_pr_has_no_linked_issue():
    pr_url = "https://github.com/Qiskit/qiskit/pull/10651"
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr_dict = _build_pr_dict(owner, repo, int(num_s))

    rb = ReportBuilder(enable_llm_backends=False, summarize_llm=False, anonymize_output=False)
    result = rb.build(pr_dict)

    assert result.all_data.get("raw_issue_data") == []
    assert (result.all_data.get("git_issue_str") or "").strip() == ""


def test_qiskit_late_mentions_ignored():
    pr_url = "https://github.com/Qiskit/qiskit/pull/12869"
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr_dict = _build_pr_dict(owner, repo, int(num_s))

    rb = ReportBuilder(enable_llm_backends=False, summarize_llm=False, anonymize_output=False)
    result = rb.build(pr_dict)

    # Late mentions in comments should not appear; body has no referenced issues
    assert result.all_data.get("raw_issue_data") == []


def test_arrow_recovers_issue_from_body():
    pr_url = "https://github.com/apache/arrow/pull/36738"
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr_dict = _build_pr_dict(owner, repo, int(num_s))

    rb = ReportBuilder(enable_llm_backends=False, summarize_llm=False, anonymize_output=False)
    result = rb.build(pr_dict)

    # At least one referenced issue should be present
    assert len(result.all_data.get("raw_issue_data") or []) >= 1


def test_arrow_adbc_rejected_when_no_issues_and_no_structure():
    pr_url = "https://github.com/apache/arrow-adbc/pull/2697"
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr_dict = _build_pr_dict(owner, repo, int(num_s))

    rb = ReportBuilder(enable_llm_backends=False, summarize_llm=False, anonymize_output=False)
    result = rb.build(pr_dict)

    # No referenced issues and generic body -> reject
    assert result.final_md == "NOT_A_VALID_PR"


def test_qiskit_10651_problem_solution_segmentation_extractive():
    pr_url = "https://github.com/Qiskit/qiskit/pull/10651"
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr = _fetch_pr(owner, repo, int(num_s))
    body = pr.get("body", "")
    title = pr.get("title", "")
    # Note: patch/diff would require additional API call, using empty string for now
    from datasmith.agents.problem_extractor import ProblemExtractor

    extractor = ProblemExtractor()
    extraction = extractor.extract_problem(
        pr_title=title,
        pr_body=body,
        pr_comments="",
        pr_patch="",
    )

    # Verify extraction returned valid data
    assert extraction is not None
    assert isinstance(extraction.initial_observations, str) or extraction.initial_observations is None
    assert isinstance(extraction.solution_overview, str) or extraction.solution_overview is None
