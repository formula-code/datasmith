"""The grind planner's diversity bounds.

The goal counts at most 10 verified rows per (owner, repo) across at least 10
repos, so a batch that spends itself on one repo buys nothing after the tenth
row. Two mechanisms enforce that, and both are tested here: the planner's own
per-repo take, and the `--tasks-per-repo` flag it hands to `fc-data`, which
re-applies the bound by RANDOM sample rather than in database order.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "scripts"))

import grind  # noqa: E402


class TestThePlannerSpreadsAcrossRepos:
    def _ledger(self):
        return {}

    def test_no_repo_exceeds_the_per_repo_take(self, monkeypatch) -> None:
        monkeypatch.setattr(grind, "candidates", lambda o, r, want, ledger: list(range(1, want + 1)))
        rotation = (("a", "one"), ("b", "two"), ("c", "three"))
        plan = grind.plan_batch(rotation, {}, self._ledger(), batch=24)
        per_repo: dict[str, int] = {}
        for task in plan:
            repo = task.split("#")[0]
            per_repo[repo] = per_repo.get(repo, 0) + 1
        assert per_repo, "planned nothing"
        assert max(per_repo.values()) <= grind.DATASMITH_GRIND_TASKS_PER_REPO

    def test_a_repo_already_at_the_cap_is_skipped(self, monkeypatch) -> None:
        """Verified rows past 10 count for nothing; do not spend builds on them."""
        monkeypatch.setattr(grind, "candidates", lambda o, r, want, ledger: list(range(1, want + 1)))
        rotation = (("a", "one"), ("b", "two"))
        counts = {("a", "one"): grind.DATASMITH_GRIND_REPO_CAP}
        plan = grind.plan_batch(rotation, counts, self._ledger(), batch=24)
        assert all(not t.startswith("a/one#") for t in plan), "a capped repo must not be planned"
        assert any(t.startswith("b/two#") for t in plan)

    def test_the_repo_furthest_below_the_cap_is_served_first(self, monkeypatch) -> None:
        monkeypatch.setattr(grind, "candidates", lambda o, r, want, ledger: list(range(1, want + 1)))
        rotation = (("a", "one"), ("b", "two"))
        counts = {("a", "one"): 5, ("b", "two"): 0}
        plan = grind.plan_batch(rotation, counts, self._ledger(), batch=3)
        assert plan[0].startswith("b/two#"), "the emptiest repo goes first"


class TestTheBatchCommandCarriesTheDiversityBound:
    """`--tasks-per-repo` is what makes the selection random rather than
    database-ordered, so a repo's retries stop landing on the same few PRs."""

    def test_the_flag_is_passed_to_fc_data(self, monkeypatch, tmp_path) -> None:
        captured: dict = {}

        class _Result:
            returncode = 0

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return _Result()

        monkeypatch.setattr(grind.subprocess, "run", fake_run)
        grind.run_batch(["o/r#1"], tmp_path / "batch.log")
        cmd = captured["cmd"]
        assert "--tasks-per-repo" in cmd
        assert cmd[cmd.index("--tasks-per-repo") + 1] == str(grind.DATASMITH_GRIND_TASKS_PER_REPO)

    def test_neighbour_enqueueing_stays_off(self, monkeypatch, tmp_path) -> None:
        """Neighbours bypass both the planner and the flag, and over-produce in
        whichever repo happened to succeed last."""
        captured: dict = {}

        class _Result:
            returncode = 0

        def fake_run(cmd, **kwargs):
            captured["env"] = kwargs.get("env", {})
            return _Result()

        monkeypatch.setattr(grind.subprocess, "run", fake_run)
        grind.run_batch(["o/r#1"], tmp_path / "batch.log")
        assert captured["env"].get("DATASMITH_NEIGHBOR_CAP") == "0"


class TestTheBatchFavoursReposThatHaveProvenTheyYield:
    """Serving the emptiest repo first is right until it isn't.

    It got the corpus to 8 distinct repos quickly. Then, measured over the
    night of 2026-08-25/26, a batch drawn from the still-empty repos returned
    0 accepts and 12 failures in two and a half hours — each failure able to
    burn an hour, because a verification timeout is 3600 s and the identical
    timeout signature repeats until the stall detector fires. Meanwhile 8
    proven repos sat at 1-2 rows of their allowed 10.
    """

    @staticmethod
    def _stub(monkeypatch):
        monkeypatch.setattr(grind, "candidates", lambda o, r, want, ledger: list(range(1, want + 1)))

    def _split(self, plan, proven_names):
        p = sum(1 for t in plan if t.split("#")[0] in proven_names)
        return p, len(plan) - p

    def test_most_of_the_batch_goes_to_proven_repos(self, monkeypatch) -> None:
        """Sized realistically: with 3 tasks per repo, N proven repos can absorb
        at most 3N of a batch, so the proven set has to be big enough to fill
        its share before this comparison means anything."""
        self._stub(monkeypatch)
        proven_repos = [(f"p{i}", "r") for i in range(6)]
        unproven_repos = [(f"u{i}", "r") for i in range(6)]
        rotation = tuple(proven_repos + unproven_repos)
        counts = dict.fromkeys(proven_repos, 2)
        plan = grind.plan_batch(rotation, counts, {}, batch=24)
        proven, unproven = self._split(plan, {f"{o}/{r}" for o, r in proven_repos})
        assert proven > unproven, f"expected the proven set to dominate, got {proven} vs {unproven}"

    def test_a_slice_is_still_reserved_for_unproven_repos(self, monkeypatch) -> None:
        """Depth-only would never find the 9th and 10th repo the goal requires,
        and would collapse into the monoculture the per-repo cap exists to stop."""
        self._stub(monkeypatch)
        rotation = (("a", "one"), ("b", "two"), ("c", "three"))
        counts = {("a", "one"): 2, ("b", "two"): 3}
        plan = grind.plan_batch(rotation, counts, {}, batch=24)
        assert any(t.startswith("c/three#") for t in plan), "exploration must not stop"

    def test_with_nothing_proven_the_whole_batch_explores(self, monkeypatch) -> None:
        """Every repo is fair game when none has yielded — bounded, as always,
        by the per-repo cap, so 3 repos yield 9 tasks and not 12."""
        self._stub(monkeypatch)
        rotation = (("a", "one"), ("b", "two"), ("c", "three"))
        plan = grind.plan_batch(rotation, {}, {}, batch=12)
        assert len(plan) == 3 * grind.DATASMITH_GRIND_TASKS_PER_REPO
        assert len({t.split("#")[0] for t in plan}) == 3, "spread while nothing has proven itself"

    def test_a_capped_repo_is_never_planned(self, monkeypatch) -> None:
        self._stub(monkeypatch)
        rotation = (("a", "one"), ("b", "two"))
        counts = {("a", "one"): grind.DATASMITH_GRIND_REPO_CAP, ("b", "two"): 1}
        plan = grind.plan_batch(rotation, counts, {}, batch=24)
        assert all(not t.startswith("a/one#") for t in plan), "rows past the cap count for nothing"

    def test_the_per_repo_cap_still_bounds_each_repo(self, monkeypatch) -> None:
        self._stub(monkeypatch)
        rotation = (("a", "one"), ("b", "two"), ("c", "three"))
        counts = {("a", "one"): 1, ("b", "two"): 1}
        plan = grind.plan_batch(rotation, counts, {}, batch=24)
        per: dict[str, int] = {}
        for t in plan:
            per[t.split("#")[0]] = per.get(t.split("#")[0], 0) + 1
        assert max(per.values()) <= grind.DATASMITH_GRIND_TASKS_PER_REPO
