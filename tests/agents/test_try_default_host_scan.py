"""The host image scan must gate the TRY_DEFAULT path, not only PRODUCE_VERIFY.

`agents/synthesizer.py` used to return from TRY_DEFAULT the moment the stock
template built, so the host scan and the verifier were only reached on the
repair path. That is backwards: a repository the stock template builds first
time is the COMMON case, so the gate covered the minority of containers.

Measured, not inferred: networkx#8148 was rebuilt through TRY_DEFAULT on
2026-08-24, sealed a manifest recording 140 benchmarks, and was never scanned.
"""

from __future__ import annotations

from datasmith.agents.reflexive.image_integrity import ImageIntegrity, IntegrityFinding
from datasmith.agents.synthesizer import _host_scan_findings


class TestHostScanFindings:
    def test_a_clean_image_yields_no_findings(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "datasmith.agents.reflexive.image_integrity.collect_and_evaluate",
            lambda image: ImageIntegrity(image=image, collected=True, facts={}),
        )
        assert _host_scan_findings("img:1") == []

    def test_a_tampered_image_yields_findings(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "datasmith.agents.reflexive.image_integrity.collect_and_evaluate",
            lambda image: ImageIntegrity(
                image=image,
                collected=True,
                facts={},
                findings=(IntegrityFinding("tamper_audit", "sitecustomize.py on sys.path"),),
            ),
        )
        out = _host_scan_findings("img:1")
        assert out and "tamper_audit" in out[0]

    def test_an_image_that_cannot_be_scanned_is_not_clean(self, monkeypatch) -> None:
        """ "We could not look" must never read the same as "it was fine"."""

        def boom(image: str):
            raise RuntimeError("docker daemon went away")

        monkeypatch.setattr("datasmith.agents.reflexive.image_integrity.collect_and_evaluate", boom)
        out = _host_scan_findings("img:1")
        assert out and "image_scan_failed" in out[0]

    def test_a_build_that_names_no_image_is_not_clean(self) -> None:
        """verify_context can report success and still extract no tag."""
        assert _host_scan_findings(None) != []
        assert _host_scan_findings("") != []


class TestTheTryDefaultPathConsultsIt:
    def test_try_default_success_is_gated_on_the_scan(self) -> None:
        """The call must sit between the build succeeding and _save_context.

        A behavioural test needs a real 8 GB build, so this asserts the
        ordering at the one place it can be got wrong: saving before scanning
        would admit the container and then complain about it.
        """
        import inspect

        from datasmith.agents.synthesizer import Synthesizer

        source = inspect.getsource(Synthesizer.run)
        assert "_host_scan_findings" in source, "TRY_DEFAULT must consult the host scan"
        scan_at = source.index("_host_scan_findings(result.image_tag)")
        save_at = source.index("self._save_context(", scan_at)
        assert scan_at < save_at, "the scan must gate the save, not follow it"


class TestVerifiedIsOnlyEverSetByCodeHoldingAllFourFacts:
    """`verified` is a claim, so the bar for making it is structural.

    Migration 00029 defaults every row to 'unverified', so an omission fails
    closed. These tests pin the two ways it could wrongly become 'verified':
    without a sealed manifest, and without the verifier having accepted.
    """

    @staticmethod
    def _synth():
        from datasmith.agents.synthesizer import Synthesizer

        return Synthesizer(agent="codex")

    def _captured_row(self, monkeypatch, **kwargs) -> dict:
        from datasmith.docker.context import DockerContext

        captured: dict = {}

        class _Tbl:
            def upsert(self, row):
                captured.update(row)
                return self

            def execute(self):
                return None

        monkeypatch.setattr(
            "datasmith.agents.synthesizer.get_client", lambda: type("C", (), {"table": lambda s, n: _Tbl()})()
        )
        monkeypatch.setattr(
            "datasmith.agents.synthesizer.evaluate_invariants", lambda m: type("R", (), {"warnings": []})()
        )
        self._synth()._save_context("o", "r", "s" * 40, 1, DockerContext(), **kwargs)
        return captured

    def test_a_verified_container_records_state_and_timestamp(self, monkeypatch) -> None:
        row = self._captured_row(monkeypatch, build_manifest={"build": {}}, verified=True)
        assert row["verification_state"] == "verified"
        assert row.get("verified_at")

    def test_verified_without_a_manifest_is_refused(self, monkeypatch) -> None:
        """All four facts, or none. A manifest is one of them."""
        row = self._captured_row(monkeypatch, build_manifest=None, verified=True)
        assert "verification_state" not in row
        assert "verified_at" not in row

    def test_the_default_is_to_claim_nothing(self, monkeypatch) -> None:
        row = self._captured_row(monkeypatch, build_manifest={"build": {}})
        assert "verification_state" not in row, "silence must leave the migration default in place"


class TestTheVerifierGatesTheDefaultPathToo:
    def test_no_agent_means_not_verified(self, monkeypatch) -> None:
        """Absence of evidence is not evidence.

        Asserting only the False return passes even with the guard deleted,
        because a verifier that runs and fails also returns False -- and it
        spends a real agent call and a real battery doing it. So assert the
        guard SHORT-CIRCUITS: nothing may be spawned at all.
        """
        from datasmith.agents.synthesizer import Synthesizer

        called: list[object] = []
        monkeypatch.setattr("datasmith.agents.reflexive.loop.DATASMITH_PV_ENABLED", True)
        monkeypatch.setattr("datasmith.agents.installed.base.get_agent", lambda n: called.append(n))
        assert Synthesizer(agent="none")._verify_built_image("o", "r", 1, "img:1") is False
        assert called == [], "the guard must short-circuit before any agent is spawned"

    def test_no_image_means_not_verified(self, monkeypatch) -> None:
        from datasmith.agents.synthesizer import Synthesizer

        called: list[object] = []
        monkeypatch.setattr("datasmith.agents.reflexive.loop.DATASMITH_PV_ENABLED", True)
        monkeypatch.setattr("datasmith.agents.installed.base.get_agent", lambda n: called.append(n))
        assert Synthesizer(agent="codex")._verify_built_image("o", "r", 1, None) is False
        assert called == [], "no image means nothing to verify; do not spawn an agent"

    def test_a_raising_verifier_means_not_verified(self, monkeypatch) -> None:
        from datasmith.agents.synthesizer import Synthesizer

        monkeypatch.setattr("datasmith.agents.reflexive.loop.DATASMITH_PV_ENABLED", True)
        monkeypatch.setattr(
            "datasmith.agents.installed.base.get_agent", lambda n: (_ for _ in ()).throw(RuntimeError("no agent"))
        )
        assert Synthesizer(agent="codex")._verify_built_image("o", "r", 1, "img:1") is False


class TestTheCallSitePassesTheVerdictThrough:
    """`verified=accepted`, not `verified=True`.

    Testing `_save_context` and `_verify_built_image` separately leaves the
    wiring between them untested, and a constant there would mark every
    container verified. This drives `run()` with the build stubbed, which is
    the same technique `test_default_template_logging.py` uses.
    """

    @staticmethod
    def _drive(monkeypatch, verifier_accepts: bool) -> dict:
        from unittest.mock import MagicMock, patch

        from datasmith.agents.sandbox import SandboxResult
        from datasmith.agents.synthesizer import Synthesizer
        from datasmith.docker.context import DockerContext

        captured: dict = {}
        synth = Synthesizer(agent="codex")
        # A rejected default build now FALLS THROUGH to PRODUCE_VERIFY instead
        # of returning, so the producer has to be stubbed or this drives a real
        # agent. The verdict being asserted is written before that happens.
        stalled = MagicMock()
        stalled.accepted = False
        stalled.context = None
        stalled.rounds = 1
        stalled.stop_reason = "no_progress"
        with (
            patch("datasmith.agents.synthesizer.verify_context") as mock_verify,
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
            patch.object(synth, "_log_default_attempt"),
            patch("datasmith.agents.synthesizer._host_scan_findings", return_value=[]),
            patch.object(synth, "_verify_built_image", return_value=verifier_accepts),
            patch.object(synth, "_run_produce_verify", return_value=stalled),
            patch.object(synth, "_save_context", side_effect=lambda *a, **k: captured.update(k)),
        ):
            mock_verify.return_value = SandboxResult(
                success=True,
                docker_context=DockerContext(build_pkg_sh="#!/bin/bash\ntrue"),
                image_tag="img:1",
                build_manifest={"build": {}},
            )
            synth.run("networkx", "networkx", 8148, "ctx", sha="a" * 40)
        return captured

    def test_an_accepting_verifier_marks_it_verified(self, monkeypatch) -> None:
        assert self._drive(monkeypatch, True).get("verified") is True

    def test_a_rejecting_verifier_leaves_it_unverified(self, monkeypatch) -> None:
        """The image is still kept -- it just makes no claim about itself."""
        assert self._drive(monkeypatch, False).get("verified") is False


class TestTheRepairPathAlsoRecordsVerification:
    """An accepted PRODUCE_VERIFY outcome must be markable `verified`.

    It holds all four facts by construction: the image built (mode was
    container_built), the host scan ran inside `verify()` and returned no
    findings, the verifier accepted, and `on_accept` carried the manifest out.

    numpy-financial#47 closed the repair loop at round 5/8 on 2026-08-25 and
    was stored `unverified`, because `verified=` was wired into the
    TRY_DEFAULT branch only. That is the path condition 4 exists to exercise,
    so it is the one that most needs to record a pass.
    """

    def test_an_accepted_loop_outcome_is_saved_verified(self) -> None:
        """Asserted over the AST, not over the source text.

        The first version of this test searched the source slice for the
        string `verified=True` -- and passed under mutation, because the
        COMMENT above the call contains that string. A test a comment can
        satisfy is not a test. The AST sees only code.
        """
        import ast
        import inspect
        import textwrap

        from datasmith.agents.synthesizer import Synthesizer

        tree = ast.parse(textwrap.dedent(inspect.getsource(Synthesizer.run)))
        saves: list[ast.Call] = []
        for node in ast.walk(tree):
            # The `if outcome.accepted and ...:` branch, whichever form it takes.
            if not isinstance(node, ast.If):
                continue
            if "outcome.accepted" not in ast.unparse(node.test):
                continue
            for inner in ast.walk(node):
                if (
                    isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Attribute)
                    and inner.func.attr == "_save_context"
                ):
                    saves.append(inner)
        assert saves, "no _save_context call found in the acceptance branch"
        for call in saves:
            kwargs = {k.arg: ast.unparse(k.value) for k in call.keywords}
            assert kwargs.get("verified") == "True", (
                "the PRODUCE_VERIFY acceptance path must record the verification it earned; "
                f"got verified={kwargs.get('verified')!r}"
            )


class TestTryDefaultIsSkippedWhenAContextIsAlreadyStored:
    """A stored context for this sha means the stock template did not build it.

    That is the only way such a row is written: agent-authored scripts land in
    `candidate_containers` because TRY_DEFAULT failed first. Re-running the
    template costs a full 300-700s build to re-learn the same fact. Measured
    over 39 TRY_DEFAULT attempts on tasks in exactly that state (the
    2026-08-24 sweeps plus the 2026-08-25 grind): 39 failures, 0 successes.

    PRODUCE_VERIFY seeds from the same stored context, so the cheap path is not
    lost -- it is the same scripts, one build earlier.
    """

    @staticmethod
    def _synth(agent="codex"):
        from datasmith.agents.synthesizer import Synthesizer

        return Synthesizer(agent=agent)

    def _synth_with(self, monkeypatch, *, stored, pv_on, agent="codex"):
        import datasmith.agents.reflexive.loop as loop_mod
        from datasmith.agents.synthesizer import Synthesizer

        monkeypatch.setattr(loop_mod, "DATASMITH_PV_ENABLED", pv_on)
        synth = Synthesizer(agent=agent)
        monkeypatch.setattr(
            Synthesizer,
            "_stored_producer_scripts",
            lambda self, o, r, sh: ("pkg", "run") if stored else None,
        )
        return synth

    def test_a_stored_context_supersedes_the_default_attempt(self, monkeypatch) -> None:
        synth = self._synth_with(monkeypatch, stored=True, pv_on=True)
        assert synth._stored_context_supersedes_default("o", "r", "sha") is True

    def test_no_stored_context_still_takes_the_cheap_path(self, monkeypatch) -> None:
        synth = self._synth_with(monkeypatch, stored=False, pv_on=True)
        assert synth._stored_context_supersedes_default("o", "r", "sha") is False

    def test_the_default_is_never_skipped_when_pv_is_off(self, monkeypatch) -> None:
        """With the flag off, TRY_DEFAULT is the only path that builds anything."""
        synth = self._synth_with(monkeypatch, stored=True, pv_on=False)
        assert synth._stored_context_supersedes_default("o", "r", "sha") is False

    def test_the_default_is_never_skipped_without_an_agent(self, monkeypatch) -> None:
        """`--agent none` has no producer, so PRODUCE_VERIFY cannot consume the
        stored context. Skipping here would leave the run with no path at all."""
        synth = self._synth_with(monkeypatch, stored=True, pv_on=True, agent="none")
        assert synth._stored_context_supersedes_default("o", "r", "sha") is False


class TestARejectedDefaultBuildReachesTheProducer:
    """A build the verifier rejects is exactly what the producer repairs.

    The TRY_DEFAULT branch used to return unconditionally once the image built
    and scanned clean. If the verifier then rejected it -- overwhelmingly on
    `pytest_pass_ratio` -- the task ended with an `unverified` row that nothing
    would revisit. On 2026-08-25 `dask/dask#6137`, `dask/dask#6186` and
    `UXARRAY/uxarray#1118` all died that way inside one hour, each having
    produced a container that built and scanned clean.
    """

    def _run_with_rejected_default(self, monkeypatch, *, accepted: bool):
        from unittest.mock import MagicMock

        import datasmith.agents.reflexive.loop as loop_mod
        import datasmith.agents.synthesizer as syn
        from datasmith.agents.sandbox import SandboxResult
        from datasmith.agents.synthesizer import Synthesizer

        monkeypatch.setattr(loop_mod, "DATASMITH_PV_ENABLED", True)
        monkeypatch.setattr(
            syn,
            "verify_context",
            lambda **kw: SandboxResult(
                success=True,
                image_tag="formulacode/x:1",
                build_manifest={"build": {}},
                resource_metrics={},
            ),
        )
        monkeypatch.setattr(syn, "_host_scan_findings", lambda tag: [])
        monkeypatch.setattr(Synthesizer, "_check_cache", lambda *a, **k: None)
        monkeypatch.setattr(Synthesizer, "_find_similar_contexts", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(Synthesizer, "_save_context", MagicMock())
        monkeypatch.setattr(Synthesizer, "_verify_built_image", lambda *a, **k: accepted)
        monkeypatch.setattr(Synthesizer, "_stored_producer_scripts", lambda *a, **k: None)

        seen: dict = {}

        def fake_pv(self, **kwargs):
            seen.update(kwargs)
            outcome = MagicMock()
            outcome.accepted = False
            outcome.context = None
            outcome.rounds = 1
            outcome.stop_reason = "no_progress"
            return outcome

        monkeypatch.setattr(Synthesizer, "_run_produce_verify", fake_pv)

        synth = Synthesizer(agent="codex")
        synth.run(
            owner="o",
            repo="r",
            issue_number=1,
            pr_context="ctx",
            sha="deadbeefcafe",
            repo_image="img",
            env_payload="{}",
            python_version="3.12",
            force=True,
        )
        return seen, synth

    def test_a_rejected_default_build_is_handed_to_the_producer(self, monkeypatch) -> None:
        seen, _ = self._run_with_rejected_default(monkeypatch, accepted=False)
        assert seen, "PRODUCE_VERIFY was never reached after the rejection"
        assert seen.get("seed_context") is not None, "the loop must start from the build that already works"

    def test_an_accepted_default_build_still_short_circuits(self, monkeypatch) -> None:
        """An accepted container must not spend agent budget being re-repaired."""
        seen, _ = self._run_with_rejected_default(monkeypatch, accepted=True)
        assert not seen, "an accepted default build must return without invoking the producer"
