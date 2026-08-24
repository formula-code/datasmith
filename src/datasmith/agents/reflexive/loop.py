"""The reflexive loop: produce, verify, feed back, stop.

Three stopping rules, and the third is the one that matters. A build costs 300
to 700 seconds, so a round that cannot learn anything must never be spent.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from datasmith.agents.reflexive.schema import ProducerPlan
from datasmith.agents.reflexive.severity import GradedReport
from datasmith.docker.context import DockerContext

logger = logging.getLogger(__name__)

DATASMITH_PV_MAX_ROUNDS: int = int(os.environ.get("DATASMITH_PV_MAX_ROUNDS", "3"))
DATASMITH_PV_ENABLED: bool = os.environ.get("DATASMITH_PV_ENABLED", "").strip().lower() in {"1", "true", "yes"}

# Mirrors scripts/prepass_trial.py. `scripts/` is not an importable package, so
# the scan is duplicated rather than imported; a test asserts the two agree.
_NAMED_CAUSES = (
    "BackendUnavailable",
    "ModuleNotFoundError",
    "Interrupted:",
    "error during collection",
    "errors during collection",
    "DISCOVERY_FAILED",
    "NO_BENCHMARKS",
    "reference is not a tree",
    "metadata-generation-failed",
    "Failed to build installable wheels",
    "unsatisfiable",
    "No `asv.conf` file found",
    "broke the interpreter",
)
_NOISE = (
    "ERROR: failed to build",
    "------",
    "> [",
    "note: This is an issue",
    "hint: See above",
    "FORMULACODE_",
    "{",
    "}",
    "=====",
    "!!!!!",
)


def _signature(build_log: str) -> str:
    """The line that says WHY. Deterministic, unlike agent-invented ids."""
    # Truncation and the pipe substitution must match scripts/prepass_trial.py
    # EXACTLY. They did not: this took [:110] and skipped the substitution
    # while prepass took [:90] and replaced "|" with "/". The agreement test
    # written to catch that drift used only branch-1 cases with no pipe, so it
    # passed while asserting something untrue.
    lines = [ln.strip() for ln in (build_log or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        if any(marker in ln for marker in _NAMED_CAUSES):
            return ln[:90].replace("|", "/")
    for ln in reversed(lines):
        if any(ln.startswith(n) for n in _NOISE):
            continue
        body = re.sub(r"^[#\d.\s]+", "", ln)
        if len(body) > 12:
            return body[:90].replace("|", "/")
    return "no signature"


def progress_key(graded: GradedReport, build_log: str) -> tuple[str, ...]:
    """What "the same failure again" means.

    Mode B uses the hard-failure ids, which the battery defines and are
    therefore canonical. Mode A uses the build-log signature, because there the
    ids come from the agent and are NOT stable -- the same model called a check
    "pytest-suite" in one run and "pep517_editable_backend_import" in another,
    so comparing invented ids would let rule 3 never fire in the mode that most
    needs it.
    """
    if graded.report.mode == "build_failed":
        return ("sig", _signature(build_log))
    return tuple(sorted(graded.hard_failures))


@dataclass
class LoopOutcome:
    accepted: bool
    rounds: int
    context: DockerContext | None
    reports: list[GradedReport] = field(default_factory=list)
    stop_reason: str = ""
    plans: list[ProducerPlan] = field(default_factory=list)
    # Carried out of the accepting round. stage 8 gates on the manifest, and a
    # PV-accepted container with a NULL build_manifest is indistinguishable
    # from one built before manifests existed.
    build_manifest: dict | None = None
    resource_metrics: dict = field(default_factory=dict)


def run_loop(
    context: DockerContext,
    build: Callable[[DockerContext], tuple[bool, str | None, str]],
    verify: Callable[[str | None, str, str], GradedReport],
    revise: Callable[[DockerContext, GradedReport], tuple[DockerContext | None, ProducerPlan | None]],
    workdir: Path,
    max_rounds: int = DATASMITH_PV_MAX_ROUNDS,
    on_accept: Callable[[], tuple[dict | None, dict]] | None = None,
) -> LoopOutcome:
    """Run the producer/verifier loop.

    `build` returns (ok, image_tag_or_None, build_log).
    `verify` takes (image_tag_or_None, build_log, mode) and grades.
    `revise` takes (context, graded) and returns a revised context.
    `on_accept` is called with the accepting round's context so the caller can
    attach the manifest and resource metrics it collected during `build`.
    """
    outcome = LoopOutcome(accepted=False, rounds=0, context=context)
    previous_key: tuple[str, ...] | None = None

    for round_index in range(1, max_rounds + 1):
        outcome.rounds = round_index
        ok, image_tag, build_log = build(context)
        mode = "container_built" if ok else "build_failed"

        try:
            graded = verify(image_tag, build_log, mode)
        # An error is a rejection, never a propagated crash.
        except Exception:
            logger.exception("verify raised in round %d", round_index)
            outcome.stop_reason = "verifier_error"
            return outcome

        outcome.reports.append(graded)
        if graded.accepted:
            outcome.accepted = True
            outcome.context = context
            outcome.stop_reason = "accepted"
            if on_accept is not None:
                manifest, metrics = on_accept()
                outcome.build_manifest = manifest
                outcome.resource_metrics = metrics
            return outcome

        key = progress_key(graded, build_log)
        if previous_key is not None and key == previous_key:
            logger.info("no progress in round %d (%s); stopping", round_index, key)
            outcome.stop_reason = "no_progress"
            return outcome
        previous_key = key

        if round_index == max_rounds:
            outcome.stop_reason = "budget"
            return outcome

        revised, plan = revise(context, graded)
        if plan is not None:
            outcome.plans.append(plan)
        if revised is None:
            outcome.stop_reason = "producer_failed"
            return outcome
        context = revised
        outcome.context = context

    outcome.stop_reason = "budget"
    return outcome
