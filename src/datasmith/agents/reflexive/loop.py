"""The reflexive loop: produce, verify, feed back, stop.

A build costs 300 to 700 seconds, so the loop stops once a round can no longer
make progress.
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

# Round cap. Repairs are multi-step (pin a version, hit the next constraint), and the
# stall rule still ends a stuck producer early, so extra rounds go only to tasks still moving.
DATASMITH_PV_MAX_ROUNDS: int = int(os.environ.get("DATASMITH_PV_MAX_ROUNDS", "8"))
DATASMITH_PV_ENABLED: bool = os.environ.get("DATASMITH_PV_ENABLED", "").strip().lower() in {"1", "true", "yes"}

# Consecutive identical progress keys that end the loop; 2 gives the producer two
# tries at the same failure.
DATASMITH_PV_STALL_REPEATS: int = int(os.environ.get("DATASMITH_PV_STALL_REPEATS", "2"))

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
    # local_ci.py's summary names only the stage, so every failure at that stage would
    # share a signature; skip it so the scan reaches the real cause from failure.json.
    "Build failed at stage",
    "Build failed:",
)
# Noise matched anywhere in the line: `_default_failure_message` prefixes the summary
# with the stage name, so the startswith test in `_NOISE` misses it.
_NOISE_CONTAINS = (
    "Build failed at stage",
    "Build failed:",
)


def _strip_buildkit_prefix(line: str) -> str:
    """Drop BuildKit's step/elapsed stamp: `#14 6.799 `, `25.05 <U+00D7> `, `2.15 `
    (BuildKit separates with U+00D7, not the letter x).

    Shared by both signature branches so a timing difference can never make
    two identical failures look like progress. `scripts/prepass_trial.py`
    carries the same helper; an agreement test asserts the two do not drift.
    """
    return re.sub(r"^(?:#\d+\s+)?[\d.]+\s*(?:\u00d7\s*)?", "", line).strip()


def _signature(build_log: str) -> str:
    """The line that says WHY. Deterministic, unlike agent-invented ids."""
    # Truncation and the pipe substitution must match scripts/prepass_trial.py exactly.
    lines = [ln.strip() for ln in (build_log or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        if any(marker in ln for marker in _NAMED_CAUSES):
            # Strip the BuildKit timestamp here too, or identical failures never compare
            # equal and the stall rule cannot fire.
            return _strip_buildkit_prefix(ln)[:90].replace("|", "/")
    for ln in reversed(lines):
        if any(ln.startswith(n) for n in _NOISE) or any(n in ln for n in _NOISE_CONTAINS):
            continue
        body = re.sub(r"^[#\d.\s]+", "", ln)
        if len(body) > 12:
            return body[:90].replace("|", "/")
    return "no signature"


def progress_key(graded: GradedReport, build_log: str) -> tuple[str, ...]:
    """What "the same failure again" means.

    Mode B uses the battery's hard-failure ids. Mode A uses the build-log signature,
    because the ids there are invented by the agent and change between runs.
    """
    if graded.report.mode == "build_failed":
        return ("sig", _signature(build_log))
    return tuple(sorted(graded.hard_failures))


def _log_plan(round_index: int, plan: ProducerPlan) -> None:
    """Record what the producer proposed, per round.

    This lets producer edits be judged without reading the next round's build log.
    """
    for edit in plan.planned_edits:
        logger.info(
            "round %d producer edit: %s (addresses %s): %s",
            round_index,
            edit.file,
            edit.addresses_check_id,
            edit.change[:400],
        )
    for request in plan.evidence_requests:
        logger.info("round %d producer asked for evidence: %s", round_index, request.request[:200])


@dataclass
class LoopOutcome:
    accepted: bool
    rounds: int
    context: DockerContext | None
    reports: list[GradedReport] = field(default_factory=list)
    stop_reason: str = ""
    plans: list[ProducerPlan] = field(default_factory=list)
    # Carried out of the accepting round, because stage 8 gates on the build manifest.
    build_manifest: dict | None = None
    resource_metrics: dict = field(default_factory=dict)


def _stalled(
    key: tuple[str, ...], previous_key: tuple[str, ...] | None, repeats: int, round_index: int
) -> tuple[bool, int]:
    """Has the loop stopped learning? Returns (stop, new repeat count).

    A key that differs from the last one resets the count: the producer changed
    something the verifier could see. A key that repeats increments it, and the
    loop stops once it has repeated `DATASMITH_PV_STALL_REPEATS` times.
    """
    if previous_key is None or key != previous_key:
        return False, 0
    repeats += 1
    if repeats >= DATASMITH_PV_STALL_REPEATS:
        logger.info(
            "no progress in round %d (%s) after %d identical round(s); stopping",
            round_index,
            key,
            repeats,
        )
        return True, repeats
    logger.info(
        "round %d repeated the key %s (%d/%d before stalling)",
        round_index,
        key,
        repeats,
        DATASMITH_PV_STALL_REPEATS,
    )
    return False, repeats


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
    repeats = 0

    for round_index in range(1, max_rounds + 1):
        outcome.rounds = round_index
        ok, image_tag, build_log = build(context)
        mode = "container_built" if ok else "build_failed"
        # Log every round: the build log lives in a temporary directory, and reflexive
        # rounds write no `error_logs` row.
        logger.info(
            "round %d/%d: build ok=%s mode=%s image=%s",
            round_index,
            max_rounds,
            ok,
            mode,
            image_tag or "-",
        )
        if not ok:
            logger.info("round %d build log tail:\n%s", round_index, (build_log or "")[-4000:])

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
        logger.info(
            "round %d/%d: rejected; hard=%s soft=%s violations=%s key=%s",
            round_index,
            max_rounds,
            list(graded.hard_failures),
            list(graded.soft_failures),
            list(graded.violations),
            key,
        )
        stop, repeats = _stalled(key, previous_key, repeats, round_index)
        if stop:
            outcome.stop_reason = "no_progress"
            return outcome
        previous_key = key

        if round_index == max_rounds:
            outcome.stop_reason = "budget"
            return outcome

        revised, plan = revise(context, graded)
        if plan is not None:
            outcome.plans.append(plan)
            _log_plan(round_index, plan)
        if revised is None:
            outcome.stop_reason = "producer_failed"
            return outcome
        context = revised
        outcome.context = context

    outcome.stop_reason = "budget"
    return outcome
