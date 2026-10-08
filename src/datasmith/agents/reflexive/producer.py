"""The producer half: consume a rejection report, edit the two owned scripts.

The producer receives the typed report and its full evidence, and NEVER the
verifier's reasoning or transcript. Keeping it out of the deliberation is what
protects both contexts. Keeping it short of evidence would only make it guess,
so the payload is unbounded even though the envelope is typed.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from datasmith.agents.installed.base import InstalledAgent
from datasmith.agents.reflexive.schema import PRODUCER_OWNED_FILES, ProducerPlan
from datasmith.agents.reflexive.severity import GradedReport
from datasmith.docker.context import DockerContext

logger = logging.getLogger(__name__)

DATASMITH_PV_AGENT_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_AGENT_TIMEOUT_S", "1800"))

_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)

_PROMPT = """You are a container build PRODUCER.

You own exactly two files, in this directory:
  docker_build_pkg.sh   installs the package under test
  docker_build_run.sh   sets up asv

You may edit ONLY those two. You never judge your own work -- a separate
verifier does that, and you do not see how it reasons.

Your last build was REJECTED. The report follows, with its evidence.

{report}

Edit the two scripts in place to address the failures. Then reply with ONLY a
JSON object, no prose, no code fence:
{{"understood_rejections":[str],
  "planned_edits":[{{"file":"docker_build_pkg.sh"|"docker_build_run.sh",
                    "change":str,"addresses_check_id":str}}],
  "evidence_requests":[{{"request":str,"why":str}}]}}
"""


def parse_plan(raw: str) -> ProducerPlan | None:
    """Pull a ProducerPlan out of an agent reply. Never raises.

    A plan naming a file the producer does not own fails validation in
    PlannedEdit and lands here as None.
    """
    if not raw:
        return None
    candidates = [raw.strip()]
    match = _JSON_OBJECT.search(raw)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        cleaned = candidate.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            return ProducerPlan.model_validate_json(cleaned)
        # A bad reply is data, not an error: try the next candidate.
        except Exception:  # noqa: S112
            continue
    return None


def _render_report(graded: GradedReport) -> str:
    lines = [f"verdict: {graded.report.verdict.value}", f"mode: {graded.report.mode}", ""]
    for check in graded.report.checks:
        if check.verdict != "fail":
            continue
        lines += [
            f"## {check.id}  (cause: {check.cause.value})",
            f"evidence:\n{check.evidence}",
            f"suggested remedy: {check.remedy}",
            "",
        ]
    return "\n".join(lines)


def revise(
    context: DockerContext,
    graded: GradedReport,
    agent: InstalledAgent,
    workdir: Path,
    timeout_s: int = DATASMITH_PV_AGENT_TIMEOUT_S,
) -> tuple[DockerContext | None, ProducerPlan | None]:
    """Ask the producer to fix the build. Returns the revised context.

    The scripts on disk are the deliverable; the JSON plan is commentary. An
    unparseable plan still yields a revised context, because refusing an edit
    the agent actually made would waste a whole round.
    """
    prompt = _PROMPT.format(report=_render_report(graded))
    try:
        result = agent.exec(prompt, timeout=timeout_s, workdir=str(workdir))
    # An agent crash ends the round; it is never propagated.
    except Exception:
        logger.exception("producer agent raised")
        return None, None
    if not result.success:
        logger.warning("producer agent failed: %s", result.error)
        return None, None

    revised = context.model_copy(deep=True)
    for filename in PRODUCER_OWNED_FILES:
        path = workdir / filename
        if path.is_file():
            field = DockerContext._FILE_MAP[filename]
            setattr(revised, field, path.read_text(encoding="utf-8", errors="replace"))
    return revised, parse_plan(result.output)
