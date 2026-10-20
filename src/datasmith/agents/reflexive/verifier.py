"""The verifier half: collect facts, ask the agent, grade the answer.

Every failure path in this module ends in accepted=False.

Facts come from two places. `image_integrity` reads the image from the host and
runs nothing from it. `battery` runs commands inside the container, where an
interpreter the builder configured can lie. So a fatal host scan finding rejects
on its own, before the battery runs or the agent is asked.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Callable

from datasmith.agents.installed.base import InstalledAgent
from datasmith.agents.reflexive.battery import BatteryFact, run_battery
from datasmith.agents.reflexive.image_integrity import (
    ImageIntegrity,
    IntegrityFinding,
    collect_and_evaluate,
    facts_block,
)
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import GradedReport, grade

logger = logging.getLogger(__name__)

DATASMITH_PV_AGENT_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_AGENT_TIMEOUT_S", "1800"))

_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)

_RETRY_SUFFIX = "\nYour last reply was not valid JSON. Reply with ONLY the JSON object."

_PROMPT = """You are a container build VERIFIER.

You judge whether a built container is sound. You NEVER edit build scripts --
copies are shown to you read-only, for diagnosis only. A separate producer
agent owns them.

Ignore hardware-specific limitations: a missing GPU, a specific CPU feature, or
any facility this host cannot provide. Judge the BUILD, not the machine.

Evidence reaches you two ways and they do not carry equal weight. The
`host_image_scan` section was read from the image on the host, without running
anything from it, so the container could not have influenced it. Every other
section is output from commands run INSIDE the container, which means a
tampered image can shape it. Where the two disagree, the host scan is right.

Report the observed CAUSE of each failure. Do not assign severity -- that is
graded downstream. Valid causes:
  module_not_found                  nothing is installed under that name
  import_raised_on_host_facility    installed, but raises for want of a host facility
  pytest_version_incompat           imports fine; pytest rejects the test's own syntax
  other                             anything else

The ids below are a NAMING CONVENTION for failures you have ALREADY OBSERVED in
the evidence above. They are NOT a list of checks to carry out: do not perform
any check you were not shown the output of, and do not report one you did not
observe.
  pytest_collect     pytest could not collect the suite
  pytest_run         the suite could not RUN at all
  pytest_pass_ratio  the suite ran and some tests FAILED. This is the id for
                     "N of M passed"; pytest_run is not
  import_sweep       the package, or a compiled extension, does not import
Any other id is graded as a hard failure whatever you meant by it.

If you record a failure that you believe should NOT block publishing, you MUST
put your argument in `waiver_reason`, and it must be checkable against the
evidence -- restating the failure is not an argument. A failure with no argued
reason is graded hard. No waiver can rescue a tampered container: integrity is
decided from the host scan before you are asked, and you are not consulted.

{context}

Reply with ONLY a JSON object, no prose, no code fence:
{{"verdict":"accept"|"reject",
  "mode":"{mode}",
  "checks":[{{"id":str,"verdict":"pass"|"fail","cause":str,"evidence":str,
             "remedy":str,"waiver_reason":str|null}}],
  "evidence_you_lack":[str]}}
"""


def parse_report(raw: str) -> RejectionReport | None:
    """Pull a RejectionReport out of an agent reply. Never raises."""
    if not raw:
        return None
    candidates = [raw.strip()]
    match = _JSON_OBJECT.search(raw)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        cleaned = candidate.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            return RejectionReport.model_validate_json(cleaned)
        # A bad reply is data, not an error: try the next candidate.
        except Exception:  # noqa: S112
            continue
    return None


def _reject(check_id: str, evidence: str, mode: str) -> GradedReport:
    """Build a rejection out of thin air, for paths with no agent answer."""
    report = RejectionReport(
        verdict=Verdict.REJECT,
        mode=mode,
        checks=[CheckResult(id=check_id, verdict="fail", cause=Cause.OTHER, evidence=evidence[:4000], remedy="")],
    )
    return grade(report)


def _facts_block(facts: list[BatteryFact]) -> str:
    parts: list[str] = []
    for fact in facts:
        head = f"### {fact.name}  (rc={fact.rc}{', CRASHED' if fact.crashed else ''})\n$ {fact.command}"
        parts.append(f"{head}\n{fact.stdout[-4000:]}\n{fact.stderr[-2000:]}")
    return "\n\n".join(parts)


def _integrity_checks(integrity: ImageIntegrity) -> list[CheckResult]:
    """Fatal integrity findings, as checks `severity.py` will grade HARD.

    `tamper_audit` is in `HARD_CHECK_IDS`; `image_scan_failed` grades HARD as an
    unknown id. Neither can be softened by a cause or a waiver_reason.
    """
    return [
        CheckResult(
            id=finding.check_id,
            verdict="fail",
            cause=Cause.OTHER,
            evidence=finding.detail[:4000],
            remedy="",
        )
        for finding in integrity.findings
    ]


def verify(
    image_tag: str | None,
    build_log: str,
    agent: InstalledAgent,
    mode: str,
    runner: Callable[[str, list[str], int], tuple[str, str, int]] | None = None,
    timeout_s: int = DATASMITH_PV_AGENT_TIMEOUT_S,
    integrity_collector: Callable[[str], ImageIntegrity] | None = None,
) -> GradedReport:
    """Verify one build. `mode` is "container_built" or "build_failed"."""
    facts: list[BatteryFact] = []
    if mode == "build_failed" or image_tag is None:
        context = f"The build FAILED, so no container exists. BUILD LOG:\n{build_log[-12000:]}"
    else:
        collect = integrity_collector or collect_and_evaluate
        try:
            integrity = collect(image_tag)
        # A collector that raises leaves us knowing nothing about the image,
        # and nothing-known is a rejection, never a skip.
        except Exception as exc:
            logger.exception("host image scan raised on %s", image_tag)
            error = f"{type(exc).__name__}: {exc}"[:2000]
            integrity = ImageIntegrity(
                image=image_tag,
                collected=False,
                error=error,
                findings=(IntegrityFinding("image_scan_failed", f"host image scan raised on {image_tag}: {error}"),),
            )

        if integrity.findings:
            # Stop here: these ids are hard and cannot be waived, and running the battery
            # would execute code from an image already found to be tampered.
            logger.error(
                "host image scan rejected %s: %s",
                image_tag,
                "; ".join(f.detail.splitlines()[0][:200] for f in integrity.findings),
            )
            return grade(RejectionReport(verdict=Verdict.REJECT, mode=mode, checks=_integrity_checks(integrity)))

        facts = run_battery(image_tag, runner=runner)
        context = (
            f"{facts_block(integrity)}\n\n"
            f"Evidence collected by running commands INSIDE the container:\n\n{_facts_block(facts)}"
        )

    prompt = _PROMPT.format(context=context, mode=mode)

    raw = ""
    for attempt in (1, 2):
        try:
            result = agent.exec(prompt if attempt == 1 else prompt + _RETRY_SUFFIX, timeout=timeout_s)
        # An agent crash is a rejection, never a propagated exception.
        except Exception as exc:
            # logger.exception already attaches the traceback, so `exc` is redundant here.
            logger.exception("verifier agent raised")
            return _reject("verifier_agent_error", f"{type(exc).__name__}: {exc}", mode)
        if not result.success:
            return _reject("verifier_agent_failed", result.error or "agent reported failure", mode)
        raw = result.output
        report = parse_report(raw)
        if report is not None:
            graded = grade(report)
            if mode == "build_failed" or image_tag is None:
                # A failed build is never accepted: `grade()` accepts an empty checks list, and in
                # build_failed mode there are no battery facts. The report is kept for the producer.
                return grade(
                    RejectionReport(
                        verdict=Verdict.REJECT,
                        mode=mode,
                        checks=[
                            *report.checks,
                            CheckResult(
                                id="build_failed",
                                verdict="fail",
                                cause=Cause.OTHER,
                                evidence=(build_log or "")[-4000:],
                                remedy="",
                            ),
                        ],
                        evidence_you_lack=report.evidence_you_lack,
                    )
                )
            crashed = [f for f in facts if f.crashed]
            if crashed:
                # A command that could not run has not passed, whatever the
                # agent concluded from the rest.
                extra = RejectionReport(
                    verdict=Verdict.REJECT,
                    mode=mode,
                    checks=[
                        *report.checks,
                        *[
                            CheckResult(
                                id=f"battery_crashed_{f.name}",
                                verdict="fail",
                                cause=Cause.OTHER,
                                evidence=f.stderr[:2000],
                                remedy="",
                            )
                            for f in crashed
                        ],
                    ],
                    evidence_you_lack=report.evidence_you_lack,
                )
                return grade(extra)
            return graded
    return _reject("verifier_unparseable_reply", raw[:4000], mode)
