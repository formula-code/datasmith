# Producer / Verifier Reflexive Container Builds — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace stage 6's `LLM_GENERATE` state with a reflexive loop between two agents that do not share a context — a producer that owns the build scripts and never judges, and a verifier that runs the container and never edits.

**Architecture:** A new package `src/datasmith/agents/reflexive/` with six single-purpose modules. Severity is graded by **our** code (`severity.py`), never by the verifier's prompt. The verifier collects facts by running a battery inside the built image, hands the producer a typed report with unbounded evidence, and the loop terminates on accept, budget, or no-progress. Everything is behind `DATASMITH_PV_ENABLED`, default off.

**Tech Stack:** Python 3.11/3.12, pydantic v2 (frozen models), pytest, Docker + BuildKit, `codex`/`claude` CLI agents via `datasmith.agents.installed`.

**Spec:** `docs/superpowers/specs/2026-08-23-producer-verifier-design.md`

## Global Constraints

Copied verbatim from the spec and CLAUDE.md. Every task's requirements implicitly include these.

- **An error is a rejection, never an acceptance.** A host-side timeout once returned success and silently verified ~34% of `candidate_containers`. Never repeat that inversion.
- **The producer must never author the manifest.** Producer statements go in `producer_claims`. Facts go in `build` (our sealer) and `verify` (the verifier's battery).
- **Severity is enforced in `severity.py`, in our code — NOT in the verifier's prompt.** An attempt to waive a hard check is ignored and logged as a verifier-side violation.
- **A battery command that crashes is `fail`, not `skip`.** This deliberately departs from `docker/manifest.py`'s three-valued convention, which is right for an unrun image and wrong here.
- Every tunable is `DATASMITH_`-prefixed, read at module scope via `os.environ.get`, with a literal default.
- Python floor is 3.11 — CI runs 3.11 and 3.12. No PEP 695 type params, no `X | Y` inside `isinstance`.
- Ruff line length 120. mypy strict (`disallow_untyped_defs`). `make check` runs `uv lock --locked`, so any `pyproject.toml` dependency edit needs a refreshed `uv.lock`.
- Tests that build or run real containers MUST be marked `slow`. `make test` and CI run `-m "not slow"`.
- **Never** run `docker volume prune` (the local Supabase DB lives in a volume). **Never** `git add -A`, `git add .`, or `git commit -a` — many unrelated files are modified in this tree. Use explicit paths.
- `SUPABASE_URL=http://127.0.0.1:54321` only. Never write to `db.formulacode.org`.
- Mermaid for diagrams in `.md`, never ASCII box art.

## File Structure

| File | Responsibility |
|---|---|
| `src/datasmith/agents/reflexive/__init__.py` | Public surface re-export |
| `src/datasmith/agents/reflexive/schema.py` | Frozen pydantic models. No logic. |
| `src/datasmith/agents/reflexive/severity.py` | `classify(check_id, cause) -> Severity`. The only place severity is decided. |
| `src/datasmith/agents/reflexive/battery.py` | What the verifier RUNS inside the image. Returns facts, never verdicts. |
| `src/datasmith/agents/reflexive/verifier.py` | Runs battery, prompts verifier agent, parses + grades the report. |
| `src/datasmith/agents/reflexive/producer.py` | Prompts producer agent with the report, applies script edits to a `DockerContext`. |
| `src/datasmith/agents/reflexive/loop.py` | Rounds, budget, no-progress, fail-closed. |
| `tests/agents/reflexive/test_*.py` | One test module per source module. |
| `scripts/pv_validate.py` | The 16-container validation harness (Task 9). |

---

## Task 1: Schema

**Files:**
- Create: `src/datasmith/agents/reflexive/__init__.py`
- Create: `src/datasmith/agents/reflexive/schema.py`
- Test: `tests/agents/reflexive/__init__.py`, `tests/agents/reflexive/test_schema.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `Severity` (str enum: `HARD`, `SOFT`), `Cause` (str enum: `MODULE_NOT_FOUND`, `IMPORT_RAISED_ON_HOST_FACILITY`, `PYTEST_VERSION_INCOMPAT`, `OTHER`), `Verdict` (str enum: `ACCEPT`, `REJECT`), `CheckResult(id: str, verdict: str, cause: Cause, evidence: str, remedy: str, severity: Severity | None = None, waiver_reason: str | None = None)`, `RejectionReport(verdict: Verdict, mode: str, checks: list[CheckResult], evidence_you_lack: list[str] = [])`, `EvidenceRequest(request: str, why: str)`, `RawOutput(request: str, command_run: str, stdout: str, stderr: str, rc: int)`, `ProducerPlan(understood_rejections: list[str], planned_edits: list[PlannedEdit], evidence_requests: list[EvidenceRequest])`, `PlannedEdit(file: str, change: str, addresses_check_id: str)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_schema.py
"""The wire format between producer and verifier.

Frozen, because a report that can be mutated after grading is a report that
can be re-graded. Severity is deliberately Optional on the wire: the verifier
reports a CAUSE and our code assigns severity. A verifier-supplied severity is
advisory and must never be trusted.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datasmith.agents.reflexive.schema import (
    Cause,
    CheckResult,
    EvidenceRequest,
    PlannedEdit,
    ProducerPlan,
    RawOutput,
    RejectionReport,
    Severity,
    Verdict,
)


def test_a_minimal_report_round_trips() -> None:
    report = RejectionReport(
        verdict=Verdict.REJECT,
        mode="container_built",
        checks=[
            CheckResult(
                id="pytest_collect",
                verdict="fail",
                cause=Cause.PYTEST_VERSION_INCOMPAT,
                evidence="in parametrize the number of names (1)",
                remedy="pin pytest below 8",
            )
        ],
    )
    again = RejectionReport.model_validate_json(report.model_dump_json())
    assert again == report
    assert again.checks[0].cause is Cause.PYTEST_VERSION_INCOMPAT


def test_a_report_is_frozen() -> None:
    """Grading must not be re-writable after the fact."""
    report = RejectionReport(verdict=Verdict.ACCEPT, mode="container_built", checks=[])
    with pytest.raises(ValidationError):
        report.verdict = Verdict.REJECT


def test_severity_is_absent_on_the_wire_by_default() -> None:
    """The verifier reports a cause. Our code assigns severity."""
    check = CheckResult(id="x", verdict="fail", cause=Cause.OTHER, evidence="e", remedy="r")
    assert check.severity is None


def test_an_unknown_cause_is_rejected_not_coerced() -> None:
    """A cause we do not model must not silently become OTHER."""
    with pytest.raises(ValidationError):
        CheckResult(id="x", verdict="fail", cause="cosmic_rays", evidence="e", remedy="r")


def test_evidence_you_lack_defaults_to_empty() -> None:
    report = RejectionReport(verdict=Verdict.ACCEPT, mode="build_failed", checks=[])
    assert report.evidence_you_lack == []


def test_producer_plan_round_trips() -> None:
    plan = ProducerPlan(
        understood_rejections=["backend missing"],
        planned_edits=[
            PlannedEdit(
                file="docker_build_pkg.sh",
                change="install hatchling before the editable install",
                addresses_check_id="pep517_backend",
            )
        ],
        evidence_requests=[EvidenceRequest(request="show pip freeze", why="confirm hatchling landed")],
    )
    assert ProducerPlan.model_validate_json(plan.model_dump_json()) == plan


def test_planned_edit_rejects_a_file_we_do_not_let_the_producer_touch() -> None:
    """The producer owns exactly two scripts. Anything else is out of bounds."""
    with pytest.raises(ValidationError):
        PlannedEdit(file="docker_build_final.sh", change="c", addresses_check_id="x")


def test_raw_output_records_what_was_actually_run() -> None:
    """The producer asks in prose; the verifier must report the real command."""
    out = RawOutput(request="show me pip freeze", command_run="pip freeze", stdout="numpy==2.1", stderr="", rc=0)
    assert out.command_run == "pip freeze"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/schema.py
"""Wire format between the producer and the verifier.

Every model is frozen. A report that can be mutated after grading is a report
that can be re-graded, and grading is the one thing this subsystem exists to
protect.

`CheckResult.severity` is Optional and defaults to None on purpose. The
verifier reports an observed CAUSE; `severity.py` maps that cause to HARD or
SOFT. A verifier-supplied severity is advisory and is never trusted.
"""

from __future__ import annotations

import enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Severity(str, enum.Enum):
    HARD = "hard"
    SOFT = "soft"


class Verdict(str, enum.Enum):
    ACCEPT = "accept"
    REJECT = "reject"


class Cause(str, enum.Enum):
    """Why a check failed. The discriminator for severity.

    The split between the first two is failure MODE, not package availability.
    cupy is on PyPI, resolves into env_payload, installs cleanly, and still
    raises with no CUDA driver -- so presence is evidence FOR a capability
    claim, not against it.
    """

    MODULE_NOT_FOUND = "module_not_found"
    IMPORT_RAISED_ON_HOST_FACILITY = "import_raised_on_host_facility"
    PYTEST_VERSION_INCOMPAT = "pytest_version_incompat"
    OTHER = "other"


# The producer owns exactly these two scripts. Everything else in the build
# context is ours, and an edit to it is out of bounds by construction.
PRODUCER_OWNED_FILES = ("docker_build_pkg.sh", "docker_build_run.sh")


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CheckResult(_Frozen):
    id: str
    verdict: str
    cause: Cause
    evidence: str
    remedy: str
    severity: Severity | None = None
    waiver_reason: str | None = None


class RejectionReport(_Frozen):
    verdict: Verdict
    mode: str
    checks: list[CheckResult] = Field(default_factory=list)
    evidence_you_lack: list[str] = Field(default_factory=list)


class EvidenceRequest(_Frozen):
    request: str
    why: str


class RawOutput(_Frozen):
    request: str
    command_run: str
    stdout: str
    stderr: str
    rc: int


class PlannedEdit(_Frozen):
    file: str
    change: str
    addresses_check_id: str

    @field_validator("file")
    @classmethod
    def _must_be_producer_owned(cls, value: str) -> str:
        if value not in PRODUCER_OWNED_FILES:
            raise ValueError(f"producer may not edit {value!r}; owns {PRODUCER_OWNED_FILES}")
        return value


class ProducerPlan(_Frozen):
    understood_rejections: list[str] = Field(default_factory=list)
    planned_edits: list[PlannedEdit] = Field(default_factory=list)
    evidence_requests: list[EvidenceRequest] = Field(default_factory=list)
```

```python
# src/datasmith/agents/reflexive/__init__.py
"""Producer/verifier reflexive container builds.

See docs/superpowers/specs/2026-08-23-producer-verifier-design.md
"""

from datasmith.agents.reflexive.schema import (
    Cause,
    CheckResult,
    EvidenceRequest,
    PlannedEdit,
    ProducerPlan,
    RawOutput,
    RejectionReport,
    Severity,
    Verdict,
)

__all__ = [
    "Cause",
    "CheckResult",
    "EvidenceRequest",
    "PlannedEdit",
    "ProducerPlan",
    "RawOutput",
    "RejectionReport",
    "Severity",
    "Verdict",
]
```

Create `tests/agents/reflexive/__init__.py` as an empty file.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_schema.py -v && uv run ruff check src/datasmith/agents/reflexive tests/agents/reflexive && uv run mypy`
Expected: 7 passed, ruff clean, mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/reflexive/__init__.py src/datasmith/agents/reflexive/schema.py tests/agents/reflexive/__init__.py tests/agents/reflexive/test_schema.py
git commit -m "feat(reflexive): wire format for the producer/verifier loop

Frozen models throughout. A report that can be mutated after grading is a
report that can be re-graded.

CheckResult.severity is Optional and defaults to None deliberately: the
verifier reports an observed CAUSE and severity.py maps it. A
verifier-supplied severity is advisory and never trusted.

PlannedEdit refuses any file outside docker_build_pkg.sh and
docker_build_run.sh, so the producer cannot reach the sealer or the
final stage by construction rather than by instruction."
```

---

## Task 2: Severity

**Files:**
- Create: `src/datasmith/agents/reflexive/severity.py`
- Test: `tests/agents/reflexive/test_severity.py`

**Interfaces:**
- Consumes: `Cause`, `Severity`, `CheckResult` from `schema.py`.
- Produces: `classify(check_id: str, cause: Cause) -> Severity`, `HARD_CHECK_IDS: frozenset[str]`, `grade(report: RejectionReport) -> GradedReport`, `GradedReport(report: RejectionReport, hard_failures: tuple[str, ...], soft_failures: tuple[str, ...], violations: tuple[str, ...], accepted: bool)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_severity.py
"""Severity is decided HERE, in our code, never in the verifier's prompt.

This is the structural answer to the standing objection that a Turing-complete
agent always defeats a verifier. The agent does not get to decide the hard set.
It reports a fact and this module grades it.
"""

from __future__ import annotations

import pytest

from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Severity, Verdict
from datasmith.agents.reflexive.severity import classify, grade


def _check(cid: str, cause: Cause, **kw) -> CheckResult:
    return CheckResult(id=cid, verdict="fail", cause=cause, evidence="e", remedy="r", **kw)


class TestClassify:
    def test_a_module_that_is_not_installed_is_hard(self) -> None:
        assert classify("pytest_collect", Cause.MODULE_NOT_FOUND) is Severity.HARD

    def test_an_installed_module_raising_on_a_host_facility_is_soft(self) -> None:
        """cupy installs cleanly and still raises with no CUDA driver."""
        assert classify("pytest_collect", Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.SOFT

    def test_a_pytest_version_incompatibility_is_hard(self) -> None:
        """aicsimageio and datashader. The remedy is to pin pytest."""
        assert classify("pytest_collect", Cause.PYTEST_VERSION_INCOMPAT) is Severity.HARD

    @pytest.mark.parametrize(
        "check_id",
        ["tamper_audit", "asv_discovers_zero_against_source", "measure_timed_out", "asv_exec_failed", "oracle_patch_failed"],
    )
    def test_the_always_hard_ids_ignore_the_cause(self, check_id: str) -> None:
        """No cause can soften these. Not even one we do not model."""
        assert classify(check_id, Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.HARD

    def test_the_pass_ratio_check_is_soft(self) -> None:
        """fluids at 554/559 is the motivating case."""
        assert classify("pytest_pass_ratio", Cause.OTHER) is Severity.SOFT

    def test_an_unknown_check_id_defaults_to_hard(self) -> None:
        """Fail closed. An unrecognised check must not be waivable."""
        assert classify("something_invented_by_the_agent", Cause.OTHER) is Severity.HARD


class TestGrade:
    def test_a_clean_report_is_accepted(self) -> None:
        report = RejectionReport(verdict=Verdict.ACCEPT, mode="container_built", checks=[])
        assert grade(report).accepted is True

    def test_one_hard_failure_rejects(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,  # the agent said accept...
            mode="container_built",
            checks=[_check("pytest_collect", Cause.PYTEST_VERSION_INCOMPAT)],
        )
        graded = grade(report)
        assert graded.accepted is False, "our grading overrides the agent's verdict"
        assert graded.hard_failures == ("pytest_collect",)

    def test_a_soft_failure_alone_is_accepted(self) -> None:
        report = RejectionReport(
            verdict=Verdict.REJECT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER, waiver_reason="5 numba failures, commit-invariant")],
        )
        graded = grade(report)
        assert graded.accepted is True
        assert graded.soft_failures == ("pytest_pass_ratio",)

    def test_a_soft_failure_with_no_waiver_reason_rejects(self) -> None:
        """A waiver must be argued, not merely asserted."""
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER)],
        )
        assert grade(report).accepted is False

    def test_waiving_a_hard_check_is_ignored_and_logged(self) -> None:
        """The spec's core promise. An agent that waives a hard check fails."""
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[
                _check(
                    "tamper_audit",
                    Cause.IMPORT_RAISED_ON_HOST_FACILITY,
                    severity=Severity.SOFT,          # the agent claims soft
                    waiver_reason="looks fine to me",
                )
            ],
        )
        graded = grade(report)
        assert graded.accepted is False
        assert graded.hard_failures == ("tamper_audit",)
        assert graded.violations == ("tamper_audit",), "the attempted waiver must be recorded"

    def test_a_passing_check_does_not_count_as_a_failure(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[CheckResult(id="x", verdict="pass", cause=Cause.OTHER, evidence="e", remedy="")],
        )
        graded = grade(report)
        assert graded.accepted is True
        assert graded.hard_failures == ()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_severity.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive.severity'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/severity.py
"""Grading. The only place severity is decided.

Deliberately NOT in the verifier's prompt. The agent reports an observed cause
and this module maps it. That is the structural answer to the objection that a
Turing-complete agent always defeats a verifier: it does not get to decide the
hard set, only to reason inside it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from datasmith.agents.reflexive.schema import Cause, RejectionReport, Severity

logger = logging.getLogger(__name__)

# Checks no cause can soften. A capability claim against one of these is
# meaningless -- a tampered container is tampered whatever the host lacks.
HARD_CHECK_IDS: frozenset[str] = frozenset(
    {
        "tamper_audit",
        "asv_discovers_zero_against_source",
        "measure_timed_out",
        "asv_exec_failed",
        "oracle_patch_failed",
        "numpy_moved_major_minor",
    }
)

# Checks whose severity depends on the observed cause.
_SOFT_CHECK_IDS: frozenset[str] = frozenset({"pytest_pass_ratio", "repo_smoke_test"})

# Only this cause can make a check soft. Everything else is hard.
#
# The discriminator is failure MODE, not package availability. cupy is on
# PyPI, resolves into env_payload, installs cleanly, and still raises when no
# CUDA driver exists -- so presence is evidence FOR a capability claim rather
# than against it.
_SOFTENING_CAUSES: frozenset[Cause] = frozenset({Cause.IMPORT_RAISED_ON_HOST_FACILITY})


def classify(check_id: str, cause: Cause) -> Severity:
    """Grade one check. Unknown ids fail closed to HARD."""
    if check_id in HARD_CHECK_IDS:
        return Severity.HARD
    if check_id in _SOFT_CHECK_IDS:
        return Severity.SOFT
    if cause in _SOFTENING_CAUSES:
        return Severity.SOFT
    return Severity.HARD


@dataclass(frozen=True)
class GradedReport:
    report: RejectionReport
    hard_failures: tuple[str, ...]
    soft_failures: tuple[str, ...]
    violations: tuple[str, ...]
    accepted: bool


def grade(report: RejectionReport) -> GradedReport:
    """Decide the verdict from the checks, ignoring the agent's own verdict.

    A soft failure must carry a written waiver_reason. A waiver has to be
    argued, not merely asserted, and the reason is what a human reads when the
    container is later found to be bad.
    """
    hard: list[str] = []
    soft: list[str] = []
    violations: list[str] = []

    for check in report.checks:
        if check.verdict != "fail":
            continue
        actual = classify(check.id, check.cause)
        if check.severity is Severity.SOFT and actual is Severity.HARD:
            violations.append(check.id)
            logger.error(
                "verifier tried to waive hard check %r as soft (cause=%s, reason=%r); ignored",
                check.id,
                check.cause.value,
                check.waiver_reason,
            )
        if actual is Severity.HARD:
            hard.append(check.id)
        elif check.waiver_reason:
            soft.append(check.id)
        else:
            hard.append(check.id)

    return GradedReport(
        report=report,
        hard_failures=tuple(hard),
        soft_failures=tuple(soft),
        violations=tuple(violations),
        accepted=not hard,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_severity.py -v && uv run mypy`
Expected: 14 passed, mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/reflexive/severity.py tests/agents/reflexive/test_severity.py
git commit -m "feat(reflexive): grade severity in our code, not the agent's prompt

classify() maps (check_id, cause) to HARD or SOFT. Unknown check ids fail
closed to HARD, so a check the agent invents cannot be waivable.

The softening discriminator is failure MODE, not package availability. An
earlier design rejected a capability claim whenever the module was on PyPI
or in env_payload -- but cupy is on PyPI, resolves into env_payload,
installs cleanly, and still raises with no CUDA driver. Presence is
evidence FOR the claim.

grade() ignores the agent's own verdict and recomputes from the checks. A
soft failure must carry a written waiver_reason: a waiver has to be
argued, not asserted. An attempt to waive a HARD check is ignored and
recorded in violations."
```

---

## Task 3: Battery

**Files:**
- Create: `src/datasmith/agents/reflexive/battery.py`
- Test: `tests/agents/reflexive/test_battery.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `BatteryFact(name: str, command: str, stdout: str, stderr: str, rc: int, crashed: bool)`, `run_battery(image_tag: str, runner: Callable[[str, list[str], int], tuple[str, str, int]] | None = None) -> list[BatteryFact]`, `BATTERY_COMMANDS: tuple[tuple[str, tuple[str, ...]], ...]`.

The `runner` parameter is dependency injection so tests never need Docker. Signature: `runner(image_tag, argv, timeout_s) -> (stdout, stderr, rc)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_battery.py
"""What the verifier RUNS. Facts only, never verdicts.

A command that crashes yields a fact with crashed=True, and the caller turns
that into a FAILING check -- not a skipped one. That departs from
docker/manifest.py's three-valued convention on purpose: the verifier CHOSE to
run this command, so failure to execute is a finding about the container.
"""

from __future__ import annotations

from datasmith.agents.reflexive.battery import BATTERY_COMMANDS, BatteryFact, run_battery


def _fake_runner(script: dict[str, tuple[str, str, int]]):
    calls: list[list[str]] = []

    def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
        calls.append(argv)
        key = argv[0] if argv else ""
        if key == "BOOM":
            raise RuntimeError("docker daemon went away")
        return script.get(key, ("", "", 0))

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


def test_every_battery_command_produces_a_fact() -> None:
    facts = run_battery("img:1", runner=_fake_runner({}))
    assert len(facts) == len(BATTERY_COMMANDS)
    assert {f.name for f in facts} == {name for name, _ in BATTERY_COMMANDS}


def test_a_fact_carries_the_raw_output() -> None:
    marker = BATTERY_COMMANDS[0][1][0]
    runner = _fake_runner({marker: ("576 passed", "warn", 1)})
    facts = {f.name: f for f in run_battery("img:1", runner=runner)}
    first = facts[BATTERY_COMMANDS[0][0]]
    assert first.stdout == "576 passed"
    assert first.stderr == "warn"
    assert first.rc == 1
    assert first.crashed is False


def test_a_crashing_command_is_a_fact_with_crashed_true_not_an_exception() -> None:
    """run_battery must never propagate. A crash is a finding."""

    def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
        raise RuntimeError("docker daemon went away")

    facts = run_battery("img:1", runner=runner)
    assert len(facts) == len(BATTERY_COMMANDS)
    assert all(f.crashed for f in facts)
    assert all(f.rc != 0 for f in facts), "a crashed command must never look successful"
    assert all("docker daemon went away" in f.stderr for f in facts)


def test_the_battery_never_writes_to_the_build_context() -> None:
    """Read-only posture, asserted on the actual argv we ship."""
    joined = " ".join(" ".join(argv) for _, argv in BATTERY_COMMANDS)
    for forbidden in ("docker_build_pkg.sh", "docker_build_run.sh", " > ", "tee ", "rm "):
        assert forbidden not in joined, f"battery must not use {forbidden!r}"


def test_the_battery_covers_the_facts_the_spec_names() -> None:
    names = {name for name, _ in BATTERY_COMMANDS}
    assert {"pytest_collect", "asv_discover", "source_benchmark_count", "import_sweep"} <= names


def test_every_env_only_binary_is_invoked_through_micromamba() -> None:
    """`asv` exiting 127 on PATH is the largest defect found this session.

    The first draft of BATTERY_COMMANDS reproduced it in the tool meant to
    detect it: bare `asv`, plus two absolute paths to files that do not exist
    in any image. All three were caught by running one docker command.
    """
    for name, argv in BATTERY_COMMANDS:
        joined = " ".join(argv)
        for binary in ("asv", "pytest", "pip"):
            if f" {binary} " in joined or joined.endswith(f" {binary}"):
                assert "micromamba run -n" in joined, f"{name} calls {binary} outside the env"


def test_no_battery_command_references_a_path_that_does_not_exist_in_the_image() -> None:
    """These two were invented. Neither is in any container we build."""
    joined = " ".join(" ".join(argv) for _, argv in BATTERY_COMMANDS)
    assert "/formulacode_testrunner.py" not in joined
    assert "count_source_benchmarks.py" not in joined
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_battery.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive.battery'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/battery.py
"""What the verifier RUNS inside the container under test.

Returns FACTS. It never decides anything -- severity.py does that, and the
verifier agent reasons in between.

A command that crashes yields a fact with crashed=True and a non-zero rc. The
caller turns that into a FAILING check, not a skipped one. That departs from
docker/manifest.py's three-valued convention on purpose: that convention is
right for a manifest read against an image that has never run, and wrong here,
because the verifier chose to run this command and failure to execute is a
finding about the container.
"""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DATASMITH_PV_BATTERY_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_BATTERY_TIMEOUT_S", "1800"))

# Each entry is (fact_name, argv). argv runs inside the image via the runner.
# Nothing here writes: the verifier's posture is read-only.
#
# EVERY command below was checked against formulacode/networkx-networkx:8148.
# The first draft of this table was written blind and three of seven entries
# were wrong:
#
#   /formulacode_testrunner.py            does not exist (run-tests.sh writes
#                                         it into cwd at container runtime)
#   /opt/formulacode/count_source_...py   never existed anywhere
#   bare `asv`                            NOT ON PATH -- the single largest
#                                         defect found this session, reproduced
#                                         in the tool meant to detect it
#
# Confirmed present: ENV_NAME=asv_3.12, CONF_NAME=./benchmarks/asv.conf.json,
# IMPORT_NAME, REPO_ROOT=/workspace/repo, /opt/formulacode/build_manifest.json,
# and asv at /opt/conda/envs/$ENV_NAME/bin/asv.
#
# So every env-only binary goes through `micromamba run -n "$ENV_NAME"`.
_IN_ENV = 'cd "$REPO_ROOT" && micromamba run -n "$ENV_NAME"'

# Counts asv-convention benchmark functions from SOURCE. Inlined rather than
# calling a script, because no such script exists in the image and parsing
# imports nothing -- a benchmark module with a missing optional dependency
# still counts.
_COUNT_SOURCE = r"""
import ast, json, os, sys
conf = os.environ.get("CONF_NAME") or ""
if not os.path.isabs(conf):
    conf = os.path.join(os.environ.get("REPO_ROOT", "/workspace/repo"), conf)
try:
    raw = open(conf, encoding="utf-8", errors="replace").read()
    stripped = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("//"))
    bench = json.loads(stripped).get("benchmark_dir") or ""
    if not os.path.isabs(bench):
        bench = os.path.join(os.path.dirname(conf), bench)
    pref = ("time_", "mem_", "peakmem_", "track_", "timeraw_")
    n = 0
    for root, _d, files in os.walk(bench):
        for name in files:
            if not name.endswith(".py"):
                continue
            try:
                tree = ast.parse(open(os.path.join(root, name), encoding="utf-8", errors="replace").read())
            except Exception:
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(pref):
                    n += 1
    print(n)
except Exception as exc:
    print("ERR: %s" % exc)
"""

BATTERY_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("pytest_collect", ("bash", "-lc", f'{_IN_ENV} python -m pytest --collect-only -q --no-header -p no:cacheprovider 2>&1 | tail -40')),
    ("pytest_run", ("bash", "-lc", f'{_IN_ENV} python -m pytest -q --no-header -p no:cacheprovider --continue-on-collection-errors 2>&1 | tail -80')),
    ("asv_discover", ("bash", "-lc", f'{_IN_ENV} asv run --bench just-discover --config "$CONF_NAME" --python=same 2>&1 | tail -20')),
    ("source_benchmark_count", ("bash", "-lc", f'{_IN_ENV} python -c {_COUNT_SOURCE!r} 2>&1 | tail -3')),
    ("import_sweep", ("bash", "-lc", f'{_IN_ENV} python -c "import os, importlib; importlib.import_module(os.environ[\'IMPORT_NAME\'])" 2>&1 | tail -20')),
    ("pip_freeze", ("bash", "-lc", f'{_IN_ENV} python -m pip freeze 2>&1')),
    ("build_manifest", ("cat", "/opt/formulacode/build_manifest.json")),
)


@dataclass(frozen=True)
class BatteryFact:
    name: str
    command: str
    stdout: str
    stderr: str
    rc: int
    crashed: bool


def _docker_runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
    proc = subprocess.run(  # noqa: S603
        ["docker", "run", "--rm", "--entrypoint", argv[0], image_tag, *argv[1:]],
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    return proc.stdout, proc.stderr, proc.returncode


def run_battery(
    image_tag: str,
    runner: Callable[[str, list[str], int], tuple[str, str, int]] | None = None,
    timeout_s: int = DATASMITH_PV_BATTERY_TIMEOUT_S,
) -> list[BatteryFact]:
    """Run every battery command and return one fact each.

    Never raises. A crash becomes a fact, because the alternative is an
    exception that some caller eventually swallows into an acceptance.
    """
    run = runner or _docker_runner
    facts: list[BatteryFact] = []
    for name, argv in BATTERY_COMMANDS:
        command = " ".join(argv)
        try:
            stdout, stderr, rc = run(image_tag, list(argv), timeout_s)
            facts.append(BatteryFact(name=name, command=command, stdout=stdout, stderr=stderr, rc=rc, crashed=False))
        except Exception as exc:  # noqa: BLE001 -- a crash is a finding, not a raise
            logger.warning("battery command %s crashed on %s: %s", name, image_tag, exc)
            facts.append(
                BatteryFact(name=name, command=command, stdout="", stderr=f"{type(exc).__name__}: {exc}", rc=-1, crashed=True)
            )
    return facts
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_battery.py -v && uv run mypy`
Expected: 5 passed, mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/reflexive/battery.py tests/agents/reflexive/test_battery.py
git commit -m "feat(reflexive): the verifier's fact-collecting battery

Returns facts, never verdicts. A crashing command yields crashed=True with
a non-zero rc rather than raising, because an exception is something a
caller eventually swallows into an acceptance.

That is deliberately NOT docker/manifest.py's three-valued convention. An
absent input should skip when reading a manifest from an image that has
never run. Here the verifier chose to run the command, so failure to
execute is a finding about the container.

The runner is injected so tests never need Docker, and a test asserts the
shipped argv contains no write, redirect, or reference to the producer's
scripts."
```

---

## Task 4: Verifier

**Files:**
- Create: `src/datasmith/agents/reflexive/verifier.py`
- Test: `tests/agents/reflexive/test_verifier.py`

**Interfaces:**
- Consumes: `run_battery`, `BatteryFact` (Task 3); `RejectionReport`, `Verdict`, `Cause` (Task 1); `grade`, `GradedReport` (Task 2); `InstalledAgent`, `AgentResult` from `datasmith.agents.installed.base`.
- Produces: `parse_report(raw: str) -> RejectionReport | None`, `verify(image_tag: str | None, build_log: str, agent: InstalledAgent, mode: str, runner=None) -> GradedReport`, `DATASMITH_PV_AGENT_TIMEOUT_S: int`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_verifier.py
"""Parsing and grading the verifier agent's reply.

An error is a rejection, never an acceptance. Every failure path below must
end in accepted=False. The predecessor inverted this once: a host-side timeout
returned success and silently verified about 34% of candidate_containers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from datasmith.agents.installed.base import AgentResult
from datasmith.agents.reflexive.verifier import parse_report, verify

_GOOD = (
    '{"verdict":"reject","mode":"container_built","checks":'
    '[{"id":"pytest_collect","verdict":"fail","cause":"pytest_version_incompat",'
    '"evidence":"parametrize names (1)","remedy":"pin pytest"}],"evidence_you_lack":[]}'
)
_CLEAN = '{"verdict":"accept","mode":"container_built","checks":[],"evidence_you_lack":[]}'


class TestParseReport:
    def test_plain_json_parses(self) -> None:
        report = parse_report(_GOOD)
        assert report is not None
        assert report.checks[0].id == "pytest_collect"

    def test_a_fenced_code_block_parses(self) -> None:
        """Agents add fences even when told not to."""
        assert parse_report(f"```json\n{_GOOD}\n```") is not None

    def test_json_with_surrounding_prose_parses(self) -> None:
        assert parse_report(f"Here is my verdict:\n{_GOOD}\nLet me know.") is not None

    def test_garbage_returns_none_rather_than_raising(self) -> None:
        assert parse_report("I could not determine a verdict.") is None

    def test_valid_json_of_the_wrong_shape_returns_none(self) -> None:
        assert parse_report('{"hello":"world"}') is None


class TestVerify:
    @staticmethod
    def _agent(output: str, success: bool = True) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=success, output=output)
        return agent

    @staticmethod
    def _runner(*_a, **_k):
        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            return ("ok", "", 0)

        return runner

    def test_a_clean_report_is_accepted(self) -> None:
        graded = verify("img:1", "", self._agent(_CLEAN), mode="container_built", runner=self._runner())
        assert graded.accepted is True

    def test_a_hard_failure_is_rejected(self) -> None:
        graded = verify("img:1", "", self._agent(_GOOD), mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert graded.hard_failures == ("pytest_collect",)

    def test_unparseable_output_retries_once_then_rejects(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [AgentResult(success=True, output="nonsense"), AgentResult(success=True, output="still nonsense")]
        graded = verify("img:1", "", agent, mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert agent.exec.call_count == 2, "exactly one retry"

    def test_the_retry_succeeds_if_the_second_reply_is_valid(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [AgentResult(success=True, output="nonsense"), AgentResult(success=True, output=_CLEAN)]
        assert verify("img:1", "", agent, mode="container_built", runner=self._runner()).accepted is True

    def test_an_agent_that_fails_outright_is_rejected(self) -> None:
        agent = self._agent("", success=False)
        assert verify("img:1", "", agent, mode="container_built", runner=self._runner()).accepted is False

    def test_an_agent_that_raises_is_rejected_not_propagated(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = RuntimeError("agent process died")
        graded = verify("img:1", "", agent, mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert any("agent process died" in c.evidence for c in graded.report.checks)

    def test_a_crashed_battery_command_forces_a_rejection_regardless_of_the_agent(self) -> None:
        """The agent says accept; a battery command could not run. Reject."""

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            raise RuntimeError("docker daemon went away")

        graded = verify("img:1", "", self._agent(_CLEAN), mode="container_built", runner=runner)
        assert graded.accepted is False

    def test_build_failed_mode_does_not_run_the_battery(self) -> None:
        """Mode A has no image. Running a battery against None would crash."""
        called = []

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            called.append(argv)
            return ("", "", 0)

        verify(None, "BackendUnavailable: hatchling.build", self._agent(_CLEAN), mode="build_failed", runner=runner)
        assert called == [], "no battery in build_failed mode"

    def test_the_prompt_carries_the_build_log_in_build_failed_mode(self) -> None:
        agent = self._agent(_CLEAN)
        verify(None, "SENTINEL_LOG_LINE", agent, mode="build_failed", runner=self._runner())
        assert "SENTINEL_LOG_LINE" in agent.exec.call_args[0][0]

    def test_the_prompt_never_contains_the_producer_scripts_as_writable(self) -> None:
        """Read-only copies are fine; an instruction to edit is not."""
        agent = self._agent(_CLEAN)
        verify("img:1", "", agent, mode="container_built", runner=self._runner())
        prompt = agent.exec.call_args[0][0]
        assert "never edit" in prompt.lower() or "read-only" in prompt.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_verifier.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive.verifier'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/verifier.py
"""The verifier half: collect facts, ask the agent, grade the answer.

The governing rule is that an ERROR IS A REJECTION, NEVER AN ACCEPTANCE. Every
failure path in this module ends in accepted=False. The predecessor inverted
this once -- a host-side timeout returned success, which silently verified
about 34% of candidate_containers.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable

from datasmith.agents.installed.base import InstalledAgent
from datasmith.agents.reflexive.battery import BatteryFact, run_battery
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import GradedReport, grade

logger = logging.getLogger(__name__)

DATASMITH_PV_AGENT_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_AGENT_TIMEOUT_S", "1800"))

_JSON_OBJECT = re.compile(r"\{.*\}", re.DOTALL)

_PROMPT = """You are a container build VERIFIER.

You judge whether a built container is sound. You NEVER edit build scripts --
copies are shown to you read-only, for diagnosis only. A separate producer
agent owns them.

Ignore hardware-specific limitations: a missing GPU, a specific CPU feature, or
any facility this host cannot provide. Judge the BUILD, not the machine.

Report the observed CAUSE of each failure. Do not assign severity -- that is
graded downstream. Valid causes:
  module_not_found                  nothing is installed under that name
  import_raised_on_host_facility    installed, but raises for want of a host facility
  pytest_version_incompat           imports fine; pytest rejects the test's own syntax
  other                             anything else

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
        except Exception:  # noqa: BLE001 -- a bad reply is data, not an error
            continue
    return None


def _reject(check_id: str, evidence: str, mode: str) -> GradedReport:
    """Build a rejection out of thin air, for paths with no agent answer."""
    report = RejectionReport(
        verdict=Verdict.REJECT,
        mode=mode,
        checks=[
            CheckResult(id=check_id, verdict="fail", cause=Cause.OTHER, evidence=evidence[:4000], remedy="")
        ],
    )
    return grade(report)


def _facts_block(facts: list[BatteryFact]) -> str:
    parts = []
    for fact in facts:
        head = f"### {fact.name}  (rc={fact.rc}{', CRASHED' if fact.crashed else ''})\n$ {fact.command}"
        parts.append(f"{head}\n{fact.stdout[-4000:]}\n{fact.stderr[-2000:]}")
    return "\n\n".join(parts)


def verify(
    image_tag: str | None,
    build_log: str,
    agent: InstalledAgent,
    mode: str,
    runner: Callable[[str, list[str], int], tuple[str, str, int]] | None = None,
    timeout_s: int = DATASMITH_PV_AGENT_TIMEOUT_S,
) -> GradedReport:
    """Verify one build. `mode` is "container_built" or "build_failed"."""
    facts: list[BatteryFact] = []
    if mode == "build_failed" or image_tag is None:
        context = f"The build FAILED, so no container exists. BUILD LOG:\n{build_log[-12000:]}"
    else:
        facts = run_battery(image_tag, runner=runner)
        context = f"Evidence collected from the container:\n\n{_facts_block(facts)}"

    prompt = _PROMPT.format(context=context, mode=mode)

    raw = ""
    for attempt in (1, 2):
        try:
            result = agent.exec(prompt if attempt == 1 else prompt + "\nYour last reply was not valid JSON. Reply with ONLY the JSON object.", timeout=timeout_s)
        except Exception as exc:  # noqa: BLE001 -- an agent crash is a rejection
            logger.error("verifier agent raised: %s", exc)
            return _reject("verifier_agent_error", f"{type(exc).__name__}: {exc}", mode)
        if not result.success:
            return _reject("verifier_agent_failed", result.error or "agent reported failure", mode)
        raw = result.output
        report = parse_report(raw)
        if report is not None:
            graded = grade(report)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_verifier.py -v && uv run mypy`
Expected: 15 passed, mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/reflexive/verifier.py tests/agents/reflexive/test_verifier.py
git commit -m "feat(reflexive): the verifier half, fail-closed on every path

Collects facts via the battery, prompts the verifier agent, parses and
grades the reply. Every failure path ends in accepted=False: agent raised,
agent failed, reply unparseable after one retry, or a battery command
crashed. The predecessor inverted exactly this once, and a host-side
timeout returning success silently verified about 34% of
candidate_containers.

parse_report tolerates code fences and surrounding prose because agents add
them even when told not to, and returns None rather than raising on
anything else.

Mode A does not run the battery at all -- there is no image to run it
against -- and puts the build log in the prompt instead."
```

---

## Task 5: Producer

**Files:**
- Create: `src/datasmith/agents/reflexive/producer.py`
- Test: `tests/agents/reflexive/test_producer.py`

**Interfaces:**
- Consumes: `ProducerPlan`, `PlannedEdit`, `RejectionReport`, `PRODUCER_OWNED_FILES` (Task 1); `GradedReport` (Task 2); `InstalledAgent` from `datasmith.agents.installed.base`; `DockerContext` from `datasmith.docker.context`.
- Produces: `parse_plan(raw: str) -> ProducerPlan | None`, `revise(context: DockerContext, graded: GradedReport, agent: InstalledAgent, workdir: Path) -> tuple[DockerContext | None, ProducerPlan | None]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_producer.py
"""The producer half: consume a report, edit only the two scripts it owns.

The producer never sees the verifier's reasoning -- only the typed report and
its evidence. Keeping it out of the deliberation is what protects both
contexts; keeping it short of evidence would only make it guess.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from datasmith.agents.installed.base import AgentResult
from datasmith.agents.reflexive.producer import parse_plan, revise
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import grade
from datasmith.docker.context import DockerContext

_PLAN = (
    '{"understood_rejections":["backend missing"],'
    '"planned_edits":[{"file":"docker_build_pkg.sh","change":"install hatchling first",'
    '"addresses_check_id":"pep517_backend"}],"evidence_requests":[]}'
)


def _graded():
    return grade(
        RejectionReport(
            verdict=Verdict.REJECT,
            mode="build_failed",
            checks=[
                CheckResult(
                    id="pep517_backend",
                    verdict="fail",
                    cause=Cause.MODULE_NOT_FOUND,
                    evidence="Cannot import 'hatchling.build'",
                    remedy="install hatchling",
                )
            ],
        )
    )


class TestParsePlan:
    def test_plain_json_parses(self) -> None:
        plan = parse_plan(_PLAN)
        assert plan is not None
        assert plan.planned_edits[0].file == "docker_build_pkg.sh"

    def test_a_fenced_block_parses(self) -> None:
        assert parse_plan(f"```json\n{_PLAN}\n```") is not None

    def test_garbage_returns_none(self) -> None:
        assert parse_plan("I will fix it, trust me.") is None

    def test_a_plan_editing_a_forbidden_file_returns_none(self) -> None:
        """docker_build_final.sh holds the sealer. Out of bounds."""
        bad = _PLAN.replace("docker_build_pkg.sh", "docker_build_final.sh")
        assert parse_plan(bad) is None


class TestRevise:
    @staticmethod
    def _agent(output: str, success: bool = True, files: list[str] | None = None) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=success, output=output, files_changed=files or [])
        return agent

    def test_the_prompt_contains_the_report_evidence(self, tmp_path: Path) -> None:
        agent = self._agent(_PLAN)
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        revise(DockerContext(), _graded(), agent, tmp_path)
        prompt = agent.exec.call_args[0][0]
        assert "Cannot import 'hatchling.build'" in prompt

    def test_the_prompt_never_contains_verifier_reasoning(self, tmp_path: Path) -> None:
        """Only the typed report crosses the boundary."""
        agent = self._agent(_PLAN)
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        revise(DockerContext(), _graded(), agent, tmp_path)
        prompt = agent.exec.call_args[0][0].lower()
        for leak in ("transcript", "reasoning", "chain of thought"):
            assert leak not in prompt

    def test_edited_scripts_are_read_back_into_the_context(self, tmp_path: Path) -> None:
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\nEDITED_BY_PRODUCER\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, plan = revise(DockerContext(), _graded(), self._agent(_PLAN), tmp_path)
        assert context is not None
        assert "EDITED_BY_PRODUCER" in context.build_pkg_sh
        assert plan is not None

    def test_an_agent_that_fails_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, plan = revise(DockerContext(), _graded(), self._agent("", success=False), tmp_path)
        assert context is None

    def test_an_agent_that_raises_returns_none_rather_than_propagating(self, tmp_path: Path) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = RuntimeError("agent died")
        context, plan = revise(DockerContext(), _graded(), agent, tmp_path)
        assert context is None
        assert plan is None

    def test_an_unparseable_plan_still_picks_up_script_edits(self, tmp_path: Path) -> None:
        """The scripts on disk are the deliverable. The plan is commentary."""
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\nEDITED\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, plan = revise(DockerContext(), _graded(), self._agent("no json here"), tmp_path)
        assert context is not None and "EDITED" in context.build_pkg_sh
        assert plan is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_producer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive.producer'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/producer.py
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
        except Exception:  # noqa: BLE001 -- a bad reply is data, not an error
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
    except Exception as exc:  # noqa: BLE001 -- an agent crash ends the round
        logger.error("producer agent raised: %s", exc)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_producer.py -v && uv run mypy`
Expected: 11 passed, mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/reflexive/producer.py tests/agents/reflexive/test_producer.py
git commit -m "feat(reflexive): the producer half, owning exactly two scripts

Receives the typed report with its full evidence and never the verifier's
reasoning. Keeping the producer out of the deliberation is what protects
both contexts; keeping it short of evidence would only make it guess, so
the envelope is typed and the payload is not bounded.

A plan naming any file outside docker_build_pkg.sh and docker_build_run.sh
fails PlannedEdit validation and parses to None, so the sealer and the
final stage are unreachable by construction.

The scripts on disk are the deliverable and the JSON plan is commentary:
an unparseable plan still yields a revised context, because discarding an
edit the agent actually made would waste a whole rebuild round."
```

---

## Task 6: Loop

**Files:**
- Create: `src/datasmith/agents/reflexive/loop.py`
- Test: `tests/agents/reflexive/test_loop.py`

**Interfaces:**
- Consumes: everything from Tasks 1-5; `_signature` from `scripts/prepass_trial.py` (re-implemented locally — see note); `verify_context`, `SandboxResult` from `datasmith.agents.sandbox`.
- Produces: `LoopOutcome(accepted: bool, rounds: int, context: DockerContext | None, reports: list[GradedReport], stop_reason: str)`, `run_loop(...) -> LoopOutcome`, `progress_key(graded: GradedReport, build_log: str) -> tuple[str, ...]`, tunables `DATASMITH_PV_MAX_ROUNDS`, `DATASMITH_PV_ENABLED`.

**Note on `_signature`:** the spec requires Mode A to compare the deterministic build-log signature rather than agent-invented check ids. `scripts/` is not an importable package, so `progress_key` re-implements the same two-stage scan (named causes first, then last non-noise line) inside `loop.py`. Task 6's tests assert both implementations agree on the five cases in `tests/scripts/test_prepass_signature.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_loop.py
"""Rounds, budget, no-progress, fail-closed.

Rule 3 (no progress) is the one that matters. A build costs 300 to 700
seconds, so a round that cannot learn anything must never be spent.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from datasmith.agents.reflexive.loop import LoopOutcome, progress_key, run_loop
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import grade
from datasmith.docker.context import DockerContext


def _graded(*check_ids: str):
    return grade(
        RejectionReport(
            verdict=Verdict.REJECT if check_ids else Verdict.ACCEPT,
            mode="container_built",
            checks=[
                CheckResult(id=c, verdict="fail", cause=Cause.MODULE_NOT_FOUND, evidence="e", remedy="r")
                for c in check_ids
            ],
        )
    )


class TestProgressKey:
    def test_mode_b_uses_the_hard_failure_ids(self) -> None:
        assert progress_key(_graded("a", "b"), build_log="") == ("a", "b")

    def test_ids_are_order_independent(self) -> None:
        assert progress_key(_graded("b", "a"), "") == progress_key(_graded("a", "b"), "")

    def test_mode_a_uses_the_build_log_signature_not_invented_ids(self) -> None:
        """Agent-invented ids are unstable: the same model said 'pytest-suite'
        in one run and 'pep517_editable_backend_import' in another."""
        log = "2.15 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n------"
        a = grade(RejectionReport(verdict=Verdict.REJECT, mode="build_failed",
                                  checks=[CheckResult(id="pytest-suite", verdict="fail", cause=Cause.OTHER, evidence="e", remedy="")]))
        b = grade(RejectionReport(verdict=Verdict.REJECT, mode="build_failed",
                                  checks=[CheckResult(id="pep517_editable_backend_import", verdict="fail", cause=Cause.OTHER, evidence="e", remedy="")]))
        assert progress_key(a, log) == progress_key(b, log), "same log means no progress, whatever the agent called it"

    def test_a_different_build_log_is_progress(self) -> None:
        a = _graded("x")
        assert progress_key(a, "ModuleNotFoundError: No module named 'salem'") != progress_key(a, "ModuleNotFoundError: No module named 'dask'")


class TestRunLoop:
    @staticmethod
    def _build(results: list[tuple[bool, str]]):
        calls = {"n": 0}

        def build(context: DockerContext) -> tuple[bool, str | None, str]:
            i = min(calls["n"], len(results) - 1)
            calls["n"] += 1
            ok, log = results[i]
            return ok, ("img:1" if ok else None), log

        build.calls = calls  # type: ignore[attr-defined]
        return build

    def test_an_immediately_clean_build_accepts_in_one_round(self) -> None:
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(True, "")]),
            verify=lambda image, log, mode: _graded(),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is True
        assert outcome.rounds == 1
        assert outcome.stop_reason == "accepted"

    def test_a_fixed_build_accepts_on_the_second_round(self) -> None:
        verdicts = [_graded("pep517_backend"), _graded()]
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "BackendUnavailable"), (True, "")]),
            verify=lambda image, log, mode: verdicts.pop(0),
            revise=lambda ctx, graded: (ctx.model_copy(update={"build_pkg_sh": "fixed"}), None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is True
        assert outcome.rounds == 2

    def test_no_progress_stops_before_the_budget_is_spent(self) -> None:
        """Same hard failures twice. Stop -- a third build learns nothing."""
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "same log")]),
            verify=lambda image, log, mode: _graded("pep517_backend"),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
            max_rounds=5,
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "no_progress"
        assert outcome.rounds == 2, "round 1 establishes the key, round 2 repeats it"

    def test_the_budget_stops_a_loop_that_keeps_changing(self) -> None:
        logs = [(False, f"distinct failure {i}") for i in range(10)]
        outcome = run_loop(
            context=DockerContext(),
            build=self._build(logs),
            verify=lambda image, log, mode: _graded(f"check_{log}"),
            revise=lambda ctx, graded: (ctx.model_copy(update={"build_pkg_sh": log_marker()}), None),
            workdir=Path("/tmp"),
            max_rounds=3,
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "budget"
        assert outcome.rounds == 3

    def test_a_producer_that_cannot_revise_ends_the_loop(self) -> None:
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "boom")]),
            verify=lambda image, log, mode: _graded("x"),
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "producer_failed"

    def test_a_verify_that_raises_is_a_rejection_not_a_crash(self) -> None:
        def boom(image, log, mode):
            raise RuntimeError("verifier exploded")

        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(True, "")]),
            verify=boom,
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is False

    def test_build_failure_uses_build_failed_mode(self) -> None:
        seen: list[str] = []

        def verify(image, log, mode):
            seen.append(mode)
            return _graded("x")

        run_loop(
            context=DockerContext(),
            build=self._build([(False, "boom")]),
            verify=verify,
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert seen == ["build_failed"]


def log_marker() -> str:
    """Distinct script text per round, so the context genuinely changes."""
    import uuid

    return f"# {uuid.uuid4()}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_loop.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'datasmith.agents.reflexive.loop'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/datasmith/agents/reflexive/loop.py
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
_NOISE = ("ERROR: failed to build", "------", "> [", "note: This is an issue", "hint: See above",
          "FORMULACODE_", "{", "}", "=====", "!!!!!")


def _signature(build_log: str) -> str:
    """The line that says WHY. Deterministic, unlike agent-invented ids."""
    lines = [ln.strip() for ln in (build_log or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        if any(marker in ln for marker in _NAMED_CAUSES):
            return ln[:110]
    for ln in reversed(lines):
        if any(ln.startswith(n) for n in _NOISE):
            continue
        body = re.sub(r"^[#\d.\s]+", "", ln)
        if len(body) > 12:
            return body[:110]
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
        except Exception as exc:  # noqa: BLE001 -- an error is a rejection
            logger.error("verify raised in round %d: %s", round_index, exc)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_loop.py -v && uv run mypy`
Expected: 11 passed, mypy `Success`

- [ ] **Step 5: Add the cross-implementation agreement test**

```python
# append to tests/agents/reflexive/test_loop.py
def test_the_duplicated_signature_agrees_with_the_prepass_one() -> None:
    """loop.py duplicates scripts/prepass_trial.py's scan because scripts/ is
    not importable. The two must not drift."""
    import importlib.util
    import sys
    from pathlib import Path as _P

    from datasmith.agents.reflexive.loop import _signature as loop_sig

    root = _P(__file__).parents[3]
    spec = importlib.util.spec_from_file_location("_pp", root / "scripts" / "prepass_trial.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_pp"] = mod
    spec.loader.exec_module(mod)

    cases = [
        "2.15 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n------",
        "!!!!!! Interrupted: 2 errors during collection !!!!!!\nFORMULACODE_SNAPSHOT_END",
        "8.6 ModuleNotFoundError: No module named 'salem'\n------",
        "0.3 fatal: reference is not a tree: abc123\n------",
        "5.5 requirements are unsatisfiable.\n------",
    ]
    for case in cases:
        assert loop_sig(case) == mod._signature({"error_message": case}), f"drifted on {case[:40]!r}"
```

Run: `uv run pytest tests/agents/reflexive/test_loop.py -v`
Expected: 12 passed

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/agents/reflexive/loop.py tests/agents/reflexive/test_loop.py
git commit -m "feat(reflexive): the loop, with no-progress detection that can fire

Three stopping rules: accept, budget, no progress. The third matters most
because a build costs 300 to 700 seconds and a round that cannot learn
anything must never be spent.

progress_key is mode-dependent, and that is the point. Mode B compares
hard-failure ids, which the battery defines and are canonical. Mode A
compares the build-log signature instead, because there the ids come from
the agent and are NOT stable: the same model called a check 'pytest-suite'
in one run and 'pep517_editable_backend_import' in another, so comparing
invented ids would let the rule never fire in the mode that most needs it.

A verify that raises ends the loop as a rejection rather than propagating.

The signature scan is duplicated from scripts/prepass_trial.py because
scripts/ is not an importable package, and a test asserts the two agree on
five real failure shapes so they cannot drift."
```

---

## Task 7: Build without the legacy test gate

**Files:**
- Modify: `src/datasmith/agents/sandbox.py` (`SandboxResult` at line 94, `verify_context` at line 534)
- Test: `tests/agents/reflexive/test_build_only.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `SandboxResult.image_tag: str = ""`, and `verify_context(..., run_tests_gate: bool = True)`.

**Why this task exists.** `verify_context` runs `local_ci.py`, which runs
`run_tests`, which fails on `rc != 0`. If `PRODUCE_VERIFY` used it unchanged, a
fluids-shaped container would come back `success=False` from the LEGACY pytest
gate, the loop would see `build_failed`, and the verifier would never receive
an image to run its battery against. It could never waive anything, and the
soft column would be unreachable for the exact case that motivated it. That is
the Section 4 / Section 12 contradiction the spec settled; this task is how the
code honours it.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_build_only.py
"""PRODUCE_VERIFY builds without the legacy `rc != 0` pytest gate.

In that path pytest runs only in the verifier's battery, and severity.py
grades the verdict. The legacy gate still applies to TRY_SIMILAR and
TRY_DEFAULT, whose behaviour must not change.
"""

from __future__ import annotations

import inspect

from datasmith.agents.sandbox import SandboxResult, verify_context


def test_sandbox_result_carries_the_image_tag() -> None:
    """The loop must never guess the tag. verify_context serves TRY_SIMILAR
    and does not necessarily tag what another caller would assume."""
    assert "image_tag" in SandboxResult.__dataclass_fields__
    assert SandboxResult().image_tag == ""


def test_verify_context_accepts_run_tests_gate() -> None:
    params = inspect.signature(verify_context).parameters
    assert "run_tests_gate" in params


def test_the_gate_defaults_to_on_so_legacy_callers_are_unchanged() -> None:
    """TRY_SIMILAR and TRY_DEFAULT must behave exactly as before."""
    assert inspect.signature(verify_context).parameters["run_tests_gate"].default is True


def test_local_ci_is_told_to_skip_the_gate_when_asked() -> None:
    source = inspect.getsource(verify_context)
    assert "run_tests_gate" in source
    assert "--skip-test-gate" in source, "the flag must reach local_ci.py"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_build_only.py -v`
Expected: FAIL — `image_tag` is not a field and `run_tests_gate` is not a parameter.

- [ ] **Step 3: Write minimal implementation**

In `src/datasmith/agents/sandbox.py`, add to `SandboxResult` (after `aborted`):

```python
    # The tag the build actually produced. Callers must never reconstruct it:
    # verify_context serves TRY_SIMILAR and does not necessarily tag what
    # another caller would guess.
    image_tag: str = ""
```

Change the signature of `verify_context`:

```python
def verify_context(
    owner: str,
    repo: str,
    sha: str,
    repo_image: str,
    env_payload: str,
    python_version: str,
    context: DockerContext,
    timeout_s: int = 3600,
    base_sha: str = "",
    solution_patch: str = "",
    run_tests_gate: bool = True,
) -> SandboxResult:
```

Extend the docstring:

```
    ``run_tests_gate=False`` builds the image and seals the manifest but does
    NOT fail on pytest's exit code. PRODUCE_VERIFY needs that: in its path
    pytest runs only in the verifier's battery and severity.py grades the
    verdict. Leaving the gate on there would reject a container before the
    verifier could weigh it, which is the contradiction the design settled.
    Defaults to True so TRY_SIMILAR and TRY_DEFAULT are unchanged.
```

Where `local_ci.py` is invoked, append the flag:

```python
        local_ci_argv = [sys.executable, str(workspace / "local_ci.py"), "--task", str(task_dir)]
        if not run_tests_gate:
            local_ci_argv.append("--skip-test-gate")
```

and use `local_ci_argv` in the `subprocess.run` call.

In `src/datasmith/agents/templates/local_ci.py`, add the argument to its parser and thread it into the `run_tests` verdict:

```python
    parser.add_argument(
        "--skip-test-gate",
        action="store_true",
        help=(
            "Build and seal, but do not fail on pytest's exit code. Used by "
            "PRODUCE_VERIFY, where pytest runs in the verifier's battery and "
            "severity.py grades the verdict instead."
        ),
    )
```

At the `if not ok:` check following `run_tests`, guard it:

```python
    # The manifest merge above still happens: skipping the GATE must not skip
    # collecting the facts, or PRODUCE_VERIFY would accept a container whose
    # manifest is empty.
    if not ok and not args.skip_test_gate:
```

Finally, set the tag on the successful return:

```python
            image_tag=tag,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_build_only.py -v && uv run pytest tests/docker -q -m "not slow" && uv run mypy`
Expected: 4 passed, docker suite unchanged (266 passed at last count), mypy `Success`

- [ ] **Step 5: Commit**

```bash
git add src/datasmith/agents/sandbox.py src/datasmith/agents/templates/local_ci.py tests/agents/reflexive/test_build_only.py
git commit -m "feat(sandbox): build without the legacy pytest gate, and report the tag

verify_context runs local_ci.py, which runs run_tests, which fails on
rc != 0. PRODUCE_VERIFY cannot use that unchanged: a fluids-shaped
container would come back success=False from the LEGACY gate, the loop
would see build_failed, and the verifier would never receive an image to
run its battery against -- so it could never waive anything and the soft
column would be unreachable for the case that motivated it.

run_tests_gate defaults to True, so TRY_SIMILAR and TRY_DEFAULT are
unchanged. Skipping the GATE does not skip collecting the facts: the
manifest merge still runs, or PRODUCE_VERIFY would accept containers with
an empty manifest.

SandboxResult now carries image_tag. Callers must not reconstruct it --
verify_context serves TRY_SIMILAR and does not necessarily tag what
another caller would guess."
```

---

## Task 8: Wire into the synthesizer

**Files:**
- Modify: `src/datasmith/agents/synthesizer.py` (the `SynthesisState` enum at line 35, and the `LLM_GENERATE` block starting line 287)
- Test: `tests/agents/reflexive/test_synthesizer_integration.py`

**Interfaces:**
- Consumes: `run_loop`, `LoopOutcome`, `DATASMITH_PV_ENABLED` (Task 6); `verify`, `revise` (Tasks 4-5); `run_tests_gate`, `SandboxResult.image_tag` (Task 7).
- Produces: `SynthesisState.PRODUCE_VERIFY = "produce_verify"`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_synthesizer_integration.py
"""PRODUCE_VERIFY replaces LLM_GENERATE, behind a flag that is off by default.

The states before it are untouched, so a repository the stock template already
builds never invokes an agent at all.
"""

from __future__ import annotations

import importlib

from datasmith.agents.synthesizer import SynthesisState


def test_the_new_state_exists() -> None:
    assert SynthesisState.PRODUCE_VERIFY.value == "produce_verify"


def test_the_flag_is_off_by_default() -> None:
    import datasmith.agents.reflexive.loop as loop_mod

    importlib.reload(loop_mod)
    assert loop_mod.DATASMITH_PV_ENABLED is False, "must not go live without an explicit opt-in"


def test_the_legacy_states_are_unchanged() -> None:
    """TRY_DEFAULT now succeeds without an agent for a growing share of
    repositories, and that path must keep costing nothing."""
    for name in ("CHECK_CACHE", "FIND_SIMILAR", "TRY_SIMILAR", "TRY_DEFAULT", "FAIL"):
        assert hasattr(SynthesisState, name)


def test_llm_generate_still_exists_for_the_disabled_path() -> None:
    """With the flag off, the old behaviour must remain reachable."""
    assert SynthesisState.LLM_GENERATE.value == "llm_generate"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_synthesizer_integration.py -v`
Expected: FAIL with `AttributeError: PRODUCE_VERIFY`

- [ ] **Step 3: Write minimal implementation**

In `src/datasmith/agents/synthesizer.py`, add to the enum (after `LLM_GENERATE`):

```python
    PRODUCE_VERIFY = "produce_verify"
```

Then, immediately before the existing `# State: LLM_GENERATE (sandbox-based)` comment block, insert:

```python
        # State: PRODUCE_VERIFY — the reflexive producer/verifier loop.
        #
        # Replaces LLM_GENERATE when DATASMITH_PV_ENABLED is set. The states
        # above are untouched: a repo the stock template already builds never
        # reaches here, and that path costs nothing.
        #
        # In this path pytest runs only in the verifier's battery and
        # severity.py grades the verdict. The legacy `rc != 0` gate in
        # run_tests applies to TRY_SIMILAR and TRY_DEFAULT only -- otherwise a
        # container would be rejected on an exit code before the verifier could
        # weigh it, and the entire soft column would be unreachable.
        from datasmith.agents.reflexive.loop import DATASMITH_PV_ENABLED

        if DATASMITH_PV_ENABLED and self._agent != "none":
            self._trace.append(SynthesisState.PRODUCE_VERIFY)
            outcome = self._run_produce_verify(
                owner=owner,
                repo=repo,
                sha=sha,
                issue_number=issue_number,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                base_sha=base_sha,
                solution_patch=solution_patch,
            )
            if outcome.accepted and outcome.context is not None:
                self._save_context(
                    owner,
                    repo,
                    sha,
                    issue_number,
                    outcome.context,
                    resource_metrics=outcome.resource_metrics,
                    build_manifest=outcome.build_manifest,
                )
                return outcome.context
            logger.info(
                "PRODUCE_VERIFY failed for %s/%s#%d after %d round(s): %s",
                owner,
                repo,
                issue_number,
                outcome.rounds,
                outcome.stop_reason,
            )
            self._trace.append(SynthesisState.FAIL)
            return None
```

Add the method `_run_produce_verify` to the `Synthesizer` class:

```python
    def _run_produce_verify(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        repo_image: str,
        env_payload: str,
        python_version: str,
        base_sha: str,
        solution_patch: str,
    ) -> "LoopOutcome":
        """Drive the reflexive loop for one task.

        Producer and verifier get SEPARATE agent instances so their contexts
        cannot merge, even when both resolve to the same backend.
        """
        import tempfile
        from pathlib import Path

        from datasmith.agents.installed.base import get_agent
        from datasmith.agents.reflexive.loop import LoopOutcome, run_loop
        from datasmith.agents.reflexive.producer import revise as producer_revise
        from datasmith.agents.reflexive.verifier import verify as verifier_verify

        producer_agent = get_agent([os.environ.get("DATASMITH_PV_PRODUCER_AGENT", "codex")])
        verifier_agent = get_agent([os.environ.get("DATASMITH_PV_VERIFIER_AGENT", "codex")])

        # `verify_context` runs local_ci.py, which runs run_tests, which fails
        # on `rc != 0`. Using it here would re-create the exact contradiction
        # the spec settled: a fluids-shaped container would come back
        # success=False from the LEGACY pytest gate, mode would become
        # build_failed, and the verifier would never receive an image to run
        # its battery against -- so it could never waive anything and the whole
        # soft column would be unreachable.
        #
        # So PRODUCE_VERIFY builds the image WITHOUT the test gate. In this
        # path pytest runs only in the verifier's battery.
        last: dict[str, SandboxResult | None] = {"result": None}

        def build(context: DockerContext) -> tuple[bool, str | None, str]:
            result = verify_context(
                owner=owner,
                repo=repo,
                sha=sha,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                context=context,
                base_sha=base_sha,
                solution_patch=solution_patch,
                run_tests_gate=False,
            )
            # Keep the whole result. The manifest is what stage 8 gates on, and
            # a PV-accepted container landing in candidate_containers with a
            # NULL build_manifest is indistinguishable from one built before
            # manifests existed.
            last["result"] = result
            log = json.dumps(result.failure_json or {})[:200000]
            # The tag comes from the result, never assumed. verify_context
            # serves TRY_SIMILAR and does not necessarily tag what this caller
            # would guess.
            tag = result.image_tag if result.success else None
            return bool(result.success), tag, log

        with tempfile.TemporaryDirectory(prefix="fc-pv-") as tmp:
            workdir = Path(tmp)
            context = _load_default_context()
            for filename, field_name in DockerContext._FILE_MAP.items():
                (workdir / filename).write_text(getattr(context, field_name), encoding="utf-8")

            def on_accept() -> tuple[dict | None, dict]:
                result = last["result"]
                if result is None:
                    return None, {}
                return result.build_manifest, result.resource_metrics

            return run_loop(
                context=context,
                build=build,
                verify=lambda image, log, mode: verifier_verify(image, log, verifier_agent, mode),
                revise=lambda ctx, graded: producer_revise(ctx, graded, producer_agent, workdir),
                workdir=workdir,
                on_accept=on_accept,
            )
```

Add `from datasmith.docker.images import get_pr_image_name` and `from datasmith.agents.reflexive.loop import LoopOutcome` to the imports if not present, and ensure `json` and `os` are imported.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/ -v && uv run mypy && uv run ruff check src/ tests/`
Expected: all pass, mypy `Success`, ruff clean

- [ ] **Step 5: Run the whole suite for regressions**

Run: `uv run pytest tests/ -q -m "not slow" -p no:randomly`
Expected: all pass. Baseline before this plan was **1201 passed, 4 deselected**.

- [ ] **Step 6: Commit**

```bash
git add src/datasmith/agents/synthesizer.py tests/agents/reflexive/test_synthesizer_integration.py
git commit -m "feat(stage6): PRODUCE_VERIFY, behind a flag that is off by default

Replaces LLM_GENERATE when DATASMITH_PV_ENABLED is set. Everything before
it is untouched: a repository the stock template already builds never
reaches this state, and that path keeps costing nothing.

Producer and verifier get SEPARATE agent instances so their contexts
cannot merge even when both resolve to the same backend -- which is the
current default, and the reason the independence claim is still marked
untested in the spec.

The comment records the gate boundary the spec settled: in this path
pytest runs only in the verifier's battery and severity.py grades the
verdict, while the legacy rc != 0 gate in run_tests applies to TRY_SIMILAR
and TRY_DEFAULT only. Without that split a container would be rejected on
an exit code before the verifier could weigh it, and the entire soft
column would be unreachable for the case that motivated it."
```

---

## Task 9: Tunables documentation

**Files:**
- Modify: `CLAUDE.md` (the "Tunable constants" section, "Existing uses" list)
- Modify: `docs/superpowers/plans/2026-08-23-prepass-progress.md` (append a status section)
- Test: `tests/agents/reflexive/test_tunables.py`

**Interfaces:**
- Consumes: the tunables defined in Tasks 3, 4, 6.
- Produces: nothing importable.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_tunables.py
"""Every knob is DATASMITH_-prefixed, env-overridable, and documented.

CLAUDE.md requires it, and a knob that is not greppable is a knob nobody finds.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[3]
_PKG = _ROOT / "src" / "datasmith" / "agents" / "reflexive"

EXPECTED = [
    ("DATASMITH_PV_MAX_ROUNDS", "datasmith.agents.reflexive.loop"),
    ("DATASMITH_PV_ENABLED", "datasmith.agents.reflexive.loop"),
    ("DATASMITH_PV_AGENT_TIMEOUT_S", "datasmith.agents.reflexive.verifier"),
    ("DATASMITH_PV_BATTERY_TIMEOUT_S", "datasmith.agents.reflexive.battery"),
]


@pytest.mark.parametrize(("name", "module"), EXPECTED)
def test_the_constant_exists_and_reads_the_environment(name: str, module: str, monkeypatch) -> None:
    mod = importlib.import_module(module)
    assert hasattr(mod, name), f"{module} must define {name}"
    source = Path(mod.__file__).read_text(encoding="utf-8")
    assert f'os.environ.get("{name}"' in source, f"{name} must read the env at module scope"


def test_every_pv_constant_is_documented_in_claude_md() -> None:
    text = (_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    for name, _ in EXPECTED:
        assert name in text, f"{name} is not documented in CLAUDE.md"


def test_no_undocumented_pv_constant_exists() -> None:
    """A knob added later must be documented too."""
    documented = (_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    found: set[str] = set()
    for path in _PKG.glob("*.py"):
        found |= set(re.findall(r'os\.environ\.get\("(DATASMITH_[A-Z0-9_]+)"', path.read_text(encoding="utf-8")))
    missing = sorted(n for n in found if n not in documented)
    assert not missing, f"undocumented tunables: {missing}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_tunables.py -v`
Expected: FAIL on `test_every_pv_constant_is_documented_in_claude_md` — the names are not in `CLAUDE.md` yet.

- [ ] **Step 3: Write minimal implementation**

In `CLAUDE.md`, in the "Tunable constants" → "Existing uses" paragraph, append this sentence to the list:

```markdown
  The producer/verifier loop (stage 6, `agents/reflexive/`) adds
  `DATASMITH_PV_ENABLED`, `DATASMITH_PV_MAX_ROUNDS`,
  `DATASMITH_PV_AGENT_TIMEOUT_S`, `DATASMITH_PV_BATTERY_TIMEOUT_S`,
  `DATASMITH_PV_EVIDENCE_BUDGET`, `DATASMITH_PV_PRODUCER_AGENT` and
  `DATASMITH_PV_VERIFIER_AGENT`. `DATASMITH_PV_ENABLED` is off by default and
  must stay off until the 16-container validation set passes — see
  `docs/superpowers/specs/2026-08-23-producer-verifier-design.md` section 9.
```

Also add a short subsection to the **Pipeline stages** description for stage 6 in `CLAUDE.md`:

```markdown
Stage 6 has two synthesis paths. `TRY_DEFAULT` uses the stock template and
needs no agent. `PRODUCE_VERIFY` (behind `DATASMITH_PV_ENABLED`) runs a
reflexive loop between a producer agent that owns `docker_build_pkg.sh` and
`docker_build_run.sh` and a verifier agent that runs the container and grades
it. Severity is decided in `agents/reflexive/severity.py`, never in the
verifier's prompt.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_tunables.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md tests/agents/reflexive/test_tunables.py
git commit -m "docs(claude): document the producer/verifier tunables

Every knob is DATASMITH_-prefixed and env-overridable per CLAUDE.md, and a
test now fails if a new one is added to agents/reflexive/ without being
documented -- a knob that is not greppable is a knob nobody finds.

DATASMITH_PV_ENABLED is recorded as off by default and required to stay
off until the 16-container validation set passes."
```

---

## Task 10: The validation harness

**Files:**
- Create: `scripts/pv_validate.py`
- Create: `tests/agents/reflexive/test_pv_validate.py`

**Interfaces:**
- Consumes: `verify` (Task 4), `GradedReport` (Task 2).
- Produces: `LabelledCase(task: str, image: str, digest: str, expected: str, note: str)`, `CASES: tuple[LabelledCase, ...]`, `confusion(results) -> dict[str, int]`, `passes_criterion(results) -> tuple[bool, list[str]]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_pv_validate.py
"""The pass criterion, and the negative controls that make it meaningful.

attack-demo:1 carries 19 lines of adversarial sitecustomize that patch
Path.is_file and shutil.which, and it DEFEATED the deterministic honesty gate.
pysindy#139 fails four honesty checks including a replaced grep. A verifier
that accepts either is worse than the script it replaces.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[3]


@pytest.fixture(scope="module")
def pv():
    spec = importlib.util.spec_from_file_location("_pv", _ROOT / "scripts" / "pv_validate.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_pv"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_the_set_has_sixteen_cases(pv) -> None:
    assert len(pv.CASES) == 16


def test_both_negative_controls_are_present(pv) -> None:
    images = {c.image for c in pv.CASES}
    assert any("attack-demo" in i for i in images)
    assert any("pysindy" in i for i in images)


def test_every_case_is_pinned_by_digest_not_only_by_tag(pv) -> None:
    """Tags are mutable. A rebuild would silently move :<sha>-final."""
    for case in pv.CASES:
        assert case.digest, f"{case.task} has no digest"


def test_the_negative_controls_expect_reject(pv) -> None:
    for case in pv.CASES:
        if "attack-demo" in case.image or "pysindy" in case.image:
            assert case.expected == "reject"


def test_confusion_counts_the_four_cells_plus_either(pv) -> None:
    results = [("a", "accept", "accept"), ("b", "reject", "reject"), ("c", "accept", "reject"),
               ("d", "reject", "accept"), ("e", "either", "accept")]
    counts = pv.confusion(results)
    assert counts == {"true_accept": 1, "true_reject": 1, "false_reject": 1, "false_accept": 1, "either": 1}


def test_an_either_case_never_fails_the_criterion(pv) -> None:
    """The spec allows pandas and arrow to go either way if reasoned.

    Counting an accept there as a false accept would fail the whole
    validation on a case the spec explicitly permits.
    """
    results = []
    for case in pv.CASES:
        results.append((case.task, case.expected, "accept" if case.expected == "either" else case.expected))
    ok, reasons = pv.passes_criterion(results)
    assert ok is True, reasons


def test_tiled_is_labelled_accept(pv) -> None:
    """It SUCCEEDED at 13:07:51 in 1401s after the backend fix. An earlier
    draft of this plan labelled it reject from the pre-fix trial."""
    tiled = next(c for c in pv.CASES if "tiled" in c.image)
    assert tiled.expected == "accept"


class TestPassCriterion:
    def test_a_clean_run_passes(self, pv) -> None:
        results = [(c.task, c.expected, c.expected) for c in pv.CASES]
        ok, reasons = pv.passes_criterion(results)
        assert ok is True, reasons

    def test_accepting_a_negative_control_fails(self, pv) -> None:
        results = []
        for case in pv.CASES:
            actual = "accept" if "attack-demo" in case.image else case.expected
            results.append((case.task, case.expected, actual))
        ok, reasons = pv.passes_criterion(results)
        assert ok is False
        assert any("negative control" in r for r in reasons)

    def test_a_false_accept_in_the_hard_class_fails(self, pv) -> None:
        results = []
        flipped = False
        for case in pv.CASES:
            actual = case.expected
            if case.expected == "reject" and "attack-demo" not in case.image and "pysindy" not in case.image and not flipped:
                actual, flipped = "accept", True
            results.append((case.task, case.expected, actual))
        ok, reasons = pv.passes_criterion(results)
        assert ok is False

    def test_a_false_reject_does_not_block(self, pv) -> None:
        """It costs one rebuild round. A false accept costs a bad container."""
        results = []
        flipped = False
        for case in pv.CASES:
            actual = case.expected
            if case.expected == "accept" and not flipped:
                actual, flipped = "reject", True
            results.append((case.task, case.expected, actual))
        ok, reasons = pv.passes_criterion(results)
        assert ok is True, reasons
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/agents/reflexive/test_pv_validate.py -v`
Expected: FAIL — `scripts/pv_validate.py` does not exist.

- [ ] **Step 3: Write minimal implementation**

First, collect the real digests. Run this and paste the output into `CASES`:

```bash
for t in networkx-networkx:8148 pydata-bottleneck:468 mie-lab-trackintel:596 \
         xarray-contrib-xbatcher:167 calebbell-fluids:7601ec820c6a-final \
         holoviz-datashader:28f6239bb6ae-final pydata-xarray:e7b1e5bb69a4-final \
         joblib-joblib:863994d5e39c-final dwavesystems-dimod:63d462986556-final \
         allencellmodeling-aicsimageio:c30ed9e2f881-final \
         ncar-geocat-comp:afe16c43437f-final bluesky-tiled:1283 \
         attack-demo:1 dynamicslab-pysindy:139 \
         pandas-dev-pandas:415dec58354c-final apache-arrow:80622fa077e5-final; do
  printf '%s %s\n' "$t" "$(docker inspect --format '{{.Id}}' "formulacode/$t" 2>/dev/null || echo MISSING)"
done
```

Then write:

```python
#!/usr/bin/env python3
"""Validate the verifier against a labelled set of 16 containers.

Every image already exists locally, so a run rebuilds nothing.

The measurement is AGREEMENT AGAINST THE LABELS, reported as a confusion
matrix, with every disagreement inspected by hand. "It accepted the right
number" is not the result.

    python scripts/pv_validate.py --out docs/superpowers/plans/pv-validation.md
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LabelledCase:
    task: str
    image: str
    digest: str
    expected: str
    note: str


# Pinned by digest, not by tag. Tags are mutable and a later rebuild would move
# :<sha>-final silently, which would measure the verifier against labels drawn
# from a different container than the one it inspects.
#
# Replace each PASTE_DIGEST with the output of the docker inspect loop in the
# plan's Task 10 Step 3 before running.
CASES: tuple[LabelledCase, ...] = (
    LabelledCase("networkx/networkx#8148", "formulacode/networkx-networkx:8148", "PASTE_DIGEST", "accept", "honest, 10/10"),
    LabelledCase("pydata/bottleneck#468", "formulacode/pydata-bottleneck:468", "PASTE_DIGEST", "accept", "honest, extensions 4/4"),
    LabelledCase("mie-lab/trackintel#596", "formulacode/mie-lab-trackintel:596", "PASTE_DIGEST", "accept", "honest"),
    LabelledCase("xarray-contrib/xbatcher#167", "formulacode/xarray-contrib-xbatcher:167", "PASTE_DIGEST", "accept", "honest"),
    LabelledCase("CalebBell/fluids#38", "formulacode/calebbell-fluids:7601ec820c6a-final", "PASTE_DIGEST", "accept", "554/559, 5 numba TypingErrors, soft"),
    LabelledCase("holoviz/datashader#1464", "formulacode/holoviz-datashader:28f6239bb6ae-final", "PASTE_DIGEST", "reject", "576 pass, 1 pytest-version collection error, HARD"),
    LabelledCase("pydata/xarray#11216", "formulacode/pydata-xarray:e7b1e5bb69a4-final", "PASTE_DIGEST", "reject", "0 tests"),
    LabelledCase("joblib/joblib#1682", "formulacode/joblib-joblib:863994d5e39c-final", "PASTE_DIGEST", "reject", "1522 pass, asv discovery failed"),
    LabelledCase("dwavesystems/dimod#1371", "formulacode/dwavesystems-dimod:63d462986556-final", "PASTE_DIGEST", "reject", "7 collection errors"),
    LabelledCase("AllenCellModeling/aicsimageio#486", "formulacode/allencellmodeling-aicsimageio:c30ed9e2f881-final", "PASTE_DIGEST", "reject", "pytest-version incompat, NOT a missing Java stack"),
    LabelledCase("NCAR/geocat-comp#748", "formulacode/ncar-geocat-comp:afe16c43437f-final", "PASTE_DIGEST", "reject", "2 collection errors"),
    LabelledCase("bluesky/tiled#1283", "formulacode/bluesky-tiled:1283", "PASTE_DIGEST", "accept", "SUCCEEDED 13:07:51 in 1401s after the backend fix; was a 60s BackendUnavailable failure before"),
    LabelledCase("attack-demo", "formulacode/attack-demo:1", "PASTE_DIGEST", "reject", "NEGATIVE CONTROL: adversarial sitecustomize defeated the honesty gate"),
    LabelledCase("dynamicslab/pysindy#139", "formulacode/dynamicslab-pysindy:139", "PASTE_DIGEST", "reject", "NEGATIVE CONTROL: replaced grep, fails 4 honesty checks"),
    # "either" is a real label, not a hedge. The spec says these two may go
    # either way provided the verdict is REASONED, so counting an accept here
    # as a false accept would fail the whole validation on a case the spec
    # explicitly allows.
    LabelledCase("pandas-dev/pandas", "formulacode/pandas-dev-pandas:415dec58354c-final", "PASTE_DIGEST", "either", "older corpus; collects 205357 tests"),
    LabelledCase("apache/arrow", "formulacode/apache-arrow:80622fa077e5-final", "PASTE_DIGEST", "either", "older corpus, must be reasoned"),
)

_NEGATIVE_CONTROL_MARKERS = ("attack-demo", "pysindy")


def _is_negative_control(case: LabelledCase) -> bool:
    return any(marker in case.image for marker in _NEGATIVE_CONTROL_MARKERS)


def confusion(results: list[tuple[str, str, str]]) -> dict[str, int]:
    """results are (task, expected, actual). "either" counts separately."""
    counts = {"true_accept": 0, "true_reject": 0, "false_accept": 0, "false_reject": 0, "either": 0}
    for _task, expected, actual in results:
        if expected == "either":
            counts["either"] += 1
        elif expected == actual:
            counts["true_accept" if actual == "accept" else "true_reject"] += 1
        else:
            counts["false_accept" if actual == "accept" else "false_reject"] += 1
    return counts


def passes_criterion(results: list[tuple[str, str, str]]) -> tuple[bool, list[str]]:
    """The spec's pass criterion, minus the manual and end-to-end conditions.

    A false REJECT does not block. It costs one rebuild round. A false ACCEPT
    puts a bad container in the dataset.

    Conditions 3 (every disagreement explained) and 4 (one end-to-end round)
    are human judgements and a live build, so they are not evaluated here.
    """
    by_task = {task: (expected, actual) for task, expected, actual in results}
    reasons: list[str] = []

    for case in CASES:
        pair = by_task.get(case.task)
        if pair is None:
            reasons.append(f"{case.task}: no result")
            continue
        expected, actual = pair
        if _is_negative_control(case) and actual != "reject":
            reasons.append(f"{case.task}: negative control was ACCEPTED")
        elif expected == "either":
            continue  # the spec allows either verdict here, if reasoned
        elif expected == "reject" and actual == "accept":
            reasons.append(f"{case.task}: false accept in the hard class")

    return (not reasons), reasons


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="docs/superpowers/plans/pv-validation.md")
    args = parser.parse_args()

    missing = [c.task for c in CASES if c.digest == "PASTE_DIGEST"]
    if missing:
        print(f"refusing to run: {len(missing)} case(s) still unpinned", file=sys.stderr)
        return 2

    print("Run the verifier over CASES and pass (task, expected, actual) triples to")
    print("confusion() and passes_criterion(). Wired in Task 10.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/agents/reflexive/test_pv_validate.py -v && uv run ruff check scripts/pv_validate.py`
Expected: 8 passed, ruff clean

- [ ] **Step 5: Commit**

```bash
git add scripts/pv_validate.py tests/agents/reflexive/test_pv_validate.py
git commit -m "test(reflexive): the 16-container validation set and its pass criterion

Every case is pinned by image digest rather than tag, because tags are
mutable and a rebuild would move :<sha>-final silently -- which would
measure the verifier against labels drawn from a different container than
the one it inspects.

Two negative controls carry hard assertions. attack-demo:1 has 19 lines of
adversarial sitecustomize patching Path.is_file and shutil.which, and it
DEFEATED the deterministic honesty gate. pysindy#139 fails four checks
including a replaced grep. A verifier that accepts either is worse than
the script it replaces.

A false REJECT does not block the criterion: it costs one rebuild round,
while a false ACCEPT puts a bad container in the dataset.

aicsimageio is labelled a pytest-version incompatibility, NOT a missing
Java stack. The earlier claim was asserted without reading the error."
```

---

## Task 11: Run the validation, record the result

**Files:**
- Modify: `scripts/pv_validate.py` (fill `main()` with the real run)
- Create: `docs/superpowers/plans/pv-validation.md` (generated)

**Interfaces:**
- Consumes: `verify` (Task 4), `CASES`, `confusion`, `passes_criterion` (Task 9).
- Produces: a committed results table.

- [ ] **Step 1: Pin the digests AND re-check every label**

Pinning freezes whatever is there, including a mislabel. The spec's claim that
"the pairing was checked by hand" covered 6 of 16 — fluids, datashader, xarray,
joblib, dimod, aicsimageio. Tiled, pandas, arrow, both negative controls and
the four honest tags were never checked, and re-checking tiled is what revealed
it had SUCCEEDED and was mislabelled `reject`.

So do both, for all 16:

```bash
for t in networkx-networkx:8148 pydata-bottleneck:468 mie-lab-trackintel:596 \
         xarray-contrib-xbatcher:167 calebbell-fluids:7601ec820c6a-final \
         holoviz-datashader:28f6239bb6ae-final pydata-xarray:e7b1e5bb69a4-final \
         joblib-joblib:863994d5e39c-final dwavesystems-dimod:63d462986556-final \
         allencellmodeling-aicsimageio:c30ed9e2f881-final \
         ncar-geocat-comp:afe16c43437f-final bluesky-tiled:1283 \
         attack-demo:1 dynamicslab-pysindy:139 \
         pandas-dev-pandas:415dec58354c-final apache-arrow:80622fa077e5-final; do
  printf '%-52s %s  %s\n' "$t" \
    "$(docker inspect --format '{{.Created}}' "formulacode/$t" 2>/dev/null | cut -c1-19 || echo MISSING)" \
    "$(docker inspect --format '{{.Id}}' "formulacode/$t" 2>/dev/null || echo MISSING)"
done
```

Then, for every non-control case, compare the image's creation time against the
`error_logs` row the label came from:

```bash
uv run --with psycopg2-binary python -c "
import psycopg2
c=psycopg2.connect('host=127.0.0.1 port=54322 dbname=postgres user=postgres password=postgres')
cur=c.cursor()
cur.execute(\"\"\"SELECT repo, success, round(duration_s), created_at FROM error_logs
  WHERE agent_name='default_template' ORDER BY created_at DESC LIMIT 40\"\"\")
for r in cur.fetchall(): print(r)
"
```

A case whose image predates its label row by more than that row's `duration_s`
is mislabelled: fix the label, do not pin the mismatch. Any image reporting
`MISSING` must be rebuilt or dropped with a note — do not silently shrink to 15.

- [ ] **Step 2: Wire `main()` to actually run the verifier**

```python
def _run_case(case: LabelledCase, agent) -> tuple[str, str, str]:
    from datasmith.agents.reflexive.verifier import verify

    graded = verify(case.image, "", agent, mode="container_built")
    return case.task, case.expected, ("accept" if graded.accepted else "reject")
```

Replace the body of `main()` after the `missing` guard with:

```python
    sys.path.insert(0, str(_ROOT / "src"))
    from datasmith.agents.installed.base import get_agent

    import os

    agent = get_agent([os.environ.get("DATASMITH_PV_VERIFIER_AGENT", "codex")])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    results: list[tuple[str, str, str]] = []

    with out.open("w", encoding="utf-8") as fh:
        fh.write("# Producer/verifier validation\n\n")
        fh.write("| task | expected | actual | agrees | note |\n|---|---|---|---|---|\n")
        fh.flush()
        for case in CASES:
            task, expected, actual = _run_case(case, agent)
            results.append((task, expected, actual))
            fh.write(f"| {task} | {expected} | {actual} | {'yes' if expected == actual else '**NO**'} | {case.note} |\n")
            fh.flush()
            print(f"[pv] {task}: expected={expected} actual={actual}", flush=True)

        counts = confusion(results)
        ok, reasons = passes_criterion(results)
        fh.write(f"\n## Confusion\n\n```\n{counts}\n```\n")
        fh.write(f"\n## Pass criterion (conditions 1 and 2)\n\n**{'PASS' if ok else 'FAIL'}**\n\n")
        for reason in reasons:
            fh.write(f"- {reason}\n")
        fh.write(
            "\nConditions 3 (every disagreement explained) and 4 (one end-to-end\n"
            "round on oggm#1830) are not evaluated here. Both are required before\n"
            "DATASMITH_PV_ENABLED flips to 1.\n"
        )
    print(f"[pv] wrote {out}; criterion {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1
```

- [ ] **Step 3: Run it**

Run: `SUPABASE_URL=http://127.0.0.1:54321 uv run python scripts/pv_validate.py`
Expected: 16 lines of output, a written table, and an explicit PASS/FAIL. This calls a real agent 16 times and runs Docker — budget roughly 30–60 minutes.

- [ ] **Step 4: Inspect every disagreement by hand**

For each row marked `**NO**`, read the verifier's report and write one paragraph in `pv-validation.md` under a `## Disagreements` heading saying which of these it was:
- the verifier was wrong,
- the label was wrong,
- the severity table is wrong.

Do not adjust the severity table in this task. Record the finding; changing the table is a separate, argued change.

- [ ] **Step 5: Commit**

```bash
git add scripts/pv_validate.py docs/superpowers/plans/pv-validation.md
git commit -m "test(reflexive): run the 16-container validation, record the matrix

Conditions 1 and 2 of the pass criterion are evaluated mechanically: both
negative controls rejected, zero false accepts in the hard class.
Conditions 3 and 4 are a human read and a live end-to-end round, and are
recorded as outstanding.

Every disagreement is classified by hand as the verifier being wrong, the
label being wrong, or the severity table being wrong. The table is NOT
adjusted here -- changing it is a separate, argued change, and fitting it
to this run is how a gate stops meaning anything."
```

---

## Task 12: The end-to-end round

**Files:**
- Create: `tests/agents/reflexive/test_end_to_end.py` (marked `slow`)
- Modify: `docs/superpowers/plans/pv-validation.md`

**Interfaces:**
- Consumes: everything.
- Produces: evidence for pass-criterion condition 4.

- [ ] **Step 1: Write the failing test**

```python
# tests/agents/reflexive/test_end_to_end.py
"""One full round: reject, producer edit, rebuild, accept.

The 16-container set exercises the verifier standalone against fixed images. It
never runs the producer half, the evidence channel, script application, or
termination -- yet DATASMITH_PV_ENABLED turns all of that on. This closes that
gap.

OGGM/oggm#1830 is the candidate: it fails on `ModuleNotFoundError: No module
named 'salem'`, so the fix is one added dependency and the loop should close in
two rounds.

Marked slow: it builds real containers and calls real agents. Budget an hour.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.slow


@pytest.mark.skipif(
    os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321") != "http://127.0.0.1:54321",
    reason="local Supabase only",
)
def test_oggm_recovers_in_at_most_two_rounds() -> None:
    from datasmith.agents.reflexive.loop import run_loop  # noqa: F401

    pytest.skip(
        "Run manually: SUPABASE_URL=http://127.0.0.1:54321 DATASMITH_PV_ENABLED=1 "
        "uv run fc-data --start-date 2017-01-01 --end-date 2030-12-31 --stage 6 "
        "--agent codex --force --tasks OGGM/oggm#1830 "
        "then assert the run reached PRODUCE_VERIFY and accepted. Record the "
        "outcome in docs/superpowers/plans/pv-validation.md."
    )
```

- [ ] **Step 2: Run the real thing**

```bash
SUPABASE_URL=http://127.0.0.1:54321 TMPDIR=/mnt/sdd2/tmp-prepass \
DATASMITH_PV_ENABLED=1 DATASMITH_DISABLE_DOCKER_PRUNE=1 DATASMITH_SKIP_IMAGE_PUSH=1 \
  uv run fc-data --start-date 2017-01-01 --end-date 2030-12-31 \
    --stage 6 --agent codex --force --tasks OGGM/oggm#1830
```

Expected: the log shows `PRODUCE_VERIFY`, at least one rejection naming `salem`, a producer edit to `docker_build_pkg.sh`, a rebuild, and an accept.

- [ ] **Step 3: Record the outcome**

Append to `docs/superpowers/plans/pv-validation.md` under `## Condition 4: end-to-end`:
the number of rounds, the check id the first round rejected on, the producer's edit, and the final verdict. If it did **not** accept, record the `stop_reason` and leave condition 4 failing — do not soften the criterion to match the result.

- [ ] **Step 4: Commit**

```bash
git add tests/agents/reflexive/test_end_to_end.py docs/superpowers/plans/pv-validation.md
git commit -m "test(reflexive): end-to-end round on oggm, closing pass-criterion 4

The 16-container set only exercises the verifier standalone. It never runs
the producer half, the evidence channel, script application, or
termination -- yet the flag turns all of that on. oggm#1830 fails on one
missing dependency, so the loop should close in two rounds.

The outcome is recorded whether or not it passed. A criterion softened to
match its result is not a criterion."
```

---

## Self-Review

**1. Spec coverage.**

| Spec section | Task |
|---|---|
| 3 — roles, claims/facts boundary | 1 (`PlannedEdit` file whitelist), 5 (producer cannot reach the sealer), 7 (separate agent instances) |
| 4 — severity, three causes, cross-check | 2 |
| 5 — loop, two modes, termination, channel | 4 (mode A/B), 6 (budget, no-progress), 5 (evidence requests in `ProducerPlan`) |
| 6 — feasibility | already measured; no task needed |
| 7 — components, read-only posture | 1–6, 3 (`test_the_battery_never_writes_to_the_build_context`) |
| 8 — failure handling, all five rows | 4 (agent raises/fails/unparseable, battery crash), 6 (verify raises) |
| 9 — validation, digests, pass criterion | 10, 11, 12 |
| 10 — tunables | 9 |
| 11 — open questions | not implemented by design; `DATASMITH_PV_*_AGENT` in Task 8 lets question 1 be measured |
| 12 — out of scope | Task 8's comment records the gate boundary |

Gap found and closed: the spec's `DATASMITH_PV_EVIDENCE_BUDGET` has no consumer, because serving evidence requests requires a second verifier turn. Task 5 parses `evidence_requests` and Task 9 documents the knob, but **the serving loop is deliberately not built** — the producer's requests are recorded and unanswered in v1. This is honest scope reduction, not an oversight: the channel's value is unproven until the base loop runs, and building an unexercised feature is how the previous system accumulated seven latent defects. Noted here so nobody reads the knob as live.

**2. Placeholder scan.** One intentional literal remains: `PASTE_DIGEST` in Task 9, which Task 11 Step 1 replaces with real output, and `main()` refuses to run while any survives. Every other step carries real code.

**3. Sequential execution.** These twelve tasks are strictly ordered and every
one commits, so the implementing workflow must be a **sequential chain, not a
fan-out**. Task N+1 imports what Task N defines, and concurrent agents
committing to the same branch would interleave.

**4. What the advisor review changed.** Three blockers, all confirmed against
the machine rather than argued:

- Task 8's `build()` called `verify_context` unchanged, which runs `run_tests`
  and its `rc != 0` gate — re-creating in code the exact contradiction the spec
  had just resolved in prose. Task 7 now exists to add `run_tests_gate=False`.
- The battery table was written blind and **three of seven entries were wrong**.
  `/formulacode_testrunner.py` and `/opt/formulacode/count_source_benchmarks.py`
  do not exist in any image, and bare `asv` is NOT ON PATH — the largest defect
  of this session, reproduced inside the tool meant to detect it. One `docker
  run` against networkx:8148 caught all three. Every env-only binary now goes
  through `micromamba run -n "$ENV_NAME"`, and two tests assert it.
- `bluesky/tiled#1283` was labelled `reject`. It **SUCCEEDED** at 13:07:51 in
  1401s after the backend fix, having failed at 60s before it. pandas and arrow
  were hardened from the spec's "either" to `reject`, which would have failed
  the whole validation on cases the spec explicitly permits.

**5. Type consistency.** `GradedReport` (Task 2) is the return of `verify` (Task 4) and the input of `revise` (Task 5) and `progress_key` (Task 6) — checked. `DockerContext._FILE_MAP` is used in Tasks 5 and 8 and exists at `docker/context.py:27`. `AgentResult.output` / `.success` / `.error` match `installed/base.py:21-31`. `verify_context(...) -> SandboxResult` with `.success` and `.failure_json` matches `sandbox.py:534` and `sandbox.py:94-108`. `run_loop`'s `verify` callable is `(image, log, mode) -> GradedReport` in both Task 6's tests and Task 8's lambda — checked.
