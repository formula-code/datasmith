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
