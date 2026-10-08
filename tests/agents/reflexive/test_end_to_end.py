"""One full round: reject, producer edit, rebuild, accept.

The 16-container set exercises the verifier standalone against fixed images. It
never runs the producer half, the evidence channel, script application, or
termination -- yet `DATASMITH_PV_ENABLED` turns all of that on. This file
closes that gap, and it is pass-criterion condition 4 in
`docs/superpowers/specs/2026-08-23-producer-verifier-design.md` section 9.

`OGGM/oggm#1830` is the candidate: it fails on `ModuleNotFoundError: No module
named 'salem'`, so the fix is one added dependency and the loop should close in
two rounds.

The body is a skip, not an assertion, and deliberately so: the real run builds
containers and calls real agents for the better part of an hour, which is not
something `make test` can do. What the module DOES check on every run is that
the symbols the manual command depends on still exist and still import. That
is a small guarantee, but it is a real one -- an import that has been renamed
out from under this file would otherwise be discovered only by the person who
next spends an hour on the manual run.

The outcome is recorded in `docs/superpowers/plans/pv-validation.md` under
`## Condition 4: end-to-end`, whether or not it passed. A criterion softened
to match its result is not a criterion.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.slow

_MANUAL_COMMAND = """
SUPABASE_URL=http://127.0.0.1:54321 TMPDIR=/mnt/sdd2/tmp-prepass \\
DATASMITH_PV_ENABLED=1 DATASMITH_DISABLE_DOCKER_PRUNE=1 DATASMITH_SKIP_IMAGE_PUSH=1 \\
DATASMITH_SKIP_SIMILAR_CONTEXTS=1 \\
  uv run fc-data --start-date 2017-01-01 --end-date 2030-12-31 \\
    --stage 6 --agent codex --force --tasks OGGM/oggm#1830
"""


def test_the_symbols_the_manual_run_needs_still_exist() -> None:
    """Runs everywhere, including in `make test`. Costs milliseconds.

    `run_loop` is what the manual command exercises; `verify` is where the
    host image scan was wired in, and `collect_and_evaluate` is the default it
    must reach. A rename in any of the three breaks the manual run, and this
    is the cheapest place to find that out.
    """
    from datasmith.agents.reflexive.image_integrity import collect_and_evaluate
    from datasmith.agents.reflexive.loop import DATASMITH_PV_ENABLED, run_loop
    from datasmith.agents.reflexive.verifier import verify

    assert callable(run_loop)
    assert callable(collect_and_evaluate)
    import inspect

    assert "integrity_collector" in inspect.signature(verify).parameters
    assert isinstance(DATASMITH_PV_ENABLED, bool)


@pytest.mark.skipif(
    os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321") != "http://127.0.0.1:54321",
    reason="local Supabase only",
)
def test_oggm_recovers_in_at_most_two_rounds() -> None:
    pytest.skip(
        "Run manually; it builds real containers and calls real agents.\n"
        f"{_MANUAL_COMMAND}\n"
        "Then assert the run reached PRODUCE_VERIFY and accepted, and record "
        "the rounds, the check id the first round rejected on, the producer's "
        "edit and the final verdict in docs/superpowers/plans/pv-validation.md. "
        "DATASMITH_SKIP_SIMILAR_CONTEXTS=1 is not optional: TRY_SIMILAR reuses "
        "agent-authored contexts, 128 of which install a sitecustomize shim "
        "into site-packages, and the host image scan rejects any container "
        "built from one."
    )
