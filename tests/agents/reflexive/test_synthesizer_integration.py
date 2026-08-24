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


def test_produce_verify_runs_the_tamper_audit_on_the_producer_context() -> None:
    """Otherwise producer-side tampering is checked by nobody.

    The legacy path calls classify_context after TRY_DEFAULT and after every
    LLM_GENERATE attempt. The battery collects functional facts only, so
    without this `tamper_audit` is a check id nothing can ever emit -- and
    attack-demo:1 and pysindy#139, both labelled reject in the validation set,
    would have nothing to be rejected on. Neither breaks function; both defeat
    detection.
    """
    import inspect

    from datasmith.agents.synthesizer import Synthesizer

    source = inspect.getsource(Synthesizer._run_produce_verify)
    assert "classify_context" in source, "PRODUCE_VERIFY must run the tamper audit"
    assert "tamper.tampered" in source, "and must act on its result"
