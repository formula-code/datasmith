"""Public API harbor imports, and credentials never rendered as literals."""

import subprocess
import sys

import pytest

from datasmith.harbor_adapter.utils import render_task_toml


def test_public_api_imports_without_harbor():
    code = (
        "import sys, datasmith.harbor_adapter as h;"
        "assert {'FormulaCodeAdapter','FormulaCodeRecord','to_record','make_task_id','normalize_difficulty'} <= set(h.__all__);"
        "assert all(hasattr(h, n) for n in h.__all__);"
        "assert 'harbor' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize("value", ["${SUPABASE_ANON_KEY}", "${SUPABASE_URL:-}"])
def test_credential_reference_renders(value):
    assert value in render_task_toml(verifier_env={"SUPABASE_URL": value})


@pytest.mark.parametrize(
    "key,value",
    [
        ("SUPABASE_ANON_KEY", "eyJhbGciOi"),
        ("SUPABASE_URL", "https://x.supabase.co"),
        ("HF_TOKEN", "${HF_TOKEN:-hf_literal}"),
    ],
)
def test_credential_literal_rejected(key, value):
    with pytest.raises(ValueError):
        render_task_toml(verifier_env={key: value})


def test_non_credential_literal_allowed():
    assert 'FORMULACODE_EXPECTED_N = "3"' in render_task_toml(verifier_env={"FORMULACODE_EXPECTED_N": "3"})
