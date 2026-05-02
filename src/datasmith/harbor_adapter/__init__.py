"""Vendored copy of eval_frameworks/harbor/adapters/formulacode.

Only the pieces datasmith needs to generate a Harbor task directory from
its own Supabase rows are included: the adapter, its template renderers,
and the template/ payload. Dataset/HF/DockerHub helpers are intentionally
omitted — datasmith materializes tasks directly from its pull_requests +
candidate_containers tables.
"""

from datasmith.harbor_adapter.adapter import (
    FormulaCodeAdapter,
    FormulaCodeRecord,
    OracleArtifacts,
    fetch_oracle_artifacts,
)
from datasmith.harbor_adapter.records import to_record

__all__ = [
    "FormulaCodeAdapter",
    "FormulaCodeRecord",
    "OracleArtifacts",
    "fetch_oracle_artifacts",
    "to_record",
]
