from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2, rmtree

from datasmith.harbor_adapter.utils import (
    DATASMITH_LSV_ROUNDS,
    make_task_id,
    render_dockerfile,
    render_instruction_md,
    render_task_toml,
    render_template,
)


@dataclass
class FormulaCodeRecord:
    container_name: str  # repo_name-{base_sha}:final
    patch: str  # diff(base_sha, merge_sha)
    owner: str
    repo: str
    issue_number: int  # PR number
    gt_hash: str  # merge_sha
    base_commit: str  # base_sha
    instructions: str
    date: str | None = None  # merge_date
    classification: str | None = None
    difficulty: str = "hard"
    repo_name: str | None = None

    @property
    def task_id(self) -> str:
        """Harbor task id; the DB is keyed by the (owner, repo, issue_number) triple, not this."""
        return make_task_id(self.owner, self.repo, self.issue_number)

    @property
    def task_dir_name(self) -> str:
        """Flat, globally unique task dir name (Harbor discovers tasks one level deep)."""
        return self.task_id


# Harbor uploads tests/ to /tests before setup.sh runs, so the helpers are read from there.
TEST_HELPERS = (
    "lsv_init.py",
    "lsv_measure.py",
    "parser.py",
    "upload.py",
    "pytest_runner.py",
    "jinja_patch_plugin_pandas.py",
)


def _write(path: Path, text: str, executable: bool = False) -> None:
    path.write_text(text)
    if executable:
        path.chmod(0o755)


class FormulaCodeAdapter:
    def __init__(self, harbor_tasks_root: Path, force: bool = False) -> None:
        self.out_root = Path(harbor_tasks_root)
        self.out_root.mkdir(parents=True, exist_ok=True)
        self.template_dir = Path(__file__).parent / "template"
        if force:
            for sub in self.out_root.iterdir():
                if sub.is_dir():
                    rmtree(sub)

    def generate_task(
        self,
        rec: FormulaCodeRecord,
        run_pytest: bool = True,
        timeout_sec: float = 21600.0,
        cpus: int = 2,
        memory: str = "4G",
        storage: str = "10G",
        rounds: int | None = None,
        verifier_env: dict[str, str] | None = None,
        expected_n: int | None = None,
    ) -> Path:
        """Render the Harbor task dir for ``rec``.

        ``rounds`` (default ``DATASMITH_LSV_ROUNDS``) drives both the baked baseline and the trial passes.
        ``expected_n`` (from ``formulacode_task_overrides``) becomes ``FORMULACODE_EXPECTED_N`` for the
        dilution_ratio invariant; ``None`` emits no key, so the invariant skips.
        """
        rounds = DATASMITH_LSV_ROUNDS if rounds is None else rounds
        out_dir = self.out_root / rec.task_dir_name
        env, tests, solution = (out_dir / d for d in ("environment", "tests", "solution"))
        for d in (env, tests, solution):
            d.mkdir(parents=True, exist_ok=True)

        copy2(self.template_dir / "environment" / "entrypoint.sh", env / "entrypoint.sh")
        for name in TEST_HELPERS:
            copy2(self.template_dir / "tests" / name, tests / name)
        # The Dockerfile's baseline bake runs lsv_init.py at build time, before /tests exists.
        copy2(self.template_dir / "tests" / "lsv_init.py", env / "lsv_init.py")

        _write(out_dir / "instruction.md", render_instruction_md(rec.instructions))
        if expected_n is not None:
            verifier_env = {**(verifier_env or {}), "FORMULACODE_EXPECTED_N": str(expected_n)}
        task_toml = render_task_toml(
            difficulty=rec.difficulty,
            category=rec.classification or "optimization",
            timeout_sec=timeout_sec,
            cpus=cpus,
            memory=memory,
            storage=storage,
            verifier_env=verifier_env,
        )
        _write(out_dir / "task.toml", task_toml)

        config_json = json.dumps({**rec.__dict__, "task_id": rec.task_id}, indent=2)
        base_image = rec.container_name or "python:3.11-slim"
        _write(env / "Dockerfile", render_dockerfile(base_image, numba_threads=cpus, lsv_rounds=rounds))
        _write(env / "config.json", config_json)

        ids = {"task_id": rec.task_id, "owner": rec.owner, "repo": rec.repo, "issue_number": rec.issue_number}
        test_sh = render_template(
            "tests/test.sh", base_commit=rec.base_commit, run_pytest=run_pytest, rounds=rounds, **ids
        )
        _write(tests / "test.sh", test_sh, executable=True)
        _write(tests / "config.json", config_json)
        _write(solution / "solve.sh", render_template("solution/solve.sh", solution_patch=rec.patch), executable=True)
        setup_sh = render_template("tests/setup.sh", rounds=rounds, extra_setup_commands="", **ids)
        _write(tests / "setup.sh", setup_sh, executable=True)

        # Imported here so `python -m datasmith.harbor_adapter.stamp` does not re-import itself via the package.
        from datasmith.harbor_adapter.stamp import write_stamp

        write_stamp(
            out_dir,
            rounds=rounds,
            cpus=cpus,
            memory=memory,
            storage=storage,
            timeout_sec=timeout_sec,
            run_pytest=run_pytest,
            base_image=rec.container_name,
        )
        return out_dir
