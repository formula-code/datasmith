"""Deeper audit of the 89 remaining candidate_containers.

We already removed the obvious bypass patterns (forged /logs/*.json,
root-level test_*.py, inline trivial assert). This script looks for *other*
bypass shapes that might still be hiding:

  - Empty/no-op build scripts
  - Scripts that don't reference any install command (pip/uv/setup.py)
  - References to /logs/ at all (in case our regex missed an obfuscation)
  - Suspicious env-var redirections (T_BENCH_AGENT_MODEL_NAME rewrites)
  - Disabling pytest_runner or skipping it
  - Writing to /workspace/repo/conftest.py to swallow collection errors
  - Writing to any */test_*.py / */*_test.py path under the repo (subdir injection)
  - Writing fake patches or pre-baked benchmark results
  - Calls to `pytest` with `-k` filters that look like single trivial tests
  - Mocking out the parser/runner via PYTHONPATH or sys.path injection
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Callable

from dotenv import load_dotenv

from datasmith.utils.db import fetch_all

load_dotenv("tokens.env")

SCRIPT_COLUMNS = (
    "dockerfile",
    "build_base_sh",
    "build_env_sh",
    "build_pkg_sh",
    "build_run_sh",
    "build_final_sh",
    "run_tests_sh",
    "entrypoint_sh",
    "profile_sh",
)


def script_blob(row: dict) -> str:
    return "\n".join((row.get(k) or "") for k in SCRIPT_COLUMNS)


# Each check returns (tag, snippet) or None
Check = Callable[[dict, str], tuple[str, str] | None]


def chk_logs_reference(row, blob):
    m = re.search(r"/logs/[\w./*-]+", blob)
    if m:
        return ("references_/logs", m.group(0))
    return None


def chk_test_file_anywhere(row, blob):
    # Heredoc / write to ANY test_*.py or *_test.py path (not just root)
    m = re.search(
        r"""(?xm)
        (?:cat|tee|printf|echo)
        [^\n]*?>\s*["']?
        ([\w./$@{}\\-]*?(?:test_[\w.-]+|_test)\.py)
        """,
        blob,
    )
    if m:
        return ("writes_test_file", m.group(1))
    # Python json.dump / open(..., 'w') to a test_*.py
    m = re.search(
        r"""(?x)
        open\(\s*["']([^"']*(?:test_[\w.-]+|_test)\.py)["']\s*,\s*["']w
        """,
        blob,
    )
    if m:
        return ("python_writes_test_file", m.group(1))
    return None


def chk_conftest_write(row, blob):
    if re.search(r"""(?:cat|echo|printf|tee)[^\n]*>\s*["']?[\w./$@{}\\-]*conftest\.py""", blob):
        return ("writes_conftest", "conftest.py heredoc")
    if re.search(r"""open\(\s*["'][^"']*conftest\.py["']\s*,\s*["']w""", blob):
        return ("python_writes_conftest", "conftest.py via Python")
    return None


def chk_t_bench_agent_override(row, blob):
    m = re.search(r"T_BENCH_AGENT_MODEL_NAME\s*=\s*[^\n]+", blob)
    if m and "agent" not in m.group(0).split("=", 1)[1].lower()[:50]:
        return ("t_bench_override", m.group(0)[:80])
    return None


def chk_no_install(row, blob):
    # Build_pkg_sh should reference a real install verb. The template defaults
    # include the literal text 'pip install', but a no-op edit might strip it.
    pkg = row.get("build_pkg_sh") or ""
    if not pkg.strip():
        return ("build_pkg_sh_empty", "")
    install_verbs = (
        "pip install",
        "uv pip install",
        "pip3 install",
        "python setup.py install",
        "conda install",
        "micromamba install",
    )
    if not any(v in pkg for v in install_verbs):
        return ("build_pkg_sh_no_install", "no install verb")
    return None


def chk_pytest_skip(row, blob):
    # If build scripts mention pytest_runner or formulacode_testrunner
    # being deleted/replaced/short-circuited
    if re.search(r"rm\s+[^\n]*formulacode_testrunner", blob):
        return ("removes_testrunner", "rm formulacode_testrunner")
    if re.search(r"rm\s+[^\n]*pytest_runner", blob):
        return ("removes_pytest_runner", "rm pytest_runner")
    if re.search(r"(?:cat|tee)[^\n]*>\s*[^\n]*formulacode_testrunner\.py", blob):
        return ("rewrites_testrunner", "rewrites testrunner")
    return None


def chk_run_tests_sh_modified(row, blob):
    rts = row.get("run_tests_sh") or ""
    if rts.strip():
        return ("run_tests_sh_set", f"{len(rts)} chars")
    return None


def chk_dockerfile_modified(row, blob):
    df = row.get("dockerfile") or ""
    if df.strip():
        return ("dockerfile_set", f"{len(df)} chars")
    return None


def chk_pythonpath_redirect(row, blob):
    if re.search(r"PYTHONPATH=[^\n]*(?:fake|stub|mock|bypass)", blob, re.IGNORECASE):
        return ("pythonpath_stub", "")
    return None


def chk_pre_baked_results(row, blob):
    if re.search(r"results/.*\.json\s*<<", blob):
        return ("prebaked_asv_results", "")
    if re.search(r"benchmarks\.json[^\n]*<<", blob):
        return ("prebaked_benchmarks_json", "")
    return None


def chk_git_commits_passing_marker(row, blob):
    if re.search(r"git\s+commit[^\n]*formulacode", blob, re.IGNORECASE):
        return ("git_commit_formulacode", "")
    return None


def chk_summary_keyword(row, blob):
    # Even any mention of "summary" in test/benchmark context might be telling
    if re.search(r'"passed"\s*:\s*[1-9]', blob):
        return ("hardcoded_passed_count", "")
    return None


CHECKS: list[Check] = [
    chk_logs_reference,
    chk_test_file_anywhere,
    chk_conftest_write,
    chk_t_bench_agent_override,
    chk_no_install,
    chk_pytest_skip,
    chk_run_tests_sh_modified,
    chk_dockerfile_modified,
    chk_pythonpath_redirect,
    chk_pre_baked_results,
    chk_git_commits_passing_marker,
    chk_summary_keyword,
]


def main() -> None:
    select = ",".join(("owner", "repo", "sha", "issue_number", *SCRIPT_COLUMNS))
    rows = fetch_all("candidate_containers", select=select)
    print(f"Inspecting {len(rows)} remaining candidate_containers\n")

    findings: dict[tuple[str, str, str], list[tuple[str, str]]] = defaultdict(list)
    tag_counts: Counter[str] = Counter()

    for r in rows:
        blob = script_blob(r)
        key = (r["owner"], r["repo"], r["sha"])
        for chk in CHECKS:
            res = chk(r, blob)
            if res:
                findings[key].append(res)
                tag_counts[res[0]] += 1

    print("Tag counts:")
    for tag, count in tag_counts.most_common():
        print(f"  {tag:>32}: {count}")

    print(f"\nContainers with at least one finding: {len(findings)} / {len(rows)}")
    clean = [r for r in rows if (r["owner"], r["repo"], r["sha"]) not in findings]
    print(f"Containers with NO findings:           {len(clean)}")

    print("\n--- All flagged containers ---")
    for r in rows:
        key = (r["owner"], r["repo"], r["sha"])
        if key not in findings:
            continue
        tags = findings[key]
        print(f"\n{r['owner']}/{r['repo']}@{r['sha'][:10]} (PR #{r.get('issue_number')})")
        for tag, snippet in tags:
            print(f"  [{tag}] {snippet[:120]}")

    print("\n--- Sample of CLEAN containers (first 10) ---")
    for r in clean[:10]:
        print(f"  {r['owner']}/{r['repo']}@{r['sha'][:10]} (PR #{r.get('issue_number')})")


if __name__ == "__main__":
    main()
