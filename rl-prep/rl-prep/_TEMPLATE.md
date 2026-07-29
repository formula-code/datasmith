<!--
Standard per-container format for rl-prep/containers/<owner>__<repo>__<issue>.md
Copy this file, fill every section, keep it SHORT. Delete these HTML comments.
Status values: rl-ready | needs-work | dropped | candidate
-->
# <owner>/<repo>#<issue> — <one-line what the PR optimizes>

**Status:** `<rl-ready|needs-work|dropped|candidate>` &nbsp;|&nbsp; **Oracle H:** `<value>` &nbsp;|&nbsp; **Image:** `formulacode/<repo>:<tag>`

## Identity
- **Task ID / dir:** `<owner>__<repo>__<issue>` (task_id = `<issue>`)
- **base_commit:** `<short sha>` &nbsp; **merge (gt_hash):** `<short sha>`
- **PR:** "<title>" — merged `<YYYY-MM-DD>` — <url>
- **Image tag:** `formulacode/<repo>:<tag>` &nbsp; <!-- flag any known-wrong tag, e.g. pvlib :latest has stale H -->

## Benchmark
- **File:** `<benchmark_*.py>` → repo path `<benchmark_dest>`
- **Targets:** `<module.Class.method>` — <what hot path / how many benchmarks>
- **Provenance:** `<synthesized-for-task | extracted-from-PR-merge-commit>`
  <!-- networkx: in the PR diff; pvlib/h11/etc: hand-authored against the known oracle -->

## Per-task overrides (`formulacode_task_overrides` — only non-defaults)
- `benchmark_dest` = `<path>`
- `pip_pins` = `<[...] or —>`
- `restore_regex` = `<pattern or default>`
- `extra_dockerfile_commands` / `extra_entrypoint_commands` = `<summary or —>`
- **Benchmark body** in Storage bucket `task-overrides/<owner>__<repo>__<issue>/<file>`

## Known issues
| Issue | Status | What's required to fully fix |
|---|---|---|
| <issue> | `fixed` / `partial` / `open` | <the concrete remaining step, or "n/a"> |

## RL signal
- **Oracle H:** `<value>` (`<n>` valid benchmarks) — confirmed `<date/source>`
- **pass@k:** <findings, e.g. pass@16: 7/13 broke import, none found the real fix>
- **GRPO diversity:** <e.g. saturated / near-1.0 / good 30–70% band>

## Notes / provenance
- Source memory: `<project_*_fix.md, project_container_changes.md, ...>`
- <anything non-obvious for whoever picks this up>
