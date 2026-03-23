![banner](https://raw.githubusercontent.com/formula-code/datasmith/main/static/formula-code-datasmith.png)


<p align="center">
  <a href="https://formula-code.github.io/">
    <img src="https://img.shields.io/badge/%F0%9F%8C%90%20Website-0A7A5E?style=for-the-badge" alt="FormulaCode Website">
  </a>
  <a href="https://huggingface.co/papers/2603.16011">
    <img src="https://img.shields.io/badge/Paper-1F6FEB?style=for-the-badge&logo=arxiv&logoColor=white" alt="FormulaCode Paper">
  </a>
  <a href="https://formula-code.github.io/leaderboard/">
    <img src="https://img.shields.io/badge/%F0%9F%93%88%20Leaderboard-EA580C?style=for-the-badge&logoColor=white" alt="FormulaCode Leaderboard">
  </a>
</p>

[FormulaCode](https://formula-code.github.io/) is a *live* benchmark for evaluating the holistic ability of LLM agents to optimize codebases. FormulaCode consists of two parts: a [pipeline](https://github.com/formula-code/datasmith) to construct performance optimization tasks, and an [execution harness](https://github.com/formula-code/terminal-bench) that connects a language model to our terminal sandbox.

This dataset contains **{total_rows}** enriched performance optimization tasks derived from real open-source Python projects, spanning {num_months} months of merged PRs.

The dataset is continuously updated with new tasks every month (available under `YYYY-MM` configs, and `default` for all tasks), and a subset of tasks are human-validated and labeled as `verified`.

## Quick Start

```python
from datasets import load_dataset

# Load all tasks
ds = load_dataset("formulacode/formulacode-all")

# Load only verified tasks (human-validated)
ds = load_dataset("formulacode/formulacode-all", "verified")

# Load tasks from a specific month
ds = load_dataset("formulacode/formulacode-all", "2024-07")
```

## Configs

| Config | Description | Tasks |
|--------|-------------|-------|
| `default` | All tasks | {total_rows} |
{verified_row}
| `YYYY-MM` | Tasks by PR merge month ({num_months} months available) | varies |

## Key Columns

| Column | Description |
|--------|-------------|
| `task_id` | Unique task identifier (e.g. `pandas_dev-pandas_50643`) |
| `repo_name` | Source repository (e.g. `pandas-dev/pandas`) |
| `container_name` | Docker container reference (`<owner>-<repo>-<sha>:final`) |
| `image_name` | Full Docker Hub image reference |
| `difficulty` | Normalized difficulty: `easy`, `medium`, `hard` |
| `classification` | Optimization type (e.g. `use_better_algorithm`, `micro_optimizations`) |
| `patch` | Ground truth performance improvement patch |
| `final_md` | Task instructions in markdown |
| `pr_merged_at` | Date the PR was merged |
| `pr_merge_commit_sha` | Merge commit SHA |
| `pr_base_sha` | Base commit SHA |
| `pr_number` | GitHub PR number |
