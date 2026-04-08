set -euo pipefail

TASK_LIST="dataset/formulacode_verified/pandas-dev_pandas/cxxabi_1_3_15_tasks.txt"

if [[ ! -f "$TASK_LIST" ]]; then
  echo "Task list not found: $TASK_LIST" >&2
  exit 1
fi

while IFS= read -r sha; do
  [[ -z "$sha" ]] && continue
  echo "==> Verifying pandas-dev/pandas task: $sha"
  uv run python dataset/verify.py --task "dataset/formulacode_verified/pandas-dev_pandas/$sha/"
done < "$TASK_LIST"
