# Stages the working tree into the index. Run output stays out; a nested repository (astropy-benchmarks/) goes in as plain files.
printf '%s\n' asv_benchmarks.txt solution_patch.diff '*.orig' '*.rej' __pycache__/ '*.py[co]' .asv/ \
  '/tmp.*' '/*.npz' /test_array.csv '*.nc' '*.nc4' .hypothesis/ .pytest_cache/ >> .git/info/exclude
python -c 'import json, os; c = json.load(open("/workspace/asv.conf.json")); print("\n".join("/" + os.path.relpath(c[k], "/workspace/repo") + "/" for k in ("results_dir", "env_dir", "html_dir") if (c.get(k) or "").startswith("/workspace/repo/")))' >> .git/info/exclude 2>/dev/null || true
git ls-files --others --exclude-standard | { grep '/$' || true; } | while read -r d; do
  env -u GIT_INDEX_FILE git -C "$d" ls-files -co --exclude-standard | sed "s|^|$d|"
done | git update-index --add --stdin
git add -A
