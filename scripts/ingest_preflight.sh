#!/usr/bin/env bash
# Preflight for an fc-data stage 2-5 run.  Every check below corresponds to a
# failure that actually cost time on the 2026-08 July/August run.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; }

echo "== entrypoint =="
# A stale conda install of a DIFFERENT checkout used to shadow this one.
if [ -x .venv/bin/fc-data ]; then
  target=$(.venv/bin/python -c 'import datasmith;print(datasmith.__file__)' 2>/dev/null)
  case "$target" in "$PWD"/src/datasmith/*) ok "fc-data -> $target";;
    *) bad "venv resolves datasmith to $target (expected this checkout)";; esac
  pv=$(.venv/bin/python -c 'import sys;print(".".join(map(str,sys.version_info[:2])))')
  [ "$pv" = "3.12" ] || [ "$pv" = "3.11" ] && ok "python $pv" || warn "python $pv (CI runs 3.11/3.12)"
else bad ".venv/bin/fc-data missing — run: uv sync"; fi

echo "== database =="
code=$(curl -s -m 8 -o /dev/null -w '%{http_code}' "${SUPABASE_URL:-http://127.0.0.1:54321}/rest/v1/" 2>/dev/null)
[ "$code" = "200" ] && ok "supabase reachable (${SUPABASE_URL:-http://127.0.0.1:54321})" \
                    || bad "supabase not reachable (http=$code) — start it before running"

.venv/bin/python scripts/ingest_preflight_checks.py || fail=1

echo
[ "$fail" = "0" ] && echo "preflight PASSED" || { echo "preflight FAILED — fix the above before running"; exit 1; }
