#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [ASV_BENCHMARKS]

Options:
  ASV_BENCHMARKS           A string that should be written to asv-benchmarks.txt (required)
  -h, --help         Show this help message and exit

Description:
  This script finalizes the Docker ASV benchmark build by activating the appropriate
  environment, installing necessary packages, and adding the asv-benchmarks.txt file.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  show_help
  exit 0
fi

if [ -z "${1:-}" ]; then
  echo "Error: ASV_BENCHMARKS argument required." >&2
  show_help
  exit 1
fi

ASV_BENCHMARKS=$(realpath "$1")
cd /workspace/repo || exit 1

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
cat <<'EOF' >> ~/.bashrc
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
EOF


set +u
micromamba shell init --shell bash
echo "micromamba activate $ENV_NAME" >> ~/.bashrc
eval "$(micromamba shell hook --shell=bash)"
micromamba activate "$ENV_NAME" || true
set -u

micromamba run -n "$ENV_NAME" uv pip install git+https://github.com/formulacode/snapshot-tester.git || true
micromamba run -n "$ENV_NAME" uv pip install -q --upgrade coverage || true

# add asv-benchmarks.txt.
echo "$ASV_BENCHMARKS" > /workspace/repo/asv-benchmarks.txt
echo "Docker ASV benchmark finalization complete."

