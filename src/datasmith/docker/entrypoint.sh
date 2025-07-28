#!/usr/bin/env bash
# set -euo pipefail

# : "${COMMIT_SHA:?Need to set COMMIT_SHA}"
: "${ASV_ARGS:?Need to set ASV_ARGS}"
# : "${ASV_CONF_PATH:?Need to set ASV_CONF_PATH}"
# : "${RECOMMENDED_DEPS:?Need to set RECOMMENDED_DEPS}"

cd_asv_json_dir() {
  local match
  match=$(find . -type f -name "asv.*.json" | head -n 1)

  if [[ -n "$match" ]]; then
    local dir
    dir=$(dirname "$match")
    cd "$dir" || echo "Failed to change directory to $dir"
  else
    echo "No 'asv.*.json' file found in current directory or subdirectories."
  fi
}

# 0) Hook in micromamba and activate `base`
eval "$(micromamba shell hook --shell=bash)"
micromamba activate base
# # COMMIT_SHA=0c65bbfe8ce816a181780d2a249c94dd653e115a
# # COMMIT_SHA=ee5d94e0a05da11272a4af1cd731f9822565048e
# COMMIT_SHA=410d8268b243f0702ca605eda5a6732376a4a557
# COMMIT_SHA=3d01a24f32ab86afd55e9918cc22dea14a21bb97

# pip install pipenv
# pipenv install pyproject.toml

# 0.5) Tune the container so all CPUs stay at fixed frequency.
# This requires root; Docker runs as root by default.
# python -m pyperf system tune || true
# git checkout "${COMMIT_SHA}"

ROOT_PATH=${PWD}
# 2) cd into the folder containing the asv.conf.json
cd_asv_json_dir || exit 1

# asv run "$COMMIT_SHA^!" \
#   --show-stderr \
#   ${BENCH_REGEX:+--bench "$BENCH_REGEX"} \
#   ${INTERLEAVE_ROUNDS:+--interleave-rounds} \
#   ${APPEND_SAMPLES:+--append-samples --record-samples} \
#   -a rounds=$ROUNDS \
#   -a number=$NUMBER \
#   -a repeat=$REPEAT \
#   ${CPU_CORE:+-a cpu_affinity=[$CPU_CORE]} \
#   | tee "$OUTPUT_DIR/benchmark_${COMMIT_SHA}.log"

# the conf name is one of "asv.conf.json" or "asv.ci.conf.json" or "asv.*.json"
CONF_NAME=$(basename "$(find . -type f -name "asv.*.json" | head -n 1)")
if [[ -z "$CONF_NAME" ]]; then
    echo "No 'asv.*.json' file found in current directory or subdirectories."
    exit 1
fi

# change the "results_dir" in asv.conf.json to "/output/{COMMIT_SHA}/"
# using python
# Read the python versions from the asv.conf.json (without jq)
python_versions=$(python -c "import asv; pythons = asv.config.Config.load('$CONF_NAME').pythons; print(' '.join(pythons))")
for version in $python_versions; do
    # Create per‑Python env and install ASV
    python -c "import asv, os, pathlib
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('$CONF_NAME')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')

asv.util.write_json('$CONF_NAME', config.__dict__, api_version=config.api_version)
asv.util.write_json(path / '$CONF_NAME', config.__dict__, api_version=config.api_version)
"
    # micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9"
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    # micromamba run -n "asv_${version}" pip install asv
    # micromamba run -n "asv_${version}" pip install -e "${ROOT_PATH}"
    # if [ -n "$RECOMMENDED_DEPS" ]; then
    #     # skip command if RECOMMENDED_DEPS=""
    #     micromamba run -n "asv_${version}" pip install "${RECOMMENDED_DEPS}"
    # fi
    micromamba run -n "asv_${version}" asv machine --yes --config $CONF_NAME
    micromamba run -n "asv_${version}" asv run --show-stderr ${ASV_ARGS} --config $CONF_NAME
done

echo "Benchmarks complete."
