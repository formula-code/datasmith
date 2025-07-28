#!/usr/bin/env bash
# set -euo pipefail

: "${COMMIT_SHA:?Need to set COMMIT_SHA}"
: "${ASV_ARGS:?Need to set ASV_ARGS}"
: "${ASV_CONF_PATH:?Need to set ASV_CONF_PATH}"
: "${RECOMMENDED_DEPS:?Need to set RECOMMENDED_DEPS}"

# 0) Hook in micromamba and activate `base`
eval "$(micromamba shell hook --shell=bash)"
micromamba activate base
# # COMMIT_SHA=0c65bbfe8ce816a181780d2a249c94dd653e115a
# # COMMIT_SHA=ee5d94e0a05da11272a4af1cd731f9822565048e

# pip install pipenv
# pipenv install pyproject.toml

# 0.5) Tune the container so all CPUs stay at fixed frequency.
# This requires root; Docker runs as root by default.
# python -m pyperf system tune || true
git checkout --quiet "${COMMIT_SHA}"

ROOT_PATH=${PWD}
# 2) cd into the folder containing the asv.conf.json
cd "$(dirname "$ASV_CONF_PATH")"

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

# change the "results_dir" in asv.conf.json to "/output/{COMMIT_SHA}/"
# using python
# Read the python versions from the asv.conf.json (without jq)
python_versions=$(python -c "import asv; pythons = asv.config.Config.load('asv.conf.json').pythons; print(' '.join(pythons))")
for version in $python_versions; do
    # Create per‑Python env and install ASV
    python -c "import asv, os, pathlib
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('asv.conf.json')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')

asv.util.write_json('asv.conf.json', config.__dict__, api_version=1)
asv.util.write_json(path / 'asv.conf.json', config.__dict__, api_version=1)
"

    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9"
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    # micromamba run -n "asv_${version}" pip install asv
    # micromamba run -n "asv_${version}" pip install -e "${ROOT_PATH}"
    # if [ -n "$RECOMMENDED_DEPS" ]; then
    #     # skip command if RECOMMENDED_DEPS=""
    #     micromamba run -n "asv_${version}" pip install "${RECOMMENDED_DEPS}"
    # fi
    micromamba run -n "asv_${version}" asv machine --yes
    micromamba run -n "asv_${version}" asv run --show-stder ${ASV_ARGS}
done

echo "Benchmarks complete."
