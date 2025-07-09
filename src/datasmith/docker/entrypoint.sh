#!/usr/bin/env bash
# set -euo pipefail

: "${COMMIT_SHA:?Need to set COMMIT_SHA}"
: "${ASV_ARGS:?Need to set ASV_ARGS}"
: "${ASV_CONF_PATH:?Need to set ASV_CONF_PATH}"

# 0) Hook in micromamba and activate `base`
eval "$(micromamba shell hook --shell=bash)"
micromamba activate base

# 0.5) Tune the container so all CPUs stay at fixed frequency.
# This requires root; Docker runs as root by default.
# python -m pyperf system tune || true
git checkout --quiet "${COMMIT_SHA}"

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
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/' '\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('asv.conf.json')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')

asv.util.write_json('asv.conf.json', config.__dict__, api_version=1)
asv.util.write_json(path / 'asv.conf.json', config.__dict__, api_version=1)
"

    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" \
       git \
       pyperf \
       libmambapy \
       mamba \
       conda
    micromamba run -n "asv_${version}" pip install asv
    micromamba run asv machine --yes
    micromamba run asv run --show-stderr "$COMMIT_SHA^!" ${ASV_ARGS}
done

echo "Benchmarks complete."
