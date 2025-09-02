#!/usr/bin/env bash
# set -euo pipefail
set -x
: "${ASV_ARGS:?Need to set ASV_ARGS}"
: "${ASV_MACHINE:=?Need to set ASV_MACHINE}"
: "${ASV_OS:=?Need to set ASV_OS}"
: "${ASV_NUM_CPU:=?Need to set ASV_NUM_CPU}"
: "${ASV_ARCH:=?Need to set ASV_ARCH}"
: "${ASV_CPU:=?Need to set ASV_CPU}"
: "${ASV_RAM:=?Need to set ASV_RAM}"


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

eval "$(micromamba shell hook --shell=bash)"

micromamba activate base
ROOT_PATH=${PWD}
cd_asv_json_dir || exit 1

# the conf name is one of "asv.conf.json" or "asv.ci.conf.json" or "asv.*.json"
CONF_NAME=$(basename "$(find . -type f -name "asv.*.json" | head -n 1)")
if [[ -z "$CONF_NAME" ]]; then
    echo "No 'asv.*.json' file found in current directory or subdirectories."
    exit 1
fi

# Read the python versions from the asv.conf.json
python_versions=$(python -c "import asv; pythons = asv.config.Config.load('$CONF_NAME').pythons; print(' '.join(pythons))")
# change the "results_dir" in asv.conf.json to "/output/{COMMIT_SHA}/"
for version in $python_versions; do
    # Create per‑Python env and install ASV
    python -c "import asv, os, pathlib
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('$CONF_NAME')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')
config.branches = ['HEAD']

asv.util.write_json('$CONF_NAME', config.__dict__, api_version=config.api_version)
asv.util.write_json(path / '$CONF_NAME', config.__dict__, api_version=config.api_version)
"
    micromamba run -n "asv_${version}" asv machine --yes --config $CONF_NAME --machine "$ASV_MACHINE" --os "$ASV_OS" --num_cpu "$ASV_NUM_CPU" --arch "$ASV_ARCH" --cpu "$ASV_CPU" --ram "$ASV_RAM"
    micromamba run -n "asv_${version}" asv run --show-stderr ${ASV_ARGS} --config $CONF_NAME
done

echo "Benchmarks complete."
