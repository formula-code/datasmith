#!/usr/bin/env bash
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
CONF_NAME=$(basename "$(find . -type f -name "asv.*.json" | head -n 1)")
if [[ -z "$CONF_NAME" ]]; then
    echo "No 'asv.*.json' file found in current directory or subdirectories."
    exit 1
fi
python_versions=$(python -c "import asv; pythons = asv.config.Config.load('$CONF_NAME').pythons; print(' '.join(pythons))")
for version in $python_versions; do
    python -c "import asv, os, pathlib
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('$CONF_NAME')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')

asv.util.write_json('$CONF_NAME', config.__dict__, api_version=config.api_version)
asv.util.write_json(path / '$CONF_NAME', config.__dict__, api_version=config.api_version)
"
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9"
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    micromamba run -n "asv_${version}" asv machine --yes --config $CONF_NAME
    micromamba run -n "asv_${version}" pip install -e .
done
