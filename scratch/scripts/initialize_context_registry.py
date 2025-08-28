from __future__ import annotations

from pathlib import Path

from datasmith.docker.context import ContextRegistry, DockerContext
from datasmith.logging_config import get_logger

logger = get_logger("docker.context_registry")

CONTEXT_REGISTRY = ContextRegistry(default_context=DockerContext())

CONTEXT_REGISTRY.register(
    "asv/astropy/astropy",
    DockerContext(
        building_data="""#!/usr/bin/env bash

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
git clone -b main https://github.com/astropy/astropy-benchmarks.git --single-branch
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
    #### BUILD STEPS GO HERE. ####
    export CFLAGS="$CFLAGS -Wno-error=incompatible-pointer-types"
    micromamba run -n "asv_${version}" pip install -e . scipy matplotlib
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)

CONTEXT_REGISTRY.register(
    "asv/scikit-learn/scikit-learn",
    DockerContext(
        building_data="""#!/usr/bin/env bash

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

apt-get update && \
    apt-get install -y \
    ninja-build \
    cmake

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
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    micromamba run -n "asv_${version}" pip install meson-python cython
    #### BUILD STEPS GO HERE. ####
    micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable ${ROOT_PATH}
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)

CONTEXT_REGISTRY.register(
    "asv/scikit-learn/scikit-learn/8bc36080d9855d29e1fcbc86da46a9e89e86c046",
    DockerContext(
        building_data="""#!/usr/bin/env bash
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
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    #### BUILD STEPS GO HERE. ####
    micromamba run -n "asv_${version}" pip install -U meson-python "cython<3" "numpy<2" "setuptools==60" "scipy<1.14"
    export CFLAGS="$CFLAGS -Wno-error=incompatible-pointer-types"
    micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable ${ROOT_PATH}
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)


CONTEXT_REGISTRY.register(
    "asv/nvidia/warp",
    DockerContext(
        building_data="""
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

# only run the below if condition if bvh.cpp is present
grep -q '^#include <climits>' "${ROOT_PATH}/warp/native/bvh.cpp" || \
  sed -i 's|#include <map>|#include <map>\n#include <climits>|' "${ROOT_PATH}/warp/native/bvh.cpp"

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
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    micromamba run -n "asv_${version}" pip install meson-python cython
    #### BUILD STEPS GO HERE. ####
    micromamba run -n "asv_${version}" python "${ROOT_PATH}/build_lib.py"
    micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable ${ROOT_PATH}
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)


CONTEXT_REGISTRY.register(
    "asv/python-control/python-control",
    DockerContext(
        building_data="""
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
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    micromamba run -n "asv_${version}" pip install meson-python cython
    #### BUILD STEPS GO HERE. ####
    # if make_version exists run it
    if [[ -f "${ROOT_PATH}/make_version.py" ]]; then
        micromamba run -n "asv_${version}" python "${ROOT_PATH}/make_version.py"
    fi
    micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable ${ROOT_PATH}
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)


CONTEXT_REGISTRY.register(
    "asv/mdanalysis/mdanalysis",
    DockerContext(
        building_data="""
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
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy "cython<3" joblib threadpoolctl pytest compilers meson-python
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    #### BUILD STEPS GO HERE. ####
    # if maintainer/install_all.sh exists run it with develop
    if [[ -f "maintainer/install_all.sh" ]]; then
      micromamba activate "asv_${version}"
      working_dir=$(pwd)
      cd "$ROOT_PATH" || exit 1
      bash maintainer/install_all.sh develop
      cd "$working_dir" || exit 1
    else
      micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable .
    fi
    #### BUILD STEPS END HERE. ####
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
    ),
)

# CONTEXT_REGISTRY.register(
#     "asv/default/nobuild",
#     DockerContext(
#         building_data="""#!/usr/bin/env bash
# cd_asv_json_dir() {
#   local match
#   match=$(find . -type f -name "asv.*.json" | head -n 1)

#   if [[ -n "$match" ]]; then
#     local dir
#     dir=$(dirname "$match")
#     cd "$dir" || echo "Failed to change directory to $dir"
#   else
#     echo "No 'asv.*.json' file found in current directory or subdirectories."
#   fi
# }
# eval "$(micromamba shell hook --shell=bash)"
# micromamba activate base

# ROOT_PATH=${PWD}
# cd_asv_json_dir || exit 1
# CONF_NAME=$(basename "$(find . -type f -name "asv.*.json" | head -n 1)")
# if [[ -z "$CONF_NAME" ]]; then
#     echo "No 'asv.*.json' file found in current directory or subdirectories."
#     exit 1
# fi
# python_versions=$(python -c "import asv; pythons = asv.config.Config.load('$CONF_NAME').pythons; print(' '.join(pythons))")
# for version in $python_versions; do
#     python -c "import asv, os, pathlib
# path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
# path.mkdir(parents=True, exist_ok=True)

# config = asv.config.Config.load('$CONF_NAME')
# config.results_dir = str(path / 'results')
# config.html_dir = str(path / 'html')

# asv.util.write_json('$CONF_NAME', config.__dict__, api_version=config.api_version)
# asv.util.write_json(path / '$CONF_NAME', config.__dict__, api_version=config.api_version)
# "
#     micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
#     micromamba activate "asv_${version}"
#     pip install git+https://github.com/airspeed-velocity/asv
#     pip install -U meson-python "cython<3" "numpy<2" "setuptools==60" "scipy<1.14"
#     # BUILD STEPS GO HERE.
# done
# """.strip(),
#         dockerfile_data=CONTEXT_REGISTRY["asv/default/default"].dockerfile_data,
#         entrypoint_data=CONTEXT_REGISTRY["asv/default/default"].entrypoint_data,
#     ),
# )

CONTEXT_REGISTRY.save_to_file(Path("scratch/context_registry.json"))
