from __future__ import annotations

from datasmith.docker.context import DockerContext
from datasmith.logging_config import get_logger

logger = get_logger("docker.context_registry")


class ContextRegistry:
    """Registry for Docker contexts to avoid rebuilding the same context multiple times."""

    def __init__(self, registry: dict[str, DockerContext] | None = None, default_context: DockerContext | None = None):
        if registry is None:
            registry = {}
        self.registry = registry

        if "default" not in self.registry:
            if default_context is None:
                default_context = DockerContext()
            self.registry["default"] = default_context
            logger.debug("Default Docker context initialized.")

    def register(self, key: str, context: DockerContext) -> None:
        """Register a new Docker context."""
        if key in self.registry:
            logger.warning(f"Context '{key}' is already registered, overwriting.")
        self.registry[key] = context
        logger.debug(f"Registered Docker context: {key}")

    def get(self, key: str) -> DockerContext:
        """Retrieve a Docker context by key."""
        if key in self.registry:
            return self.registry[key]
        # for asv-owner-repo-sha, check if asv-owner-repo exists.
        logger.debug(f"Context '{key}' not found in registry. Searching for a matching context.")
        owner_repo_key = key.rsplit("-", 1)[0]
        if owner_repo_key in self.registry:
            logger.debug(f"Found context '{owner_repo_key}' for key '{key}'.")
            return self.registry[owner_repo_key]

        logger.info(f"No context found for key '{key}'. Using default context.")
        return self.registry["default"]

    def __getitem__(self, key: str) -> DockerContext:
        return self.get(key)


CONTEXT_REGISTRY = ContextRegistry(default_context=DockerContext())

CONTEXT_REGISTRY.register(
    "asv-astropy-astropy",
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
    micromamba run -n "asv_${version}" asv machine --yes --config $CONF_NAME
    micromamba run -n "asv_${version}" pip install -e . scipy matplotlib
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["default"].entrypoint_data,
    ),
)


CONTEXT_REGISTRY.register(
    "asv-scikit-learn-scikit-learn",
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
    micromamba run -n "asv_${version}" asv machine --yes --config $CONF_NAME
    micromamba run -n "asv_${version}" pip install meson-python cython
    micromamba run -n "asv_${version}" pip install --verbose --no-build-isolation --editable ${ROOT_PATH}
done
""".strip(),
        dockerfile_data=CONTEXT_REGISTRY["default"].dockerfile_data,
        entrypoint_data=CONTEXT_REGISTRY["default"].entrypoint_data,
    ),
)
