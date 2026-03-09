#!/usr/bin/env bash
set -euo pipefail

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-numpy_financial}"
  log "Using import name: $IMP"

  # Create temporary pyproject.toml with fixed formatting
  cat > "$REPO_ROOT/pyproject.toml.tmp" << 'EOF'
[build-system]
build-backend = "mesonpy"
requires = ["meson-python>=0.15.0", "Cython>=3.0.9", "numpy>=1.23.5"]

[project]
name = "numpy-financial"
version = "2.0.0"
requires-python = ">=3.10"
description = "Simple financial functions"
license = { text = "BSD-3-Clause" }
authors = [
    { name = "Travis E. Oliphant et al." }
]
maintainers = [
    { name = "Numpy Financial Developers", email = "numpy-discussion@python.org" }
]
readme = "README.md"
homepage = "https://numpy.org/numpy-financial/latest/"
repository = "https://github.com/numpy/numpy-financial"
documentation = "https://numpy.org/numpy-financial/latest/#functions"
classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Intended Audience :: Financial and Insurance Industry",
    "License :: OSI Approved :: BSD License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3 :: Only",
    "Topic :: Software Development",
    "Topic :: Office/Business :: Financial :: Accounting",
    "Topic :: Office/Business :: Financial :: Investment",
    "Topic :: Office/Business :: Financial :: Spreadsheet",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Operating System :: Unix",
    "Operating System :: MacOS"
]

[project.optional-dependencies]
test = ["pytest", "pytest-xdist", "hypothesis"]
doc = ["sphinx>=7.0", "numpydoc>=1.5", "pydata-sphinx-theme>=0.15", "myst-parser>=2.0.0"]
dev = ["ruff>=0.3.0", "asv>=0.6.0"]

[tool.spin]
package = "numpy_financial"

[tool.spin.commands]
"Build" = ["spin.cmds.meson.build", "spin.cmds.meson.test", "spin.cmds.build.sdist", "spin.cmds.pip.install"]
"Documentation" = ["spin.cmds.meson.docs"]
"Environments" = ["spin.cmds.meson.shell", "spin.cmds.meson.ipython", "spin.cmds.meson.python", "spin.cmds.meson.run"]
EOF

  mv "$REPO_ROOT/pyproject.toml.tmp" "$REPO_ROOT/pyproject.toml"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=1.23.5" scipy "cython>=3.0.9" \
    "meson-python>=0.15.0" cmake ninja pkg-config \
    compilers pytest tomli

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
      warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
