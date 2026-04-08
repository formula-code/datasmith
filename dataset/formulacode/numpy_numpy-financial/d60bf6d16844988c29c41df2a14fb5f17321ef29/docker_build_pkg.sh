#!/usr/bin/env bash
set -euo pipefail

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

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

  # Create and activate environment
  micromamba create -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" pip setuptools wheel \
    numpy cython "meson-python>=0.15.0" \
    compilers cmake ninja pkg-config \
    pytest pytest-xdist hypothesis

  # Fix pyproject.toml format
  log "Fixing pyproject.toml format"
  cat > "$REPO_ROOT/pyproject.toml" << 'EOF'
[build-system]
build-backend = "mesonpy"
requires = [
    "meson-python>=0.15.0",
    "Cython>=3.0.9",
    "numpy>=1.23.5",
]

[project]
name = "numpy-financial"
version = "2.0.0"
requires-python = ">=3.10"
description = "Simple financial functions"
license = {text = "BSD-3-Clause"}
authors = [{name = "Travis E. Oliphant et al."}]
maintainers = [{name = "Numpy Financial Developers", email = "numpy-discussion@python.org"}]
readme = "README.md"
urls = {homepage = "https://numpy.org/numpy-financial/latest/", repository = "https://github.com/numpy/numpy-financial", documentation = "https://numpy.org/numpy-financial/latest/#functions"}
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
    "Operating System :: MacOS",
]

[project.optional-dependencies]
test = [
    "pytest",
    "pytest-xdist",
    "hypothesis",
]
doc = [
    "sphinx>=7.0",
    "numpydoc>=1.5",
    "pydata-sphinx-theme>=0.15",
    "myst-parser>=2.0.0",
]
dev = [
    "ruff>=0.3.0",
    "asv>=0.6.0",
]

[tool.spin]
package = 'numpy_financial'
EOF

  # Install the package
  log "Installing package with pip"
  micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"$EXTRAS

  # Run basic import test
  log "Testing import"
  micromamba run -n "$ENV_NAME" python -c "import $IMP; print($IMP.__version__)"

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
