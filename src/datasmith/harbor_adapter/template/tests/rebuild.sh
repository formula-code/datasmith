#!/usr/bin/env bash
# Rebuilds the repo's compiled extensions the way the image build installed them; the agent gets it as rebuild-repo.
set -eo pipefail
cd /workspace/repo
# The Python project can sit in a subfolder (nanoarrow: python/); rebuild where the package was installed from.
if [ ! -f setup.py ] && [ ! -f pyproject.toml ]; then
  proj="$(python -c 'import importlib.metadata as m, json; print(next((u[7:] for d in m.distributions() for u in [json.loads(d.read_text("direct_url.json") or "{}").get("url", "")] if u.startswith("file:///workspace/repo")), ""))')"
  if [ -n "${proj}" ]; then cd "${proj}"; fi
fi
# setuptools >= 82 has no pkg_resources; old setup.py files import it (the image build shims it the same way).
if ! python -c "import pkg_resources" 2>/dev/null; then
  shim="$(mktemp -d)"; echo 'from pip._vendor.pkg_resources import *' > "${shim}/pkg_resources.py"
  export PYTHONPATH="${shim}${PYTHONPATH:+:${PYTHONPATH}}"
fi
# TileDB-Py links the env's libtiledb (as in the image build) instead of downloading and compiling one.
if [ -e "${CONDA_PREFIX:-/nonexistent}/lib/libtiledb.so" ]; then export TILEDB_PATH="${TILEDB_PATH:-${CONDA_PREFIX}}"; fi
# Old setup.py files add numpy's headers only in a build step that editable installs skip (TileDB-Py#1005).
NP_INC="$(python -c 'import numpy; print(numpy.get_include())' 2>/dev/null || true)"
if [ -n "${NP_INC}" ]; then export CPPFLAGS="-I${NP_INC}${CPPFLAGS:+ ${CPPFLAGS}}"; fi
# GCC 14 made these errors; code that built with older compilers (shapely#1562's ufuncs.c) must still build.
export CFLAGS="-Wno-error=incompatible-pointer-types -Wno-error=int-conversion -Wno-error=implicit-function-declaration${CFLAGS:+ ${CFLAGS}}"
# Same install as docker_build_pkg.sh.
# The image build falls back to an isolated build when this fails (old numpy needs its own setuptools); do the same.
if ! PIP_NO_BUILD_ISOLATION=1 python -m pip install --no-build-isolation --no-deps -v -e .; then
  echo "no-isolation rebuild failed; retrying with an isolated build" >&2
  if ! python -m pip install --no-deps -v -e .; then
    # Old setup.py projects (numpy 1.16, skimage 0.19) run from the repo; rebuilding their extensions in place is enough.
    # numpy.distutils fails with setuptools >= 60's own distutils, hence stdlib.
    [ -f setup.py ] || exit 1
    echo "isolated rebuild failed; building extensions in place" >&2
    SETUPTOOLS_USE_DISTUTILS=stdlib python setup.py build_ext --inplace
  fi
fi
