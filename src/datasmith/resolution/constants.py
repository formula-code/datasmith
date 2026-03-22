"""Constants used in dependency resolution."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# Regular expressions
ASV_REGEX = re.compile(r"(^|/)\.?asv[^/]*\.jsonc?$")
REQ_TXT_REGEX = re.compile(r"(^|/)(?:constraints(?:\.[-\w]+)?|requirements.*)\.txt$")
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
EXTRA_MARKER_RE = re.compile(r';\s*extra\s*==\s*["\']([^"\']+)["\']')

# File names
PYPROJECT = "pyproject.toml"
SETUP_CFG = "setup.cfg"
SETUP_PY = "setup.py"
ENV_YML_NAMES = {"environment.yml", "environment.yaml"}

# Cache location for SQLite caches
CACHE_LOCATION: str = os.getenv("CACHE_LOCATION", "cache.db")

# Git cache directory
GIT_CACHE_DIR = Path(os.getenv("GIT_CACHE_DIR", str(Path(CACHE_LOCATION).parent / "git"))).expanduser()
GIT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Import name to PyPI package name mapping
SPECIAL_IMPORT_TO_PYPI = {
    "sklearn": "scikit-learn",
    "PIL": "Pillow",
    "cv2": "opencv-python",
    "yaml": "PyYAML",
    "bs4": "beautifulsoup4",
    "Crypto": "pycryptodome",
}

# Conda-only and system packages that don't exist on PyPI
CONDA_SYSTEM_PACKAGES = {
    "pkg-config",
    "compilers",
    "c-compiler",
    "cxx-compiler",
    "fortran-compiler",
    "gcc",
    "gxx",
    "gfortran",
    "clang",
    "clangxx",
    "make",
    "cmake",
    "autoconf",
    "automake",
    "libtool",
    "m4",
    "patch",
    "bison",
    "flex",
}

# Packages that are stdlib or not installable via PyPI
NOT_REQUIREMENTS = {
    # py2 names / stdlib modules seen in repos
    "configparser",
    "ConfigParser",
    "asyncore",
    "distutils",
    "sqlite3",
    "tkinter",
    "Tkinter",
    "cStringIO",
    "urllib",
    "urllib2",
    # setuptools internals
    "pkg_resources",
    # platform frameworks (macOS)
    "AppKit",
    "Foundation",
    # Build/packaging tools (not runtime dependencies)
    "py2exe",
    "cx_Freeze",
    "py2app",
    "nuitka",
    # CLI verbs / interpreter references that slip through tokenization
    "python",
    "Python",
    "python3",
    "install",
    "0-29-32",
    "1-0",
    "1-2",
    "1-22-0",
    "1-3-2",
    "2-18-4",
    "2024-1-1",
    "3-0-0a10",
    "absl",
    "afl",
    "allel",
    "cartopy-userconfig",
    "closest-peak-direction-getter",
    "conans",
    "cprofile",
    "dask-core",
    "dateutil",
    "dbe",
    "deepchecks-metrics",
    "geopandas-base",
    "interpnd",
    "jpeg-ls",
    "libblas",
    "libpantab",
    "libwriter",
    "mo-pack",
    "mpl-toolkits",
    "pylab",
    "pyqt4",
    "pytables",
    "skbuild",
    "sklearnex",
    "skspatial",
    "system",
    "tunits",
    "vcr",
    "0-29-21",
    "0-29-33",
    "1-11-2",
    "1-12",
    "1-14-0",
    "1-23-5",
    "1-8-1",
    "3-0",
    "3-0-0a11",
    "3-1-2",
    "59-2-0",
    "c-distances-openmp",
    "column-parsers",
    "copy-reg",
    "cpickle",
    "cryptodome",
    "cupyx",
    "h5r",
    "h5s",
    "h5t",
    "givens-elimination",
    "imp",
    "libreader",
    "nattype",
    "omniscidbe",
    "openjpeg",
    "patoolib",
    "peerplaysbase",
    "probabilistic-direction-getter",
    "pyhdk",
    "pymake",
    "pyqt",
    "sksparse",
    "splitting",
    "stringio",
    "uninstall",
    "urlparse",
    "0-29-30",
    "1-9-1",
    "2-2",
    "3-0-5",
    "3-2-0",
    "backports",
    "cdms2",
    "flatted",
    "h5z",
    "pnetdicom",
    "pypocketfft",
    "vectorized",
    "voyager-ext",
}

# Well-known PyPI packages (allowlist for common names)
ALLOWLIST_COMMON_PYPI = {
    "numpy",
    "scipy",
    "pandas",
    "matplotlib",
    "xarray",
    "shapely",
    "fiona",
    "pyproj",
    "rtree",
    "torch",
    "functorch",
    "pytest",
    "ipython",
    "IPython",
    "ipykernel",
    "ipywidgets",
    "Cython",
    "cython",
    "numba",
    "scikit-learn",
    "sklearn",
    "sympy",
    "h5py",
    "Pillow",
    "pillow",
    "networkx",
    "dask",
    "seaborn",
    "xgboost",
    "statsmodels",
    "pyarrow",
    "geopandas",
    "cartopy",
    "tqdm",
    "psycopg2",
    "sqlalchemy",
    "SQLAlchemy",
    "requests",
    "setuptools",
    "wheel",
    "pip",
    "sphinx",
    "nbsphinx",
    "sphinx-gallery",
    "black",
    "isort",
    "flake8",
    "hypothesis",
    "pyqt5",
    "pyqt4",
    "qtpy",
    "jupyter",
    "pooch",
    "graphviz",
    "numexpr",
}

# Generic names that are likely local modules, not PyPI packages
GENERIC_LOCAL_NAMES = {
    "lib",
    "libs",
    "utils",
    "util",
    "utilities",
    "core",
    "helpers",
    "helper",
    "common",
    "base",
    "tools",
    "tool",
    "config",
    "configs",
    "constants",
    "const",
    "types",
    "models",
    "model",
    "tests",
    "test",
    "testing",
    "benchmarks",
    "benchmark",
    "examples",
    "example",
    "scripts",
    "script",
    "data",
    "docs",
    "doc",
    "documentation",
}

# Python stdlib modules (Python 3.10+)
try:
    STDLIB = set(sys.stdlib_module_names)
except Exception:  # pragma: no cover
    STDLIB = set()
