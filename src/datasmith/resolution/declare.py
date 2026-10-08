"""Collect the dependencies a project actually declares.

The predecessor built its requirement set from four sources, three of which are
not declarations:

* ``requirements*.txt`` globbed anywhere in the tree — which is how ``sphinx``,
  ``towncrier``, ``ruff``, ``PyInstaller``, ``myst-nb``, ``jupytext``, ``torch``,
  ``jax``, ``cupy`` and ``conda-build`` became *runtime* dependencies.  scipy's
  resolution failed on a ``conda-build`` yanked-version conflict: a documentation
  requirement made the environment unsatisfiable.
* ``environment.yml`` — conda names such as ``boost-cpp``, ``thrift-cpp``,
  ``libprotobuf`` and ``lz4-c``, which do not exist on PyPI.
* import analysis — which produced ``arraypad``, ``multiarray``, ``umath``,
  ``mtrand`` (numpy's own submodules), plus ``version`` and ``plex``.  ``version``
  is a dead py2 distribution whose sdist raises ``ImportError: cannot import name
  'izip_longest'``, and that single harvested token is what failed numpy.

Only these are declarations, and only these are read here:

* ``[project].dependencies`` / ``[project.optional-dependencies]``
* ``install_requires`` / ``options.extras_require``
* ``[build-system].requires``
* ASV ``matrix.req`` — a genuine statement of what the benchmarks need
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from .models import CandidateMeta
from .requirements import Dropped, parse_many, render

__all__ = ["Declared", "declare"]


@dataclass(frozen=True)
class Declared:
    """What a project says it needs."""

    runtime: list[str] = field(default_factory=list)
    build: list[str] = field(default_factory=list)
    extras: dict[str, list[str]] = field(default_factory=dict)
    dropped: list[Dropped] = field(default_factory=list)


def _matrix_requirements(matrix: Mapping[str, set[str]] | None) -> list[str]:
    """Turn an ASV ``matrix`` into requirement strings.

    The keys carry the package names.  Passing a bare version such as ``0.29.21``
    to the resolver treats it as a package name, so the key must not be dropped.

    Four shapes of version set mean four different things, and conflating them
    either inverts the answer or destroys the seed:

    * an empty set -- the matrix names the package with no version, meaning "any
      version": emit the bare name;
    * a set holding exactly one real version -- emit that pin;
    * a set holding several real versions -- the matrix asks for a *sweep*, which
      a single environment cannot be.  Emitting one ``==`` pin per version makes
      the set unsatisfiable and takes every unrelated requirement down with it
      (lmfit-py names ``scipy`` at ``0.18`` and ``0.19``, and its whole seed came
      back empty).  A seed is one environment, so it states the weakest true
      thing: the bare name;
    * a set whose every entry is ``"None"`` -- ASV's ``null``, meaning "do not
      install in this combination".  Emit **nothing**.  Falling through to the
      bare name here would turn "never install cython" into "install any cython".
    """
    out: list[str] = []
    for pkg, versions in (matrix or {}).items():
        name = str(pkg).strip()
        if not name or name.startswith("-"):
            continue
        stated = {v for v in (str(x).strip() for x in versions) if v}
        real = {v for v in stated if v != "None"}
        if len(real) == 1:
            out.append(f"{name}=={next(iter(real))}")
        elif real or not stated:
            out.append(name)
    return out


def declare(meta: CandidateMeta, asv_matrix: Mapping[str, set[str]] | None) -> Declared:
    """Collect declared runtime, build and extra requirements."""
    dropped: list[Dropped] = []

    runtime_raw = list(meta.core_deps) + _matrix_requirements(asv_matrix)
    runtime_reqs, runtime_dropped = parse_many(runtime_raw)
    dropped.extend(runtime_dropped)

    build_reqs, build_dropped = parse_many(meta.build_requires)
    dropped.extend(build_dropped)

    extras: dict[str, list[str]] = {}
    for name in sorted(meta.extras):
        extra_reqs, extra_dropped = parse_many(meta.extras[name])
        dropped.extend(extra_dropped)
        extras[name] = render(extra_reqs)

    return Declared(
        runtime=render(runtime_reqs),
        build=render(build_reqs),
        extras=extras,
        dropped=sorted(dropped, key=lambda d: d.raw),
    )
