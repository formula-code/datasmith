"""The BuildKit cache mounts that make a rebuild cheap must not be silently dropped.

`Dockerfile.pr` declares two `RUN --mount=type=cache` mounts: `/ccache` for
compiled object files and `/opt/uvcache` for downloaded and built wheels.  On
pandas they turn a 10-minute compile into a 1-minute one, and they keep ~1.4 GB
of dead uv cache out of every published image.

Presence alone is not the property worth testing.  The failure mode that costs
real time is *silent decoupling*: rename `CCACHE_DIR` and not the mount target
(or the reverse) and every build still succeeds, just with a cache that is
permanently cold and no error anywhere.  So these tests assert the coupling —
mount target equals the environment variable the tool actually reads — and the
ordering that makes the mounts reachable at all.

The `--no-cache` test guards a separate, sharper edge: a `--no-cache` build
empties a cache mount, and the previously accumulated contents do not come
back.  `python_on_whales.buildx.build` defaults `cache=True`, so the pipeline is
correct today, but it is one keyword argument away from throwing the cache away
on every build.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_DOCKERFILE_PR = _ROOT / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr"

# Cache-directory environment variable -> the mount target that must back it.
_CACHE_MOUNTS = {
    "CCACHE_DIR": "/ccache",
    "UV_CACHE_DIR": "/opt/uvcache",
}

# Every build step that runs an agent-editable or download/compile script.
# `docker_build_run.sh` is on the list because it is agent-editable and the
# stock template already runs `uv pip install`; without a mount those writes
# land in the image layer, which is the thing the mount exists to prevent.
_CACHED_SCRIPTS = ("docker_build_env.sh", "docker_build_pkg.sh", "docker_build_run.sh")

# Build call sites that must never disable BuildKit's cache.
_BUILD_CALL_SITES = (
    _ROOT / "src" / "datasmith" / "docker" / "images.py",
    _ROOT / "src" / "datasmith" / "agents" / "templates" / "local_ci.py",
)


def _dockerfile() -> str:
    assert _DOCKERFILE_PR.is_file(), f"missing template: {_DOCKERFILE_PR}"
    return _DOCKERFILE_PR.read_text()


def _run_instructions(text: str) -> list[str]:
    """Return each RUN instruction as a single logical line (continuations joined)."""
    joined = re.sub(r"\\\n\s*", " ", text)
    return [line for line in joined.splitlines() if line.startswith("RUN ")]


def _env_values(text: str) -> dict[str, str]:
    """Return every `KEY=value` assignment made by an ENV instruction."""
    joined = re.sub(r"\\\n\s*", " ", text)
    values: dict[str, str] = {}
    for line in joined.splitlines():
        if not line.startswith("ENV "):
            continue
        for key, value in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=(\S+)", line[4:]):
            values[key] = value
    return values


@pytest.mark.parametrize(("env_var", "target"), sorted(_CACHE_MOUNTS.items()))
def test_mount_target_matches_cache_dir_env(env_var: str, target: str) -> None:
    """A mount whose target does not match the tool's cache dir is permanently cold."""
    values = _env_values(_dockerfile())
    assert env_var in values, f"Dockerfile.pr no longer sets {env_var}; the {target} mount is now dead"
    assert values[env_var] == target, (
        f"{env_var}={values[env_var]} but the cache mount targets {target}. "
        "The tool would write outside the mount and every build would start cold."
    )


@pytest.mark.parametrize("script", _CACHED_SCRIPTS)
@pytest.mark.parametrize("target", sorted(_CACHE_MOUNTS.values()))
def test_both_mounts_on_every_script_step(script: str, target: str) -> None:
    """Every step that runs a build script needs both mounts."""
    runs = [run for run in _run_instructions(_dockerfile()) if script in run]
    assert runs, f"no RUN instruction invokes {script}"
    for run in runs:
        assert f"type=cache,target={target}" in run, f"RUN invoking {script} lost the {target} cache mount"


@pytest.mark.parametrize("target", sorted(_CACHE_MOUNTS.values()))
def test_mounts_are_shared_not_locked_or_private(target: str) -> None:
    """Concurrency mode is a deliberate choice, not a default to drift away from.

    The pipeline runs up to `--n-concurrent 16` builds against one cache.
    `locked` serializes the most expensive stage of all of them; `private`
    gives each its own copy, so N-1 builds pay full price and discard it.
    """
    mounts = [
        mount
        for run in _run_instructions(_dockerfile())
        for mount in re.findall(rf"--mount=(type=cache,target={re.escape(target)}[^\s]*)", run)
    ]
    assert mounts, f"no cache mount targets {target}"
    for mount in mounts:
        assert "sharing=shared" in mount, f"cache mount on {target} is not sharing=shared: {mount}"


def test_ccache_bootstrap_precedes_the_first_cached_step() -> None:
    """ccache must be on PATH before anything that could compile runs.

    The bootstrap installs ccache and writes the PATH hooks.  If it ever moved
    after `docker_build_env.sh`, uv's isolated sdist builds in the env stage
    would compile uncached and nothing would report it.
    """
    text = _dockerfile()
    bootstrap = text.find("/opt/ccache-bin")
    first_cached_step = text.find("docker_build_env.sh")
    assert bootstrap != -1, "Dockerfile.pr no longer sets up /opt/ccache-bin"
    assert first_cached_step != -1, "Dockerfile.pr no longer runs docker_build_env.sh"
    assert bootstrap < first_cached_step, (
        "the ccache bootstrap must precede docker_build_env.sh, or the env stage compiles uncached"
    )


def test_ccache_bin_is_on_path() -> None:
    """The masquerade directory is useless unless PATH reaches it.

    This covers uv's isolated build environments, which never activate a conda
    env and so never see the activate.d hook.
    """
    values = _env_values(_dockerfile())
    assert "PATH" in values, "Dockerfile.pr no longer sets PATH"
    assert values["PATH"].startswith("/opt/ccache-bin:"), (
        f"PATH={values['PATH']} does not put /opt/ccache-bin first; "
        "uv's isolated sdist builds would use the real compiler directly"
    )


@pytest.mark.parametrize("path", _BUILD_CALL_SITES, ids=lambda p: p.name)
def test_no_build_call_site_disables_the_cache(path: Path) -> None:
    """`--no-cache` empties a cache mount and the old contents never return."""
    assert path.is_file(), f"missing build call site: {path}"
    source = path.read_text()
    offenders = [
        line.strip() for line in source.splitlines() if re.search(r"\b(cache\s*=\s*False|no_cache\s*=\s*True)", line)
    ]
    assert not offenders, (
        f"{path.name} disables the BuildKit cache, which empties the /ccache and "
        f"/opt/uvcache mounts on every build: {offenders}"
    )


def test_ccache_bootstrap_is_not_keyed_on_the_commit() -> None:
    """The bootstrap layer must be reusable across every task in a repo.

    Below `ARG COMMIT_SHA` its cache key carries a per-task input, so it would
    miss on all 1856 tasks — 1856 conda-forge solves instead of one per repo
    image, each of them a network call that can fail into a silently uncached
    build.
    """
    text = _dockerfile()
    bootstrap = text.find("/opt/ccache-bin")
    commit_arg = text.find("ARG COMMIT_SHA")
    assert commit_arg != -1, "Dockerfile.pr no longer declares ARG COMMIT_SHA"
    assert bootstrap < commit_arg, (
        "the ccache bootstrap moved below ARG COMMIT_SHA; its layer would now "
        "miss on every task instead of being built once per repo image"
    )


def test_uncached_build_announces_itself() -> None:
    """A cold cache is the failure that costs the most and shows the least.

    ccache setup is deliberately non-fatal, so a conda-forge outage produces a
    fully uncached build that still succeeds. The only thing separating that
    from a healthy build is this marker in the log.
    """
    text = _dockerfile()
    assert text.count("FORMULACODE_CCACHE_UNAVAILABLE") >= 2, (
        "both the bootstrap's no-ccache branch and the stats step must print "
        "FORMULACODE_CCACHE_UNAVAILABLE, or an uncached build is indistinguishable "
        "from a cached one"
    )
