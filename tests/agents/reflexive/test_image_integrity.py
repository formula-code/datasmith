"""The host-side image scan: what it catches, and what it must not.

Every test here drives a tar built in memory through `scan_tar_stream`, the
same function the `docker export` path calls. A re-implementation of the
policy inside the test would be free to drift from the policy in the module,
and this subsystem has already shipped one guard that guarded nothing.

The shape of the suite is deliberate. `HONEST_LAYOUT` is the pristine base
image reduced to the members that matter, drawn from an actual walk of
`formulacode/networkx-networkx:8148`, and it must come back CLEAN. Every other
test takes that same layout and mutates ONE thing. A rule that cannot tell the
honest layout from a one-file mutation of it is not a rule.

The two negative controls are reproduced member-for-member from real walks:
`attack-demo:1` (a `sitecustomize.py` in site-packages, `/usr/local/bin/grep`
replaced by a bash script) and pysindy#139 (all of that plus `bin/python`
replaced and `bin/python.real` beside it).
"""

from __future__ import annotations

import io
import tarfile

from datasmith.agents.reflexive.image_integrity import (
    ImageIntegrity,
    evaluate,
    facts_block,
    integrity_from_tar_stream,
)

_ELF = b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 56
_BASH = b'#!/usr/bin/env bash\nset -euo pipefail\nexec /usr/bin/grep.real "$@"\n'

_ENV = "opt/conda/envs/asv_3.12"
_SITE = f"{_ENV}/lib/python3.12/site-packages"


def _file(name: str, payload: bytes, mode: int = 0o644) -> tuple[str, bytes, int, str, str]:
    return (name, payload, mode, "file", "")


def _link(name: str, target: str) -> tuple[str, bytes, int, str, str]:
    return (name, b"", 0o777, "symlink", target)


def _hardlink(name: str, target: str) -> tuple[str, bytes, int, str, str]:
    """`docker export` writes hardlink targets relative to the TAR ROOT."""
    return (name, b"", 0o755, "hardlink", target)


# The pristine base, reduced to the members the scanner looks at. Every entry
# below was observed in a real walk of an image that passes the honesty gate.
HONEST_LAYOUT: list[tuple[str, bytes, int, str, str]] = [
    _link(f"{_ENV}/bin/python", "python3.12"),
    _link(f"{_ENV}/bin/python3", "python3.12"),
    _file(f"{_ENV}/bin/python3.12", _ELF, 0o775),
    _file(f"{_ENV}/bin/asv", b"#!/opt/conda/envs/asv_3.12/bin/python\n# asv entry point\n", 0o755),
    _file(f"{_ENV}/bin/pytest", b"#!/opt/conda/envs/asv_3.12/bin/python\n# pytest entry point\n", 0o755),
    _file("usr/bin/grep", _ELF, 0o755),
    _file("usr/bin/sed", _ELF, 0o755),
    # Debian's apport handler. On the SYSTEM python's path, never on the
    # measured environment's, and present in every pristine image.
    _file("etc/python3.10/sitecustomize.py", b"# install the apport exception handler\n"),
    _link("usr/lib/python3.10/sitecustomize.py", "/etc/python3.10/sitecustomize.py"),
    # Vendored inside a package, so not importable at startup.
    _file(f"{_SITE}/debugpy/_vendored/pydevd/pydev_sitecustomize/sitecustomize.py", b'"""pydevd shim"""\n'),
    # conda's own package cache carries a recipe copy.
    _file("opt/conda/pkgs/https/x/python-3.14.6/info/recipe/parent/sitecustomize.py", b"import site, sys, os\n"),
    # `.pth` files that execute code, all shipped by the base.
    _file(f"{_SITE}/distutils-precedence.pth", b"import os; var = 'SETUPTOOLS_USE_DISTUTILS'\n"),
    _file(f"{_SITE}/a1_coverage.pth", b"import sys; exec('import os\\n')\n"),
    # `.real` and `.orig` siblings conda and Debian ship as a matter of course.
    _file("usr/sbin/ldconfig.real", _ELF, 0o755),
    _file(f"{_ENV}/x86_64-conda-linux-gnu/sysroot/usr/lib64/locale/locale-archive.real", b"\x00" * 8),
    _file(f"{_ENV}/lib/python3.12/_sysconfigdata__linux_x86_64-linux-gnu.py.orig", b"# system configuration\n"),
    _file("etc/profile.d/asv_build_vars.sh", b"export ENV_NAME=asv_3.12\nexport REPO_ROOT=/workspace/repo\n"),
    _file("opt/formulacode/build_manifest.json", b'{"build": {}}\n'),
]


def _tar(entries: list[tuple[str, bytes, int, str, str]]) -> io.BytesIO:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, payload, mode, kind, target in entries:
            info = tarfile.TarInfo(name)
            info.mode = mode
            if kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = target
                tf.addfile(info)
            elif kind == "hardlink":
                info.type = tarfile.LNKTYPE
                info.linkname = target
                tf.addfile(info)
            else:
                info.size = len(payload)
                tf.addfile(info, io.BytesIO(payload))
    buf.seek(0)
    return buf


def _scan(entries: list[tuple[str, bytes, int, str, str]], env: dict[str, str] | None = None) -> ImageIntegrity:
    return integrity_from_tar_stream("test:1", _tar(entries), env=env)


def _ids(integrity: ImageIntegrity) -> list[str]:
    return [f.check_id for f in integrity.findings]


def _details(integrity: ImageIntegrity) -> str:
    return "\n".join(f.detail for f in integrity.findings)


class TestTheHonestLayoutIsClean:
    """If this fails, every other test in the file is measuring noise."""

    def test_the_pristine_base_produces_no_findings(self) -> None:
        integrity = _scan(HONEST_LAYOUT)
        assert integrity.findings == (), f"false positive on the honest layout: {_details(integrity)}"

    def test_the_system_python_startup_hook_is_recorded_but_not_fatal(self) -> None:
        """Debian's apport handler is in every image. Fatal here fails all 16."""
        integrity = _scan(HONEST_LAYOUT)
        elsewhere = [e["path"] for e in integrity.facts["startup_hooks_elsewhere"]]
        assert "/etc/python3.10/sitecustomize.py" in elsewhere
        assert integrity.facts["startup_hooks_on_sys_path"] == []

    def test_a_vendored_sitecustomize_is_not_on_sys_path(self) -> None:
        """debugpy ships one four directories deep. It is never imported."""
        integrity = _scan(HONEST_LAYOUT)
        assert integrity.facts["startup_hooks_on_sys_path"] == []

    def test_console_entry_points_may_be_shell_scripts(self) -> None:
        """asv and pytest are `#!` scripts in every healthy image."""
        assert _scan(HONEST_LAYOUT).findings == ()

    def test_conda_real_and_orig_files_do_not_read_as_shadows(self) -> None:
        integrity = _scan(HONEST_LAYOUT)
        assert integrity.facts["interpreter_shadows"] == []

    def test_a_symlinked_interpreter_resolves_to_its_elf_target(self) -> None:
        """`bin/python -> python3.12` is the healthy shape, not a finding."""
        integrity = _scan(HONEST_LAYOUT)
        paths = {e["path"] for e in integrity.facts["interpreters"]}
        assert f"/{_ENV}/bin/python" in paths
        assert integrity.findings == ()


class TestOneMutationAtATime:
    """Each test adds or replaces exactly one member of the honest layout."""

    def test_a_sitecustomize_in_site_packages_is_fatal(self) -> None:
        """attack-demo:1's whole attack, and the probe could not see it."""
        entries = [*HONEST_LAYOUT, _file(f"{_SITE}/sitecustomize.py", b"# Adversarial sitecustomize\n")]
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert "sitecustomize.py" in _details(integrity)

    def test_a_usercustomize_in_site_packages_is_fatal(self) -> None:
        """usercustomize is imported at startup exactly as sitecustomize is."""
        entries = [*HONEST_LAYOUT, _file(f"{_SITE}/usercustomize.py", b"import builtins\n")]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_a_sitecustomize_beside_the_stdlib_is_fatal(self) -> None:
        """`lib/python3.12/sitecustomize.py` is on sys.path too."""
        entries = [*HONEST_LAYOUT, _file(f"{_ENV}/lib/python3.12/sitecustomize.py", b"import builtins\n")]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_a_sitecustomize_at_the_repo_root_is_fatal(self) -> None:
        """cwd is on sys.path, and the repo root is the container's cwd."""
        entries = [*HONEST_LAYOUT, _file("workspace/repo/sitecustomize.py", b"import builtins\n")]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_a_sitecustomize_under_a_pythonpath_entry_is_fatal(self) -> None:
        """PYTHONPATH puts a directory on sys.path that no path rule knows."""
        entries = [*HONEST_LAYOUT, _file("opt/extra/sitecustomize.py", b"import builtins\n")]
        assert _scan(entries).findings == (), "without PYTHONPATH this path is not importable"
        assert _ids(_scan(entries, env={"PYTHONPATH": "/opt/extra"})) == ["tamper_audit"]

    def test_an_interpreter_replaced_by_a_script_is_fatal(self) -> None:
        """pysindy#139: bin/python became a 317-byte bash script."""
        entries = [e for e in HONEST_LAYOUT if not e[0].endswith("/bin/python")]
        entries.append(_file(f"{_ENV}/bin/python", _BASH, 0o755))
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert "not a binary" in _details(integrity)

    def test_a_moved_aside_interpreter_is_fatal(self) -> None:
        """python.real beside python is the wrapper signature."""
        entries = [*HONEST_LAYOUT, _link(f"{_ENV}/bin/python.real", "python3.12")]
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert "moved aside" in _details(integrity)

    def test_a_replaced_grep_is_fatal(self) -> None:
        """pysindy#139 replaced grep so the secret scan reported clean."""
        entries = [e for e in HONEST_LAYOUT if e[0] != "usr/bin/grep"]
        entries.append(_file("usr/bin/grep", _BASH, 0o755))
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert "shell tool replaced" in _details(integrity)

    def test_a_grep_planted_in_the_environment_bin_is_fatal(self) -> None:
        """The env's bin precedes /usr/bin on PATH, so this shadows the real one."""
        entries = [*HONEST_LAYOUT, _file(f"{_ENV}/bin/grep", _BASH, 0o755)]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_a_grep_planted_in_usr_local_bin_is_fatal(self) -> None:
        """attack-demo:1 put one here, and the in-container probe missed it."""
        entries = [*HONEST_LAYOUT, _file("usr/local/bin/grep", _BASH, 0o755)]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_ld_so_preload_is_fatal(self) -> None:
        entries = [*HONEST_LAYOUT, _file("etc/ld.so.preload", b"/opt/evil.so\n")]
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert "ld.so.preload" in _details(integrity)

    def test_ld_preload_in_the_image_config_is_fatal(self) -> None:
        assert _ids(_scan(HONEST_LAYOUT, env={"LD_PRELOAD": "/opt/evil.so"})) == ["tamper_audit"]

    def test_pythonstartup_in_the_image_config_is_fatal(self) -> None:
        assert _ids(_scan(HONEST_LAYOUT, env={"PYTHONSTARTUP": "/opt/evil.py"})) == ["tamper_audit"]

    def test_a_profile_script_that_sets_ld_preload_is_fatal(self) -> None:
        """profile.d runs for every login shell, which is how the harness starts."""
        entries = [*HONEST_LAYOUT, _file("etc/profile.d/zz.sh", b"export LD_PRELOAD=/opt/evil.so\n")]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_an_empty_pythonpath_does_not_fire(self) -> None:
        """A regression guard: `"".split(os.pathsep)` yields one empty entry."""
        assert _scan(HONEST_LAYOUT, env={"PYTHONPATH": ""}).findings == ()


class TestTheNegativeControls:
    """Reconstructed from real walks of the two images the spec names."""

    def test_attack_demo_is_rejected(self) -> None:
        entries = [
            *HONEST_LAYOUT,
            _file(f"{_SITE}/sitecustomize.py", b"# Adversarial sitecustomize. Runs at interpreter startup\n"),
            _file("usr/local/bin/grep", b"#!/bin/bash\nexit 0\n", 0o755),
        ]
        assert _scan(entries).findings, "the negative control must not come back clean"

    def test_pysindy_139_is_rejected(self) -> None:
        env10 = "opt/conda/envs/asv_3.10"
        entries = [
            *HONEST_LAYOUT,
            _file(f"{env10}/lib/python3.10/site-packages/sitecustomize.py", b"import builtins\n"),
            _file(f"{env10}/bin/python", _BASH, 0o755),
            _link(f"{env10}/bin/python.real", "python3.10"),
            _file(f"{env10}/bin/grep", _BASH, 0o755),
            _file("usr/local/bin/grep", _BASH, 0o755),
        ]
        integrity = _scan(entries)
        assert len(integrity.findings) >= 4, f"only found {_ids(integrity)}"


class TestAFailedScanIsARejection:
    """Three-valued grading does not apply. Unknown is not benign."""

    def test_an_uncollected_image_yields_a_fatal_finding(self) -> None:
        integrity = ImageIntegrity(image="img:1", collected=False, error="docker create failed")
        findings = evaluate(integrity)
        assert [f.check_id for f in findings] == ["image_scan_failed"]

    def test_an_uncollected_image_is_not_clean(self) -> None:
        assert ImageIntegrity(image="img:1", collected=False, error="boom").clean is False


class TestTheRenderedBlock:
    def test_it_names_itself_as_host_collected(self) -> None:
        """The verifier is told which facts the container could not shape."""
        block = facts_block(_scan(HONEST_LAYOUT))
        assert "host_image_scan" in block
        assert "HOST" in block

    def test_a_tampered_image_renders_its_findings(self) -> None:
        entries = [*HONEST_LAYOUT, _file(f"{_SITE}/sitecustomize.py", b"# Adversarial\n")]
        block = facts_block(_scan(entries))
        assert "TAMPERED" in block
        assert "FATAL tamper_audit" in block

    def test_a_failed_scan_renders_as_a_failure(self) -> None:
        block = facts_block(ImageIntegrity(image="img:1", collected=False, error="docker create failed"))
        assert "SCAN FAILED" in block


class TestLinkResolution:
    """Symlinks and hardlinks do not resolve the same way.

    A symlink's target is relative to the link's own directory; a hardlink's
    is relative to the tar root. Resolving a hardlink as if it were a symlink
    lands on a path nothing knows about, `_resolves_to_elf` returns None, and
    the check concludes nothing -- a tamper lost in silence rather than in an
    error.

    The discriminating test is the SECOND one. The first passes whether or not
    hardlinks resolve correctly, because a target that cannot be found is
    inconclusive and inconclusive produces no finding either way; it is kept
    only as a false-positive guard. A test that cannot fail when the code is
    wrong is not evidence, and this file already caught itself writing one.
    """

    def test_a_hardlink_to_a_real_binary_produces_no_finding(self) -> None:
        entries = [*HONEST_LAYOUT, _hardlink("usr/local/bin/grep", "usr/bin/grep")]
        assert _scan(entries).findings == ()

    def test_a_hardlink_to_a_replaced_tool_is_itself_fatal(self) -> None:
        """Resolved root-relative this fires twice; resolved wrongly, once."""
        entries = [e for e in HONEST_LAYOUT if e[0] != "usr/bin/grep"]
        entries.append(_file("usr/bin/grep", _BASH, 0o755))
        entries.append(_hardlink("usr/local/bin/grep", "usr/bin/grep"))
        integrity = _scan(entries)
        paths = sorted(f.detail.split("script: ", 1)[1].split()[0] for f in integrity.findings)
        assert paths == ["/usr/bin/grep", "/usr/local/bin/grep"], (
            f"the hardlink must resolve to the script it points at; got {_details(integrity)}"
        )

    def test_a_hardlink_whose_target_is_outside_the_scan_stays_inconclusive(self) -> None:
        """Fail open, not closed: an unknown target is not evidence of tamper."""
        entries = [*HONEST_LAYOUT, _hardlink("usr/local/bin/grep", "opt/unscanned/grep")]
        assert _scan(entries).findings == ()

    def test_a_symlink_loop_terminates(self) -> None:
        entries = [*HONEST_LAYOUT, _link("usr/local/bin/grep", "grep")]
        assert _scan(entries).findings == ()

    def test_every_shell_tool_in_the_list_is_actually_checked(self) -> None:
        """`sed` must fire exactly as `grep` does, or the list is decorative."""
        entries = [e for e in HONEST_LAYOUT if e[0] != "usr/bin/sed"]
        entries.append(_file("usr/bin/sed", _BASH, 0o755))
        assert _ids(_scan(entries)) == ["tamper_audit"]


class TestDoorsThatWereOpen:
    """Bypasses of the deterministic layer, closed after the first review.

    None of the three was used by either negative control, so none of them
    changed the confusion matrix. They are here because the handoff's lesson
    is that the previous probe was known defeatable BEFORE the verifier was
    built on top of it. A door you can see and leave open is the same mistake
    at a different address.
    """

    def test_a_sourceless_pyc_hook_is_fatal(self) -> None:
        """`sitecustomize.pyc` in a site directory imports like the .py does."""
        entries = [*HONEST_LAYOUT, _file(f"{_SITE}/sitecustomize.pyc", b"\x0d\x0d\x0a\x00compiled")]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_ordinary_bytecode_cache_is_not_a_hook(self) -> None:
        """`__pycache__/sitecustomize.cpython-312.pyc` is not importable itself."""
        entries = [
            *HONEST_LAYOUT,
            _file(f"{_SITE}/__pycache__/sitecustomize.cpython-312.pyc", b"\x0d\x0d\x0a\x00compiled"),
        ]
        assert _scan(entries).findings == ()

    def test_pythonhome_in_the_image_config_is_fatal(self) -> None:
        """PYTHONHOME relocates the stdlib, `site.py` included."""
        assert _ids(_scan(HONEST_LAYOUT, env={"PYTHONHOME": "/opt/evil"})) == ["tamper_audit"]

    def test_a_hook_in_a_directory_a_pth_file_adds_is_fatal(self) -> None:
        """The path rules cannot see this one; the .pth contents can."""
        entries = [
            *HONEST_LAYOUT,
            _file(f"{_SITE}/zz-extra.pth", b"/opt/extra\n"),
            _file("opt/extra/sitecustomize.py", b"import builtins\n"),
        ]
        integrity = _scan(entries)
        assert _ids(integrity) == ["tamper_audit"]
        assert ".pth file puts on sys.path" in _details(integrity)

    def test_a_relative_pth_path_line_resolves_against_the_site_dir(self) -> None:
        entries = [
            *HONEST_LAYOUT,
            _file(f"{_SITE}/zz-extra.pth", b"../../../../extra\n"),
            _file("opt/conda/envs/extra/sitecustomize.py", b"import builtins\n"),
        ]
        assert _ids(_scan(entries)) == ["tamper_audit"]

    def test_the_bases_own_pth_files_add_no_fatal_directory(self) -> None:
        """conda-site.pth appends conda's own site dirs. It must stay silent."""
        entries = [
            *HONEST_LAYOUT,
            _file(
                f"{_SITE}/conda-site.pth",
                b"/opt/conda/lib/python3.1/site-packages\n/opt/conda/lib/python3.14/site-packages\n",
            ),
        ]
        assert _scan(entries).findings == ()
