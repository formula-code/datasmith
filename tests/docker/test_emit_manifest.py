"""Tests for the in-image manifest sealer."""

import importlib.util
import json
from pathlib import Path

_SEALER = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "emit_manifest.py"


def _load():
    """Import the sealer by path — it is a template, not a package module."""
    spec = importlib.util.spec_from_file_location("emit_manifest", _SEALER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestParseNotes:
    def test_parses_key_value_lines(self):
        m = _load()
        notes = m.parse_notes(['{"k": "discovered_n", "v": "3"}'])
        assert notes["discovered_n"] == "3"

    def test_last_write_wins(self):
        m = _load()
        notes = m.parse_notes(['{"k": "cpu_cap", "v": "128"}', '{"k": "cpu_cap", "v": "4"}'])
        assert notes["cpu_cap"] == "4"

    def test_malformed_line_is_skipped_not_fatal(self):
        m = _load()
        notes = m.parse_notes(["not json at all", '{"k": "rounds", "v": "5"}'])
        assert notes == {"rounds": "5"}

    def test_empty_input_yields_empty_dict(self):
        m = _load()
        assert m.parse_notes([]) == {}

    def test_valid_line_after_malformed_line_is_parsed(self):
        """Prove partial degradation: one malformed line does not skip valid ones."""
        m = _load()
        notes = m.parse_notes([
            '{"k": "discovered_n", "v": "1"}',
            '{"invalid json"',  # Malformed
            '{"k": "cpu_cap", "v": "8"}',
        ])
        assert notes["discovered_n"] == "1"
        assert notes["cpu_cap"] == "8"

    def test_recursion_error_line_is_skipped_not_fatal(self):
        """RecursionError from deeply nested JSON must not crash parse_notes."""
        m = _load()
        # Deeply nested JSON triggers RecursionError in json.loads
        notes = m.parse_notes(["[" * 100000, '{"k": "valid", "v": "yes"}'])
        # The recursion-error line is skipped, but the valid line is parsed
        assert notes == {"valid": "yes"}


class TestBuildBlock:
    def test_coerces_declared_types(self):
        m = _load()
        block = m.build_block(
            {"discovered_n": "3", "cpu_cap": "4", "discovery_fallback_used": "0"},
            {},
        )
        assert block["discovered_n"] == 3
        assert block["cpu_cap"] == 4
        assert block["discovery_fallback_used"] is False

    def test_missing_breadcrumb_becomes_none(self):
        m = _load()
        block = m.build_block({}, {})
        assert block["discovered_n"] is None
        assert block["benchmark_dest"] is None

    def test_list_fields_split_on_whitespace(self):
        m = _load()
        block = m.build_block({"pins_requested": "scipy<=1.10 numpy>=1.20"}, {})
        assert block["pins_requested"] == ["scipy<=1.10", "numpy>=1.20"]

    def test_introspected_values_win_over_breadcrumbs(self):
        m = _load()
        block = m.build_block({"head_at_seal": "aaa"}, {"head_at_seal": "bbb"})
        assert block["head_at_seal"] == "bbb"

    def test_bad_int_becomes_none_not_crash(self):
        m = _load()
        block = m.build_block({"discovered_n": "not-a-number"}, {})
        assert block["discovered_n"] is None

    def test_infinity_int_becomes_none_not_crash(self):
        """Infinity from env vars (e.g. cpu_cap=inf) must not crash."""
        m = _load()
        block = m.build_block({"cpu_cap": "inf"}, {})
        assert block["cpu_cap"] is None


class TestMain:
    def test_writes_manifest_with_empty_verify_block(self, tmp_path):
        m = _load()
        notes = tmp_path / "notes.jsonl"
        notes.write_text('{"k": "discovered_n", "v": "7"}\n')
        out = tmp_path / "build_manifest.json"

        rc = m.main(["--notes", str(notes), "--out", str(out)])

        assert rc == 0
        written = json.loads(out.read_text())
        assert written["schema_version"] == 1
        assert written["build"]["discovered_n"] == 7
        assert written["verify"] == {}

    def test_missing_notes_file_still_writes_manifest(self, tmp_path):
        """A build that emitted no breadcrumbs must still seal a manifest."""
        m = _load()
        out = tmp_path / "build_manifest.json"

        rc = m.main(["--notes", str(tmp_path / "absent.jsonl"), "--out", str(out)])

        assert rc == 0
        written = json.loads(out.read_text())
        assert written["build"]["discovered_n"] is None

    def test_invalid_utf8_in_notes_file_still_writes_manifest(self, tmp_path):
        """A notes file with truncated/invalid UTF-8 must not crash main()."""
        m = _load()
        notes = tmp_path / "notes.jsonl"
        # Write valid JSON, then truncate with invalid UTF-8 byte sequence
        notes.write_bytes(b'{"k": "discovered_n", "v": "3"}\n\xff\xfe')
        out = tmp_path / "build_manifest.json"

        rc = m.main(["--notes", str(notes), "--out", str(out)])

        assert rc == 0
        written = json.loads(out.read_text())
        assert written["schema_version"] == 1
        # The valid line should be parsed, invalid bytes replaced
        assert written["build"]["discovered_n"] == 3


class TestBuildWiring:
    def test_dockerfile_copies_and_runs_the_sealer(self):
        """The sealer must run from the Dockerfile, after build_final_sh.

        Invoking it from docker_build_final.sh would be bypassed by any
        context carrying a stored build_final_sh.
        """
        dockerfile = (
            Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr"
        ).read_text()
        assert "COPY emit_manifest.py /emit_manifest.py" in dockerfile
        assert "emit_manifest.py" in dockerfile.split("docker_build_final.sh")[-1]

    def test_sealer_is_backfilled_into_contexts(self):
        """_fill_missing_scripts must supply it or the COPY fails."""
        src = (Path(__file__).parents[2] / "src" / "datasmith" / "runners" / "synthesize_images.py").read_text()
        assert '"emit_manifest.py"' in src

    def test_declared_commit_is_recorded_in_env_stage(self):
        """The env stage must record the declared commit from the build arg.

        head_commit_drift compares HEAD at seal time against this value; it
        must come from COMMIT_SHA (the build arg), never from `git
        rev-parse`, or the comparison would be tautological and could never
        fire.
        """
        dockerfile = (
            Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr"
        ).read_text()
        env_stage = dockerfile.split("FROM env AS pkg")[0]
        assert "declared_commit" in env_stage

    def test_final_stage_preserves_build_final_sh_exit_status(self):
        """The sealer's ``|| true`` must not swallow docker_build_final.sh's exit code.

        ``docker_build_final.sh ...; python3 /emit_manifest.py || true`` as the last
        statement in the RUN chain makes the RUN unconditionally exit 0 — a failed
        build_final_sh would silently produce a "successfully built" image. The
        script's status must be captured before the sealer runs and re-asserted as
        the RUN's own exit status afterward.
        """
        dockerfile = (
            Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr"
        ).read_text()
        final_stage = dockerfile.split("FROM run AS final")[-1]

        assert "rc=$?" in final_stage
        assert "exit $rc" in final_stage

        # Ordering: script runs -> status captured -> sealer runs (fail-open) -> captured status re-exits.
        script_idx = final_stage.index("docker_build_final.sh /tmp")
        capture_idx = final_stage.index("rc=$?")
        sealer_idx = final_stage.index("python3 /emit_manifest.py")
        exit_idx = final_stage.rindex("exit $rc")
        assert script_idx < capture_idx < sealer_idx < exit_idx
