from __future__ import annotations

import pytest

from datasmith.update.cli import parse_args, validate_dates


class TestCLI:
    def test_parses_dates(self):
        args = parse_args(["--start-date", "2024-01-01", "--end-date", "2024-12-31"])
        assert args.start_date == "2024-01-01"
        assert args.end_date == "2024-12-31"

    def test_resume_flag(self):
        args = parse_args(["--start-date", "2024-01-01", "--end-date", "2024-12-31", "--resume"])
        assert args.resume is True

    def test_dry_run_flag(self):
        args = parse_args(["--start-date", "2024-01-01", "--end-date", "2024-12-31", "--dry-run"])
        assert args.dry_run is True

    def test_stage_flag(self):
        args = parse_args(["--start-date", "2024-01-01", "--end-date", "2024-12-31", "--stage", "3"])
        assert args.stage == 3

    def test_invalid_date_format_exits(self):
        args = parse_args(["--start-date", "not-a-date", "--end-date", "2024-12-31"])
        with pytest.raises(SystemExit):
            validate_dates(args)

    def test_missing_required_args(self):
        with pytest.raises(SystemExit):
            parse_args([])
