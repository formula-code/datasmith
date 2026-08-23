"""The prune watcher must relieve disk pressure, not delete cache on a timer.

`docker builder prune` removes the BuildKit cache, which is exactly what makes a
rebuild cheap. The watcher used to fire every 7200s regardless of disk state,
against a Docker filesystem at 8% capacity, while the median build attempt ran
4098s. So it landed inside roughly every second build and threw away the cache
that build had just filled.

These tests pin the corrected trigger: prune when the disk is under pressure,
leave the cache alone otherwise, and prune anyway when the disk state cannot be
determined.
"""

from __future__ import annotations

from unittest.mock import patch

from datasmith.utils import docker_prune


class TestShouldPrune:
    def test_below_threshold_keeps_the_cache(self):
        with (
            patch.object(docker_prune, "_docker_root", return_value="/mnt/sdd2/docker"),
            patch.object(docker_prune, "_used_pct", return_value=12.4),
        ):
            wanted, reason = docker_prune._should_prune()
        assert wanted is False
        assert "keeping build cache" in reason

    def test_at_or_above_threshold_prunes(self):
        with (
            patch.object(docker_prune, "_docker_root", return_value="/mnt/sdd2/docker"),
            patch.object(docker_prune, "_used_pct", return_value=91.0),
        ):
            wanted, reason = docker_prune._should_prune()
        assert wanted is True
        assert "91.0% used" in reason

    def test_exactly_at_threshold_prunes(self):
        with (
            patch.object(docker_prune, "_docker_root", return_value="/x"),
            patch.object(docker_prune, "_used_pct", return_value=docker_prune.DATASMITH_DOCKER_PRUNE_MIN_USED_PCT),
        ):
            wanted, _ = docker_prune._should_prune()
        assert wanted is True

    def test_unknown_docker_root_prunes(self):
        """Fail safe. A stalled pipeline costs more than a cold cache."""
        with patch.object(docker_prune, "_docker_root", return_value=None):
            wanted, reason = docker_prune._should_prune()
        assert wanted is True
        assert "unknown" in reason

    def test_unreadable_usage_prunes(self):
        with (
            patch.object(docker_prune, "_docker_root", return_value="/gone"),
            patch.object(docker_prune, "_used_pct", return_value=None),
        ):
            wanted, reason = docker_prune._should_prune()
        assert wanted is True
        assert "cannot read usage" in reason


class TestRunPrune:
    def test_does_not_shell_out_when_disk_is_fine(self):
        """The regression. A healthy disk must produce no prune command at all."""
        with (
            patch.object(docker_prune.shutil, "which", return_value="/usr/bin/docker"),
            patch.object(docker_prune, "_should_prune", return_value=(False, "plenty of room")),
            patch.object(docker_prune, "_run_prune_cmd") as run_cmd,
        ):
            docker_prune._run_prune()
        run_cmd.assert_not_called()

    def test_prunes_when_the_disk_is_full(self):
        with (
            patch.object(docker_prune.shutil, "which", return_value="/usr/bin/docker"),
            patch.object(docker_prune, "_should_prune", return_value=(True, "94% used")),
            patch.object(docker_prune, "_run_prune_cmd") as run_cmd,
        ):
            docker_prune._run_prune()
        called = [call.args[1] for call in run_cmd.call_args_list]
        assert ["builder", "prune", "-f"] in called
        assert ["image", "prune", "-f"] in called

    def test_force_skips_the_disk_check(self):
        """Between stages, where no build is running, pruning unconditionally is fine."""
        with (
            patch.object(docker_prune.shutil, "which", return_value="/usr/bin/docker"),
            patch.object(docker_prune, "_should_prune") as should,
            patch.object(docker_prune, "_run_prune_cmd") as run_cmd,
        ):
            docker_prune._run_prune(force=True)
        should.assert_not_called()
        assert run_cmd.call_count == 2

    def test_missing_docker_binary_is_not_an_error(self):
        with (
            patch.object(docker_prune.shutil, "which", return_value=None),
            patch.object(docker_prune, "_run_prune_cmd") as run_cmd,
        ):
            docker_prune._run_prune()
        run_cmd.assert_not_called()


class TestWatcher:
    def test_env_opt_out_starts_no_thread(self):
        with (
            patch.dict("os.environ", {"DATASMITH_DISABLE_DOCKER_PRUNE": "1"}),
            patch.object(docker_prune.threading, "Thread") as thread,
        ):
            with docker_prune.builder_prune_watcher():
                pass
        thread.assert_not_called()
