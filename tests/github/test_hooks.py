"""Tests for datasmith.github.hooks — HookRegistry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.github.hooks import HookRegistry


@pytest.fixture(autouse=True)
def _clean_hooks() -> None:
    """Ensure each test starts with a clean hook registry."""
    HookRegistry.clear()


class TestHookRegistry:
    def test_register_and_call_hook(self) -> None:
        """Register a hook without caching and call it."""

        def my_hook(x: int, y: int) -> int:
            return x + y

        HookRegistry.register("adder", my_hook, cached=False)
        assert HookRegistry.has("adder")
        result = HookRegistry.call("adder", 3, 4)
        assert result == 7

    def test_hook_listed_after_registration(self) -> None:
        def noop() -> None:
            pass

        HookRegistry.register("noop_hook", noop, cached=False)
        assert "noop_hook" in HookRegistry.list_hooks()

    @patch("datasmith.github.hooks.supabase_cached")
    def test_hook_auto_cached(self, mock_cached: MagicMock) -> None:
        """When cached=True (default), the function is wrapped with supabase_cached."""
        original_fn = MagicMock(return_value="result")
        # Make supabase_cached return a wrapper that just calls through
        mock_cached.return_value = original_fn

        HookRegistry.register("cached_hook", original_fn, cached=True)

        # supabase_cached should have been called with the function
        mock_cached.assert_called_once_with(original_fn)

        # The hook should be the cached version
        assert HookRegistry.has("cached_hook")

    def test_unregistered_hook_raises_attributeerror(self) -> None:
        with pytest.raises(AttributeError, match="No hook registered: nonexistent"):
            HookRegistry.get("nonexistent")

    def test_unregistered_hook_call_raises(self) -> None:
        with pytest.raises(AttributeError, match="No hook registered"):
            HookRegistry.call("missing_hook")

    def test_has_returns_false_for_missing(self) -> None:
        assert not HookRegistry.has("ghost")

    def test_clear_removes_all_hooks(self) -> None:
        HookRegistry.register("h1", lambda: 1, cached=False)
        HookRegistry.register("h2", lambda: 2, cached=False)
        assert len(HookRegistry.list_hooks()) == 2
        HookRegistry.clear()
        assert len(HookRegistry.list_hooks()) == 0

    def test_register_overwrites_existing(self) -> None:
        HookRegistry.register("dup", lambda: "first", cached=False)
        HookRegistry.register("dup", lambda: "second", cached=False)
        assert HookRegistry.call("dup") == "second"
