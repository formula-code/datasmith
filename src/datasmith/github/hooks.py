"""Hook registry for pluggable GitHub data enrichment functions."""

from __future__ import annotations

from typing import Any, Callable, ClassVar

from datasmith.utils import get_logger, supabase_cached

logger = get_logger("github.hooks")


class HookRegistry:
    """A simple registry for named hooks, optionally cached via Supabase."""

    _hooks: ClassVar[dict[str, Callable[..., Any]]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[..., Any], cached: bool = True) -> None:
        """Register a hook function under *name*, optionally wrapping it with caching."""
        if cached:
            func = supabase_cached(func)
        cls._hooks[name] = func
        logger.info("Registered hook: %s (cached=%s)", name, cached)

    @classmethod
    def get(cls, name: str) -> Callable[..., Any]:
        """Retrieve a registered hook by name, raising AttributeError if missing."""
        if name not in cls._hooks:
            raise AttributeError(f"No hook registered: {name}")
        return cls._hooks[name]

    @classmethod
    def has(cls, name: str) -> bool:
        """Check whether a hook with the given name exists."""
        return name in cls._hooks

    @classmethod
    def call(cls, name: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke a registered hook by name with the given arguments."""
        return cls.get(name)(*args, **kwargs)

    @classmethod
    def list_hooks(cls) -> list[str]:
        """Return the names of all registered hooks."""
        return list(cls._hooks.keys())

    @classmethod
    def clear(cls) -> None:
        """Remove all registered hooks (primarily for testing)."""
        cls._hooks = {}
