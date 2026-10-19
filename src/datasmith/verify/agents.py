"""Harbor agents for measuring: NopTouch appends a newline to each file the oracle patch changes, so LSV times the
oracle's benchmarks on unchanged code (an empty diff selects nothing)."""

from __future__ import annotations

import shlex

from harbor.agents.nop import NopAgent


class NopTouch(NopAgent):
    def __init__(self, *args, files: list[str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._files = list(files or [])

    async def run(self, instruction, environment, context) -> None:
        if not self._files:
            return
        quoted = " ".join(shlex.quote(f) for f in self._files)
        await environment.exec(
            command=f'cd /workspace/repo && for f in {quoted}; do [ -f "$f" ] && printf "\\n" >> "$f"; done; git diff --stat'
        )
