# Installed Agent Abstraction

An **installed agent** is a CLI coding agent installed on the host machine that
can execute prompts non-interactively, auto-approve tool calls, and return
structured output.

## Supported agents

| Agent | CLI binary | Install |
|-------|-----------|---------|
| Claude Code | `claude` | `npm install -g @anthropic-ai/claude-code` |
| Codex | `codex` | `npm install -g @openai/codex` |
| Gemini CLI | `gemini` | `npm install -g @anthropic-ai/gemini-cli` |
| Qwen Code | `qwen` | `npm install -g @qwen-code/qwen-code` |

## Interface contract

Every `InstalledAgent` implementation must satisfy these requirements:

1. **Non-interactive execution** — run a prompt, return when done
2. **Auto-approve all tool calls** — no human-in-the-loop
3. **JSON/structured output** — parseable stdout with agent messages and file changes
4. **Working directory** — operate in a specified directory (via subprocess `cwd=`)
5. **Ephemeral sessions** — don't persist state across runs
6. **Shell + file editing** — can run bash and edit files in the workspace
7. **External timeout** — can be killed via subprocess timeout

## Auto-detection

`get_agent()` tries agents in preference order (default: `claude → codex → gemini`)
and returns the first one whose CLI binary is on `PATH`:

```python
from datasmith.agents.installed import get_agent

agent = get_agent()                          # auto-detect
agent = get_agent(preference=["codex"])      # force codex
result = agent.exec("Fix the build", timeout=600, workdir="/tmp/workspace")
```

## Adding a new agent

1. Create `src/datasmith/agents/installed/<name>.py`
2. Subclass `InstalledAgent` and implement `name()`, `is_available()`, `exec()`
3. Add a `_parse_<name>_stdout()` function to normalise CLI output
4. Register the class in `base.py`'s `get_agent()` registry dict
5. Re-export from `__init__.py`

## Output parsing

Each agent's CLI emits a different JSON schema. The `_parse_*_stdout()` function
for each agent normalises the output into `(output_lines, files_changed)` which
is then wrapped in an `AgentResult`.
