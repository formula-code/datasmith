# PR style

The PR template (`.github/pull_request_template.md`) gives the sections. This page gives the rules.

## Language

- Use ASD-STE100 Simplified Technical English.
- Use short active sentences in the present tense: 20 words maximum for steps, 25 for other text.
- Write one instruction per sentence. Use simple words. Do not use idioms or needless hyphens.
- A reader must understand the PR in 30 seconds.
- Do not use these words: "contract", "fail loud", "fail closed", "straddle", "owns", "surface", "guard", "load-bearing", "robust", "seamless", "footgun". Say what the code does.
- Do not write host names. Write "training node", "inference node", "this machine" or "the second machine".
- Do not write secrets. Write credentials only as `${VAR}`.

## Sections, in this order

Delete a section if it adds nothing.

1. **Purpose**: 2 to 4 sentences. The problem, then what the PR does.
2. **How it works**: a Mermaid diagram for flows. A `text` file tree for new or changed files. The Mermaid must parse.
3. **Changes**: a numbered list, grouped by area. Each item gives what changed, `Effect:` and `Before:`.
4. **Usage**: a code block with the exact commands or config.
5. **Verification**: the real commands and the real numbers. Never invent numbers.
6. **Notes**: merge order, dependencies on other PRs, what is not done, decisions for the reviewer.

Write math as GitHub math: `$…$` inline and `$$…$$` for display.

The last line is the footer, exactly:

```text
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

## Code in the PR

- Use the minimum number of lines.
- Comments: 1 or 2 lines. Say what the code does and why. No narrative.
- Write one test for each real behavior.
- Do not add co-author or session trailers to commits.

## Example

````markdown
## Purpose

The reward counts a timeout as a failure. A slow task then gets a negative reward. This PR gives a timeout a reward of 0.

## Changes

1. **Timeout reward is 0.**
   Effect: a timed out trial does not push the policy away from the task.
   Before: a timeout got the failure penalty $-1.25$.

## Verification

`pytest tests/test_reward.py`: 14 passed.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
````
