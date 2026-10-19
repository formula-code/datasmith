# Code design

These rules decide where code goes and how much code a change adds. They come from code review. `docs/PR_STYLE.md` gives the rules for PR text.

## Put code where its concern lives

- Group code by domain, not by workflow step. A package called after a pipeline stage (for example `verify/`) with unrelated modules inside it is not a design.
- Extend the package that already has the concern before you add a new one:
  - images and containers go in `docker/`;
  - task rendering and `task.toml` go in `harbor_adapter/`;
  - LLM calls and prompt extraction go in `agents/`;
  - pipeline steps go in `runners/`;
  - pushing data to a registry or dataset goes in `publish/`.
- Add a top-level package only for a new domain with several modules, for example `measurement/` for timing, the noise rule and the measure gate.
- Keep code that runs in different places apart:
  - code that runs inside a task container goes with the templates;
  - long-running host services go in their own module;
  - the library holds the logic;
  - the CLI only parses arguments and calls the library.

## Change the source, not the output

- Fix behavior where it starts. If new tasks need a different prompt, render it that way: add a runner class (for example `RenderShortProblemsRunner`) and make it the default. Do not add a tool that edits rendered files after the fact.
- Extend the existing patterns (a runner subclass, a publisher next to the other publishers) instead of a parallel copy.
- Before you add a step, check whether the existing pipeline already does it. For example, publishing is a step of `fc-data`.

## Write less code

- Apply the ponytail rules (https://github.com/DietrichGebert/ponytail/blob/main/skills/ponytail/SKILL.md) to every component: the smallest change that fully solves the task, deletion before addition, no option or layer that nobody asked for.
- Before you write a helper, check in this order:
  1. this repository;
  2. the standard library;
  3. an installed dependency;
  4. a maintained external package.
- Generic helpers (JSONL files, shell calls, free disk space) are not main modules. Put them in `utils/` or in the module that uses them.
- One-off scripts (reports, counts, operational runs) are not library code. Put them in `scripts/`, or in the repository that uses them, or delete them after use.
- Archive old code and documents when nothing uses them. Do not keep them for history; git keeps the history.

## Make it work on every host

- A service or script must not depend on one machine: no fixed home paths, host names or local mounts. It must run the same way on an AWS host as on the local machine.
- Settings come from arguments or environment variables with clear names, documented in the module that uses them.

## Review checklist

- Does each new module belong to the package that holds its concern?
- Is any new module a utility, a one-off script or a duplicate of something that exists?
- Does the change fix the source of the behavior, or edit its output?
- Can a smaller change do the same job?
