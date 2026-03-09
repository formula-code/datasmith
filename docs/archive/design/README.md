# DataSmith Design Documentation

This directory contains design documents for the core modules in DataSmith. Each design doc captures the **intent** of a functionality and how it is implemented to achieve that intent.

## Purpose

Design docs serve several key goals:

1. **Capture trade-offs** — Document the reasoning behind design decisions
2. **Enable onboarding** — Help new contributors understand the system quickly  
3. **Preserve institutional knowledge** — Record why things work the way they do
4. **Facilitate review** — Allow stakeholders to evaluate designs before implementation

## Document Structure

Each design doc follows a consistent structure inspired by Google's design doc practices:

| Section | Purpose |
|---------|---------|
| **Context & Scope** | Brief overview of the problem space |
| **Goals & Non-Goals** | What we're trying to achieve (and explicitly not) |
| **Design** | The actual implementation approach with trade-offs |
| **Data Flow** | How data moves through the system |
| **Alternatives Considered** | Other approaches and why they weren't chosen |
| **Cross-Cutting Concerns** | Error handling, performance, observability |

## Domain Index

| Document | Component | Implementation | Description |
|----------|-----------|----------------|-------------|
| [candidate_commit_discovery.md](candidate_commit_discovery.md) | Candidate Discovery | `collect_and_filter_commits.py` | Strategy for identifying potential benchmark candidates from repo history |

## Writing Guidelines

- **Name by Intent** — Document titles and filenames should reflect *what* the system does (e.g., "Candidate Discovery"), not the script name (e.g., `collect_commits.py`).
- **Decouple from Implementation** — The design doc should remain valid even if the underlying script is refactored or renamed.
- Keep paragraphs short — each should compress to a single idea
- Focus on **trade-offs**, not just implementation details
- Include diagrams where they clarify data flow or architecture
- Anticipate and preemptively address reader objections
- Target length: 3-10 pages depending on complexity

## Related Resources

- [README.md](../../README.md) — Project overview and pipeline documentation
- [AGENTS.md](../../AGENTS.md) — Guidelines for AI agents working on this codebase
