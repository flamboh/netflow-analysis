# AGENTS.md

Generally speaking, you should browse the codebase to figure out what is going on.

## Task Completion Requirements

- All of `bun format`, `bun lint`, and `bun typecheck` must pass before considering tasks completed. Testing can be narrowed where appropriate based on the diff.
- Never run `bun test`, use `bun run test` for all testing (runs Vitest).

## Project Snapshot

ATLANTIS is a large-scale network telemetry visualization platform.

This repository is WIP. Proposing sweeping changes that improve long-term maintainability is encouraged.

## Core Priorities

1. Performance first.
2. Reliability first.
3. Simple code first.

If a trade-off is required, choose correctness and robustness over short-term convenience.

Avoide complex type assertions, needless casts, and extreme robustness. No "legacy fallbacks" or excessive try catch blocks.

## Maintainability

Long-term maintainability is a core priority. If you add new functionality, first check if there are shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.

## Package Roles

- `tools/netflow-db`: Manages NetFlow database
- `apps/web`: Data visualization dashboard frontend
- `apps/landing`: App landing page for SEO, discoverability, and marketing
- `vendor/*`: Git submodules containing compiled binaries for network analysis
- `scripts`: One-off scripts or migrations
- `plans`: Generated plans
- `docs`: Maintainer/Admin facing documentation

## Svelte

### $effect

Never update state inside of an effect, effects are an escape hatch and should NEVER be used.

- If you need to sync state to an external library, it is often neater to use {@attach ...}
- If you need to run some code in response to user interaction, put the code directly in an event handler or use a function binding as appropriate
- If you need to log values for debugging purposes, use $inspect
- If you need to observe something external to Svelte, use createSubscriber

## References

- Skills: use ~/.agents/skills/find-skills to locate relevant skills wherever possible
