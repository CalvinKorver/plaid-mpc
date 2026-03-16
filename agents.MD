## Purpose

This `AGENTS.md` file tells AI agents how to work in this repo. Treat it as source of truth for project expectations and conventions.

## Global Rules

- **All plans require unit tests to be written.**
  - If you add or change behavior, you must either:
    - Add new tests that cover the change, or
    - Update existing tests to reflect the new behavior.
  - Only skip tests for pure documentation-only edits (like this file) or when the user has explicitly stated tests are not needed.

## How To Work in This Repo

- **Be proactive**: When the user asks for a feature or fix and the intent is clear, go ahead and implement it rather than only proposing ideas.
- **Assume ownership**: Drive tasks to completion in a single session when possible (including tests, lint fixes, and small refactors needed to keep things clean).
- **Keep changes cohesive**: Group related code and test changes together in the same batch of edits.
- **Prefer small, targeted edits**: Avoid large, sweeping refactors unless the user explicitly asks or there is a clear, contained benefit.

## Testing Expectations

- **When adding new code**:
  - Add unit tests that cover the happy path and at least one important edge case.
  - Prefer fast, deterministic tests over slow or flaky ones.
- **When modifying existing code**:
  - Run and update nearby tests in the same module or package.
  - If you fix a bug, add a regression test that fails before the fix and passes after.

## Code Quality & Style

- **Follow existing patterns** in the codebase for structure, naming, and error handling.
- **Avoid over-commenting**: Only add comments for non-obvious intent, trade-offs, or constraints.
- **Keep functions focused**: Prefer small, composable helpers over large, monolithic functions.
- **Handle errors deliberately**: Propagate or log errors in a consistent way rather than silently swallowing them.

## Collaboration With the User

- **Clarify only when necessary**: If requirements are ambiguous in a way that could materially change the design, ask brief, targeted questions. Otherwise, document assumptions and proceed.
- **Explain at a high level**: Summarize what you changed and why, without dumping large code blocks in the final summary.
- **Respect user preferences**: If the user mentions specific patterns (e.g., functional style, particular testing frameworks, or naming schemes), follow them consistently.

## When Unsure

- Prefer **safe, incremental changes** over speculative refactors.
- Document important assumptions in your response so future agents and humans can understand the context.