## Refactor Contract

This workflow is a refactor, not a migration-by-copy.

- You MUST analyze the legacy code first and extract only:
  - required behavior
  - preserved public contracts
  - real business rules
  - important edge cases
- You MUST then build a fresh implementation on top of the regenerated scaffold.
- You MUST treat the legacy code as reference material, not as the implementation to transplant.
- You MUST prefer the scaffold-native design even when the legacy project uses different folders, names, layers, wrappers, helper files, or abstractions.
- You MUST preserve only what preservation rules explicitly require.
- You MUST remove unjustified legacy complexity during the refactor.

The following are FORBIDDEN unless an explicit preservation requirement makes them necessary:

- copying legacy files into the new scaffold and making only superficial edits
- reproducing the legacy folder tree, file split, naming, helper layers, wrappers, or abstraction patterns as-is
- porting code module-by-module just because it exists in the source project
- preserving plural feature folders, unnecessary typed errors, helper files, wrapper services, or other structures that violate the active standards

If the legacy code conflicts with the scaffold or standards, the fresh scaffold and regenerated managed files win.
If unsure whether to preserve a legacy structure, default to the simpler standards-compliant scaffold design.

## Requirements

- You MUST preserve only the contracts explicitly marked for preservation.
- You MUST use the snapshot under `.code-standards/refactor-source/latest/` as reference, not as a structure to copy blindly.
- You MUST use the snapshot to understand old behavior, public API, edge cases, and data flow; you MUST NOT mirror its files, layers, or module graph unless preservation requirements force it.
- You MUST treat `.code-standards/refactor-source/latest/` as legacy reference only; you MUST NEVER restore `AGENTS.md`, `ai/*`, `prompts/*`, `.vscode/*`, `biome.json`, `tsconfig*.json`, `package.json`, or lockfiles from that snapshot.
- You MUST treat the freshly regenerated managed files in the project root as authoritative; if checks fail, you MUST fix `src/` and `test/` to satisfy them instead of replacing managed files.
- During refactor work, pre-existing managed-file drift created by the scaffold regeneration is expected and is NOT a blocker by itself.
- You MUST NEVER use `git checkout`, `git restore`, or snapshot copies to roll managed files back to an older contract or toolchain state during refactor work.
- You MUST actively remove unjustified legacy complexity during the refactor instead of preserving it by inertia.
- Before writing final code, you MUST explicitly compare the planned target structure against the active standards and remove any copied legacy shape that is not required.
- In class-oriented source files, you MUST fold helper logic into private or static class methods instead of leaving module-scope helper functions behind.
- You MUST break oversized classes into smaller cohesive units instead of preserving monolithic class files.
- You MUST execute `npm run check` yourself before finishing.
- If `npm run check` fails, you MUST fix the issues and rerun it until it passes.

## Required Execution Order

1. Analyze the legacy source and identify behavior/contracts to preserve.
2. Discard the legacy structure as implementation input.
3. Rebuild the solution in the fresh scaffold so it satisfies the active standards.
4. Run `npm run check`.
5. Fix every failure by changing `src/` and `test/`, not by restoring legacy managed files.

Finish with:

- changed files
- preserved contracts checklist
- intentionally broken or non-preserved items, if any
- proof that `npm run check` passed
