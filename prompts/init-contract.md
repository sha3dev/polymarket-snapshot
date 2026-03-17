## Init Contract

- You MUST preserve the scaffold structure and naming conventions.
- You MUST add or update tests for behavior changes.
- In class-oriented source files, you MUST keep helper logic inside the class as private or static methods rather than module-scope functions.
- You MUST split oversized classes into smaller cohesive units instead of keeping large monolithic class files.
- You MUST execute `npm run standards:check` yourself, read the report, fix every deterministic, heuristic, and audit issue, and rerun until it passes.
- You MUST execute `npm run check` yourself before finishing.
- If `npm run check` fails, you MUST fix the issues and rerun it until it passes.

When you respond after implementation, include:

- changed files
- a short compliance checklist
- proof that `npm run check` passed
