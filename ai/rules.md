# Project Rules

Read this file together with `AGENTS.md` and `ai/contract.json` before making implementation changes.

## Core Rules

- Treat `ai/contract.json` as the machine-readable source of truth.
- Treat `AGENTS.md` as blocking local policy.
- Keep managed files read-only unless the user explicitly requests a standards update.
- Run `npm run check` yourself before finishing and fix any failures before you stop.

## Refactor Rule

- When working from a legacy codebase or refactor snapshot, you MUST analyze the old code and rebuild a fresh implementation that matches the active standards.
- Legacy code MUST be treated as behavior/reference input, not as a structure to copy into the scaffold.
- You MUST NOT preserve legacy folders, file splits, plural feature names, helper layers, wrappers, or typed errors unless they are both standards-compliant and required by preserved contracts.
- If legacy code conflicts with the scaffold, the scaffold MUST win.

## Simplicity

- Choose the simplest correct design for the current requirement.
- Do not add speculative abstractions, helper layers, wrappers, or extension points without immediate need.
- Do not use simplicity as a reason to remove valid responsibility boundaries.

## Compactness

- If a declaration, expression, call, object literal, array literal, import, or constructor call fits on one line within the configured line limit, it MUST stay on one line.
- Prefer fewer line breaks when readability is preserved.
- Do not split code into multiple lines just because it is “safer”; only split when it no longer fits cleanly or readability would suffer.

## Simple Callbacks

- Simple callbacks in `map`, `filter`, `reduce`, `some`, `every`, `find`, and `forEach` MUST use concise arrow functions when the body is a single expression.
- Do not use block-bodied callbacks with explicit `return` for simple expressions.
- If a callback fits on one line, keep it on one line.

## Errors

- Throw plain `Error` by default.
- Use custom error types only when other code must distinguish failure kinds.
- Do not add error hierarchies without a real consumer.

## Type Files

- Keep small or local types close to the code that uses them.
- Create `*.types.ts` only when shared feature types are substantial enough to justify a dedicated file.

## Feature Classes

- Inside `src/<feature>/`, files MUST expose exactly one public class unless the file is `*.types.ts`.
- Do not implement feature modules as exported function collections.

## README

- Rewrite `README.md` as package-quality integration documentation once real behavior exists.
- Document every public export from `src/index.ts`.
- If a public export is a class, document each public method with purpose, return value, and behavior notes.
- Use a structure inspired by high-quality package READMEs: short value proposition, practical examples first, exhaustive API reference after.
- Do not leave scaffold-placeholder API descriptions once implementation is real.

## Active Deterministic Rules

- `single-return`: Functions and methods in src/ must use a single return statement. (verify, confidence: high)
- `async-await-only`: Asynchronous code in src/ must use async/await instead of promise chains. (verify, confidence: high)
- `one-public-class-per-file`: Each source file may expose at most one public class. (verify, confidence: high)
- `feature-class-only`: Files inside src/<feature>/ must expose exactly one public class, except .types.ts files. (verify, confidence: high)
- `class-section-order`: Files that expose a public class must include valid ordered @section markers and omit empty section blocks. (verify, confidence: high)
- `canonical-config-import`: Imports of config.ts must use the config identifier and include the .ts extension. (verify, confidence: high)
- `domain-specific-identifiers`: New identifiers must avoid generic names such as data, obj, tmp, val, thing, helper, utils, and common. (verify, confidence: high)
- `boolean-prefix`: Boolean variables and properties must start with is, has, can, or should. (verify, confidence: high)
- `feature-filename-role`: Feature files must use the feature name plus an explicit role suffix such as .service.ts or .types.ts. (verify, confidence: high)
- `typescript-only`: Implementation and test code must stay in TypeScript files only. (verify, biome, confidence: high)
- `kebab-case-paths`: Source and test paths must use kebab-case names for files and directories unless explicitly reserved. (verify, confidence: high)
- `singular-feature-folders`: Feature folder names under src/ must be singular unless they are reserved structural folders. (verify, confidence: high)
- `test-file-naming`: Tests must live under test/ and use the .test.ts suffix. (verify, confidence: high)
- `module-constant-case`: Module-level constants must use SCREAMING_SNAKE_CASE except for the canonical config export. (verify, confidence: high)
- `local-constant-case`: Local constants must use camelCase names. (verify, confidence: high)
- `config-default-export-name`: src/config.ts must export a default object named config. (verify, confidence: high)
- `no-any`: Explicit any is forbidden in source and tests. (verify, confidence: high)
- `explicit-export-return-types`: Exported functions and public methods of exported classes must declare explicit return types. (verify, confidence: high)
- `type-only-imports`: Imports used only in type positions must use import type. (verify, confidence: high)
- `prefer-types-over-interfaces`: Interfaces are forbidden for local modeling unless they are part of the public contract exported from src/index.ts. (verify, confidence: high)
- `control-flow-braces`: if, else, for, while, and do blocks must always use braces. (verify, confidence: high)
- `concise-simple-callbacks`: Simple callbacks must use concise arrow expressions instead of block bodies with an explicit return. (verify, confidence: high)
- `cross-feature-entrypoint-imports`: Cross-feature imports must go through an explicit feature entrypoint rather than another feature's internal file. (verify, confidence: high)
- `ambiguous-feature-filenames`: Feature code must not use ambiguous file names such as helpers.ts, utils.ts, or common.ts. (verify, confidence: high)
- `typed-error-must-be-used`: Custom error types must be consumed by logic that distinguishes them from plain failures. (verify, confidence: high)
- `no-silent-catch`: Catch blocks must rethrow, transform, or report errors instead of silently swallowing them. (verify, confidence: high)
- `node-test-runner-only`: Test files must use node:test instead of alternative runners. (verify, confidence: high)
- `assert-strict-preferred`: Tests must use node:assert/strict for assertions. (verify, confidence: high)
- `no-ts-ignore-bypass`: TypeScript and Biome ignore directives must not be used to bypass real issues in source or tests. (verify, confidence: high)
- `readme-sections`: README.md must keep the required sections for setup, API, config, troubleshooting, and AI workflow. (verify, confidence: high)
- `readme-config-coverage`: README must document each top-level configuration key exposed from src/config.ts. (verify, confidence: high)
- `required-scripts`: Generated projects must expose the required standards, lint, format, typecheck, test, and check scripts. (verify, confidence: high)
- `standards-check-script`: npm run standards:check must execute code-standards verify. (verify, confidence: high)
- `package-exports-alignment`: Generated projects must stay aligned with @sha3/code-standards biome and tsconfig exports. (verify, confidence: high)

## Active Heuristic Rules

- `compact-single-line-constructs`: Declarations, calls, imports, arrays, and objects that fit on one line should not be spread across multiple lines. (verify, confidence: high)
- `feature-first-layout`: Projects with feature modules must keep domain code under feature folders instead of mixing flat modules at src/ root. (verify, confidence: medium)
- `restricted-shared-boundaries`: src/app and src/shared should exist only when real composition or cross-feature sharing justifies them. (verify, confidence: medium)
- `types-file-justification`: Dedicated .types.ts files should only exist when they contain substantial shared feature types. (verify, confidence: medium)
- `plain-error-default`: Plain Error must be used by default; custom error types require a real control-flow consumer. (verify, confidence: medium)
- `actionable-error-messages`: Error messages should include actionable context rather than empty or generic text. (verify, confidence: medium)
- `test-determinism-guards`: Tests should avoid uncontrolled time, randomness, real network calls, and un-restored process.env mutation. (verify, confidence: medium)
- `readme-no-placeholder-language`: README content must not look like a scaffold placeholder or contain TODO-style filler text. (verify, confidence: medium)
- `readme-runnable-examples`: README must include plausible runnable code or command examples instead of abstract placeholders. (verify, confidence: medium)
- `no-speculative-abstractions`: Factories, options types, wrappers, and helper layers should not exist without a real current consumer or complexity reduction. (verify, confidence: medium)
- `single-responsibility-heuristic`: Long functions and methods should be split when they appear to mix multiple responsibilities. (verify, confidence: medium)

## Active Audit Rules

- `managed-files-read-only`: Managed contract and tooling files must not be edited during normal feature work. (verify, confidence: medium)
- `behavior-change-tests`: Behavior changes must update or add tests, and tests should focus on observable behavior. (verify, confidence: medium)
- `simplicity-audit`: Projects should avoid needless layers, wrappers, and extra files when a smaller direct implementation would suffice. (verify, confidence: medium)
- `comments-policy-audit`: Non-trivial logic should include explicit comments when the profile requires extensive comments. (verify, confidence: medium)
