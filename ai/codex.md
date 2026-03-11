# Codex Adapter

- Read `AGENTS.md` and `ai/contract.json` before implementation.
- Treat deterministic rules from `ai/contract.json` as blocking.
- Keep `@sha3/code-standards` managed files read-only unless the user explicitly requests a standards update.
- Rewrite `README.md` as real package documentation and document public exports and public methods after implementation.
- Run `npm run check` before finalizing and fix any failing rule or type error.
