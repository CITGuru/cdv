# Agent Rules

## Branches

Name branches `{type}/{name}`. Use a short kebab-case name.

- `feat/` for new features
- `bug/` for bug fixes
- `docs/` for documentation changes only
- `chore/` for tooling, dependencies, config, and other maintenance
- `refactor/` for code changes that do not change behavior

```
feat/case-views
feat/model-registry
bug/alert-dedupe
```

## Commits

Write commit messages as `{type}: {message}`. The type matches the branch type.

- Use present tense, imperative mood: "Add", not "Added" or "Adds".
- Start the message with a capital letter. Do not end it with a period.
- Keep the subject line under 72 characters. Put more detail in the body if needed.
- Do not add `Co-Authored-By` trailers or any other agent attribution to commits.

```
# ✅ GOOD
feat: Add case views page
bug: Fix duplicate alerts on refresh
docs: Add Python SDK quickstart
chore: Upgrade pnpm to v9
refactor: Extract workflow step runner

# ❌ BAD
feat: Added case views page
bug: fixes duplicate alerts.
Update stuff
```

## Pull Requests

- Title the PR in the same format as a commit: `{type}: {message}`.
- Do not add co-author lines, "Generated with" footers, session links, or any other agent attribution to the PR title or body.

## Code Comments

Add a comment only when the code cannot explain itself: a non-obvious constraint, a workaround, or a reason a reader would not guess. Delete all other comments aggressively, including existing ones in code you touch.

Do not write:

- Comments that repeat what the next line does.
- Large comment blocks or banners that describe a file or function at length.
- Comments that explain your change, its history, or why it is correct. That belongs in the commit message or PR.
- Commented-out code.
