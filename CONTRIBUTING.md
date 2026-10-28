# How we work

Everything goes through a pull request. Nobody pushes directly to `dev` or `main`; both are protected.

## Branches

| Branch | Purpose |
|---|---|
| `main` | Stable. Updated only at milestones (Nov 13, Nov 20, Dec 4). |
| `dev` | Where all finished work is merged. This is the default branch. |
| `issue-<n>-short-name` | Your work, branched from `dev`. Example: `issue-9-lsh-banding`. For work without an issue: `chore/short-name`. |

## One-time setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pre-commit install
```

`pre-commit install` makes the linter run on every `git commit`. GitHub runs the same checks on every pull request, so a pull request cannot be merged with lint errors even if you skip the hook.

## The loop

1. Pick an issue and assign yourself.
2. Start from the latest `dev`:
   ```bash
   git switch dev && git pull
   git switch -c issue-9-lsh-banding
   ```
3. Work in small commits. Commit messages: a short title in the imperative, for example `Add MinHash signatures`.
4. Before you push, run the same checks as GitHub (pre-commit already does the first two):
   ```bash
   ruff check . --fix
   ruff format .
   pytest
   ```
5. Push and open a pull request **into `dev`**. Fill in the template, and write `Closes #9` so the issue closes when it merges.
6. The **Lint and test** check must be green, and one teammate (not you) must approve.
7. Merge with **Squash and merge**. The branch deletes itself.

## Releasing to `main`

Open a pull request from `dev` into `main` at each milestone. One approval, green check, and merge with **Create a merge commit** (not squash). GitHub may then say `dev` is "behind" `main` by the merge commit; that is normal.

## Reviewing

Review within a day. Check that: the code does what the issue asks; you can follow it; settings and paths are arguments or in `configs/`; no data or large files are added. Small pull requests (one issue, roughly under 400 changed lines) are quicker to review.

## Rules

- Never commit data, large files, or student IDs (UCIDs). The repo is public. A test fails if you do.
- Each script takes its input paths, output paths, and Spark master as arguments (see [docs/dev_setup.md](docs/dev_setup.md)).
- Write type hints on function signatures and a short docstring saying what the function does and why. Tests for the algorithm code (MinHash, A-Priori, metrics) are expected.
- If a pull request is blocked on a teammate, comment on it and mention them.

## Tools we use, and what we left out on purpose

We use `ruff` (linter and formatter), `pre-commit`, `pytest`, and GitHub Actions. We deliberately do not use type checkers, Docker, experiment trackers, or extra linters: they cost more time than they save in a four-week project.
