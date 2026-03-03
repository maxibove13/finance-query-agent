# Resolve PR

Automate resolving the current PR: merge conflicts, CI failures, and review findings.

## Step 0 — Setup

1. Run `gh pr view --json number,headRefName,baseRefName` to get the PR number, head branch, and base branch.
2. If not on a PR branch, stop and tell the user.
3. Set limits:
   - `MAX_TOTAL_ITERATIONS = 10` — global cap on fix-push-wait cycles.
   - `MAX_REVIEW_ITERATIONS = 3` — AI review is non-deterministic; best-effort only.
4. Initialize counters: `total_iterations = 0`, `review_iterations = 0`.

## Step 1 — Resolve merge conflicts with main

1. Run `git fetch origin main`.
2. Run `git merge origin/main`.
3. If there are merge conflicts:
   - Read each conflicted file and resolve the conflict intelligently based on the intent of both sides.
   - After resolving all conflicts, stage the files and commit: `git commit -m "chore: resolve merge conflicts with main"`.
4. If the merge is clean, continue.

## Step 2 — Run local checks before pushing

Run these locally to catch issues before wasting CI minutes:

1. **Lint:** Run `uv run ruff check . --fix && uv run ruff format .`
2. **Type check:** Run `uv run mypy src/`
3. **Tests:** Run `uv run pytest`
4. If any fail, fix the issues, stage changes, and repeat Step 2.
5. If fixes require changing **more than 5 files not already in the PR diff**, STOP and report:
   > "Scope creep detected: fixing this issue requires changes to X files outside the PR diff. Please review manually."

## Step 3 — Commit and push

1. If there are staged or unstaged changes from fixes:
   - Stage relevant files (prefer specific files over `git add .`).
   - Commit with a descriptive message: `git commit -m "fix: <description of what was fixed>"`.
2. Push: `git push`.

## Step 4 — Wait for CI to start and complete

1. Wait 30 seconds for GitHub Actions to register the new push.
2. Poll CI status using: `gh pr checks --watch --fail-fast` (timeout after 15 minutes).
3. If no checks appear within 90 seconds of the push:
   - Push an empty commit: `git commit --allow-empty -m "chore: trigger CI" && git push`
   - Resume polling.

## Step 5 — Analyze CI results

Run `gh pr checks` and categorize the results:

- **All checks pass** → Go to **Step 8** (success).
- **Deterministic failures** (lint, test, type-check) → Go to **Step 6**.
- **Review failures** → Go to **Step 7**.
- **Infrastructure failures** (network timeout, runner unavailable) → Report and stop.

If multiple categories fail, handle deterministic failures first (Step 6), then review (Step 7).

## Step 6 — Fix deterministic failures

1. Read the failing check's logs: `gh run view <run-id> --log-failed`.
2. Identify the root cause from the error output.
3. Apply the fix. Check the scope rule: if >5 files outside the PR diff need changes, bail out (Step 9).
4. Increment `total_iterations`. If `total_iterations >= MAX_TOTAL_ITERATIONS`, go to **Step 9**.
5. Go back to **Step 2** (local validation before pushing).

## Step 7 — Fix review findings (best-effort)

1. Read the review comments on the PR: `gh pr view --comments`.
2. Parse the findings.
3. For each finding:
   - If it's a legitimate issue in code **changed by this PR**, fix it.
   - If it's about pre-existing code not changed in this PR, ignore it.
4. Increment `review_iterations` and `total_iterations`.
5. If `review_iterations >= MAX_REVIEW_ITERATIONS`:
   - Report: "Review iteration limit reached. Remaining findings may be false positives or pre-existing issues."
   - Go to **Step 8** (treat as success).
6. If `total_iterations >= MAX_TOTAL_ITERATIONS`, go to **Step 9**.
7. Go back to **Step 2**.

## Step 8 — Success report

```
PR #<number> is ready
- Total fix iterations: <count>
- Review iterations: <count>
- Checks status: all passing (or: review best-effort limit reached)
```

## Step 9 — Bail out

```
PR #<number> — iteration limit reached (<total_iterations>/<MAX_TOTAL_ITERATIONS>)
- Fixed: <list of issues fixed>
- Remaining: <list of still-failing checks>
- Action needed: <specific guidance>
```
