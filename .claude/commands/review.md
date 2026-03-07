# Codex Review

Review code changes using the Codex CLI (`codex review`) and report findings.

## Arguments

`$ARGUMENTS` can be:
- Empty (default) — reviews uncommitted changes
- A branch name — reviews diff against that branch (e.g., `main`)
- A commit SHA — reviews that specific commit

## Instructions

### Step 1: Determine review mode

Parse `$ARGUMENTS` to decide which `codex review` mode to use:

- **No arguments or empty:** Use `--uncommitted` to review all staged, unstaged, and untracked changes.
- **Looks like a branch name** (e.g., `main`, `develop`, `feature/foo`): Use `--base $ARGUMENTS`.
- **Looks like a commit SHA** (7+ hex characters): Use `--commit $ARGUMENTS`.

Before running the review, check that there are actually changes to review:
- For `--uncommitted`: run `git status --short` and check there are changes. If clean, tell the user there's nothing to review.
- For `--base`: run `git log --oneline $ARGUMENTS..HEAD` and check there are commits. If none, tell the user the branch is up to date.
- For `--commit`: run `git cat-file -t $ARGUMENTS` to verify the SHA exists.

### Step 2: Run codex review

Run the appropriate command. The review can take 30-120 seconds depending on diff size.

```bash
codex review --uncommitted 2>&1
```

or

```bash
codex review --base <branch> 2>&1
```

or

```bash
codex review --commit <sha> 2>&1
```

Use a timeout of 300000ms (5 minutes) to allow for large reviews.

**Important:** Capture both stdout and stderr (`2>&1`). The structured review output goes to stdout, but progress/metadata goes to stderr.

### Step 3: Report findings

Parse the Codex output and present findings to the user. The output contains:
- Metadata lines (model, workdir, session ID — skip these)
- Plan updates and exec blocks (intermediate steps — skip these)
- The final `codex` block with the actual review findings

Extract and present:
1. **Summary** — the opening paragraph of the review
2. **Findings** — each finding has a priority tag (P0 = critical, P1 = high, P2 = medium, P3 = low), a description, and a file path with line numbers
3. List findings in priority order (P0 first)
4. For each finding, include the file path and line range so the user can navigate directly

If the review found no issues, say so clearly.

### Step 4: Act on findings (optional)

After presenting findings, ask the user if they want you to fix any of the reported issues. If yes, implement the fixes directly.

Do NOT automatically fix anything — wait for the user to decide.
