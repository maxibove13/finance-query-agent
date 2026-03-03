# Safe Commit

Perform pre-commit checks with parallel analysis, then commit and push.

## Instructions

Follow these steps in order:

### Step 1: Parallel Analysis (run ALL THREE subagents simultaneously)

Launch these three subagents IN PARALLEL using the Agent tool:

1. **Diff Review Agent** (subagent_type: "general-purpose")
   - Run `git diff` and `git diff --cached` to get all uncommitted changes
   - Review the diff for:
     - Security vulnerabilities (hardcoded secrets, SQL injection)
     - Accidental debug code (print statements, breakpoint())
     - TODO/FIXME comments that should be addressed
     - Large files that shouldn't be committed
   - Return a summary: PASS/FAIL with list of issues found

2. **Test Runner Agent** (subagent_type: "general-purpose")
   - Run `uv run pytest` to execute the full test suite
   - Return: PASS/FAIL with summary of test results

3. **Lint Agent** (subagent_type: "general-purpose")
   - Run `uv run ruff check . && uv run ruff format --check .`
   - If failures, run `uv run ruff check . --fix && uv run ruff format .` and report what was auto-fixed
   - Return: PASS/FAIL with summary

### Step 2: Review Results

After ALL subagents complete:
- Summarize results from all three analyses
- If ANY critical issues found (security, failing tests), STOP and report to user
- If only style issues that were auto-fixed, stage those fixes and proceed

### Step 3: Commit

Ask the user for a commit message using AskUserQuestion with these options:
- "Generate from changes" - Analyze the diff and generate an appropriate message
- "Let me type it" - User provides custom message

Create the commit with the message.

### Step 4: Push

Push to the current branch:
```bash
git push origin HEAD
```

Report success with:
- Summary of what was committed
- Link to create PR if on a feature branch
