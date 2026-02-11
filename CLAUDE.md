# Repository Work Rules

## DO NOT MODIFY THIS DIRECTORY DIRECTLY

This is the main repository checkout. All development work MUST be done in a separate git worktree.

## Worktree Rules

1. **Never commit to master or this directory** - This checkout should remain clean.

2. **Each task gets its own worktree** - Before starting work, create or use a dedicated worktree:
   ```bash
   git worktree add ../streamsplit-<task-name> -b <branch-name>
   ```

3. **Existing worktrees** - Check for existing worktrees before creating new ones:
   ```bash
   git worktree list
   ```

4. **Use absolute paths** - Always use the full path to your worktree (e.g., `/Users/tamir/streamsplit-two-zone`) for all file operations and git commands.

5. **Current active worktree** - If continuing existing work, check which worktree is relevant to the task and work there.

6. **Always rebase onto master before starting work** - Before making any changes or applying any edits to a worktree branch, run:
   ```bash
   cd <worktree-path> && git fetch origin && git rebase master
   ```
   If the branch has uncommitted changes, stash them first. If the branch has commits that conflict with master, resolve the rebase BEFORE doing any new work. Never start applying changes to a branch that is behind master — this causes merge conflicts later that waste time and risk losing work.

7. **Clean up after merging** - After a branch is merged to master, immediately delete the worktree and branch:
   ```bash
   git worktree remove /Users/tamir/streamsplit-<task-name>
   git branch -d <branch-name>
   ```
   Do not leave stale worktrees or merged branches lying around.

## Example Workflow

```bash
# List existing worktrees
git worktree list

# Create a new worktree for a feature
git worktree add ../streamsplit-my-feature -b my-feature

# All work happens in the worktree
cd ../streamsplit-my-feature
# ... make changes, run tests, commit ...

# After merging to master, clean up
git worktree remove ../streamsplit-my-feature
git branch -d my-feature
```

## Why This Matters

- Keeps the main checkout clean for reference
- Allows parallel work on multiple features
- Prevents accidental commits to wrong branches
- Each agent/task is isolated from others

## Git Safety

Before running `git checkout <file>`, `git restore <file>`, or `git stash`:
1. Check if the file has uncommitted changes: `git status <file>`
2. If it does, backup first: `cp <file> <file>.backup`

Never assume uncommitted changes can be "restored" from git - they can't.

## Performance Regression Testing

Use `cmd/bench` to check for performance regressions. Run benchmarks before and after changes to compare.

### MPHF Mode (primary test)

Test the core MPHF build performance with 10M keys:

```bash
go run ./cmd/bench -keys 10000000 -payload 0 -algo ptrhash -workers 1
```

This tests the Cuckoo solver without payload/fingerprint overhead. Look at "Build throughput" (M/sec).

### Default Mode (with payload + fingerprint)

Also test the full code path including payload and fingerprint:

```bash
go run ./cmd/bench -keys 10000000 -algo ptrhash -workers 1
```

This uses the default payload (4 bytes) and fingerprint (1 byte).

### Comparing Against Baseline

To compare performance against a baseline commit:

```bash
# Run current version
go run ./cmd/bench -keys 10000000 -payload 0 -algo ptrhash -workers 1

# Stash changes, checkout baseline, run again
git stash
git checkout <baseline-commit>
go run ./cmd/bench -keys 10000000 -payload 0 -algo ptrhash -workers 1

# Restore
git checkout -
git stash pop
```

Run 5+ iterations of each to account for variance. A difference of <5% is typically within noise.

**Always run benchmarks serially.** Never run multiple benchmark processes in parallel — they contend for the same CPU, memory bandwidth, and cache, producing unreliable results.

## Test Failures

- **Never dismiss test failures as "pre-existing" or "unrelated".** Every failure must be investigated and either fixed or explicitly tracked. A test that times out under `-race` is a real problem — fix it (e.g., skip under race detector with a clear reason) rather than ignoring it.
- If a test fails in a package you didn't change, investigate the root cause. It may reveal a real bug, a missing build tag, or an environment issue.

## Plan Execution Rules

- **Every section of an approved plan MUST be executed**, including verification, benchmarks, and profiling. Do not declare work complete until all sections are done.
- **Create a separate task for EACH verification step** (go vet, tests, benchmarks, profiling). A single "run tests" task is not sufficient — each verification type gets its own task.
- **Benchmarks and profiling are mandatory**, not optional. They catch performance regressions that tests alone cannot detect.
- **Before changing function behavior** (e.g., adding a panic, removing a fallback), grep ALL callers including test files. Do not assume a code path is "dead" without verifying no tests exercise it.

## Code Review & Quality Improvement Checklist

Apply this checklist when reviewing code or improving code quality.

**Review methodology:**

- **Systematic, grep-verified audit**: Every exported symbol must be verified with `grep pkg.SymbolName` outside the package. Every struct field must have both write and read sites identified. Every magic number must be traced to a named constant or commented.
- **Multi-pass sweep**: First pass scans for structural issues (dead code, exports, DRY). Second pass checks correctness (overflow, off-by-one, parameter assumptions). Third pass checks comments/naming/tests.
- **Checklist-driven, not scan-and-spot**: Each checklist item applied to every relevant code element, not just the ones that catch the eye.
- **No finding is too small**: Report inconsistencies even if technically correct (e.g., `<< 1` vs `<< 8` for the same composite key pattern).

**Correctness is the top priority.** Every review must actively verify that code produces correct results for all valid inputs — not just the common case. Look for untested parameter combinations, optimized paths that diverge from the generic path, and hardcoded assumptions about configurable values. A bug that only manifests in uncommon configurations is still a bug.

### 1. Comments & Documentation Accuracy

- [ ] **Redundant naming**: Does the comment repeat the package/type/function name unnecessarily?
- [ ] **Stale references**: Do comments reference function/method/type names that have since been renamed or deleted?
- [ ] **Orphaned section headers**: Are there section-separator comments for code that was removed, leaving an empty section?
- [ ] **Wrong types in comments**: Do comments describe the correct types?
- [ ] **Misleading descriptions**: Does the comment accurately describe what this function does, not what a different function does?
- [ ] **Stale casing after visibility changes**: After exporting/unexporting, do comments still reference the old casing?
- [ ] **Mismatched error names in tests**: Do test assertion messages name the correct error?
- [ ] **Non-obvious guards explained**: Is there a comment explaining why a correctness guard exists when the reason isn't self-evident?
- [ ] **Clever techniques documented**: Are non-standard algorithmic tricks explained?
- [ ] **Rationale for constraints**: Are arbitrary-looking constraints explained?
- [ ] **Shared-but-independent state**: When two arrays share a generation counter but track different things, is that relationship documented?
- [ ] **Algorithm/constant attribution**: Are well-known constants cited with their source?

### 2. Magic Numbers

- [ ] **Idiomatic alternatives**: Can bit manipulation be replaced with standard library constants?
- [ ] **Statistical context**: Do multipliers of statistical parameters have comments explaining what percentile they target?
- [ ] **Minimum floors**: Are minimum values explained?
- [ ] **Fractions of totals**: Are fractional allocations explained?
- [ ] **Safety margins**: Are additive safety margins justified?
- [ ] **Repeated values**: Is the same magic number used in multiple places? Extract to a named constant to keep them in sync.

### 3. Dead Code & Leftovers

- [ ] **Test-only symbols in production files**: Are there any functions, types, constants, or variables (exported or unexported) that live in non-test files but are only referenced by test code? Move into a `_test.go` file or delete if the test should use the real production function instead.
- [ ] **Orphaned artifacts**: Are there functions, types, or test helpers that are artifacts of a previous design?
- [ ] **Tests testing dead code**: Are there tests that only exercise dead/trivial helpers and provide no real coverage?
- [ ] **Assertion-free tests**: Are there tests that call functions and log results but never assert? They exercise code paths but can't catch regressions.
- [ ] **Stray blank lines**: Extra blank lines between or inside functions that don't serve a grouping purpose.

### 4. DRY / Duplication

- [ ] **Identical implementations on different types**: Do two types have methods with the same body? Extract to a shared function.
- [ ] **Same calculation in multiple places**: Is the same formula copy-pasted? Extract to a single function.
- [ ] **Same constant value in multiple call sites**: Should it be a named constant?
- [ ] **Inconsistent patterns in tests**: Is the same thing done differently in different test files?

### 5. Naming

- [ ] **Variables serving double duty**: Does a variable name describe only one of its uses?
- [ ] **Test names matching current code**: Do test function names still match the types/functions they test after refactoring?
- [ ] **Variable shadowing**: Does a local variable shadow a function, parameter, or outer variable with the same name?
- [ ] **Ambiguous numeric suffixes**: Does a function name end with a number that could be misread? E.g., `WriteEntry5` — is it entry #5 or a 5-byte entry? Prefer a parameter (`WriteEntry(…, 5, …)`) or explicit units (`WriteEntry5B`) over bare numeric suffixes.

### 6. Abstraction & Structure

- [ ] **Unnecessary wrapper types**: Is there a type that wraps another 1:1, adding only a few fields and forwarding methods? Merge them.
- [ ] **Trivial delegation pairs**: Are there public methods that just call a private method with no additional logic? Inline the private method.
- [ ] **File length**: Are files over ~500-600 lines? Can they be split along natural seams?
- [ ] **Split criteria**: Orchestration/entry points in one file. Self-contained algorithmic routines in another. Independent data structures in a third.

### 7. Visibility / Export Surface

- [ ] **Over-exported internal symbols**: Are symbols exported that are only used within the package? Unexport them.
- [ ] **Audit each export**: For every exported symbol, verify at least one caller exists outside the package. If not, unexport.
- [ ] **Keep exported**: Symbols that implement interfaces consumed by other packages, or are used in external test files.

### 8. Idiomatic Go

- [ ] **`clear()` over manual zero loops** (Go 1.21+)
- [ ] **`math.MaxInt`** over bit manipulation
- [ ] **`t.Logf`** over `fmt.Printf` in tests
- [ ] **Unexport** what isn't part of the API contract

### 9. Correctness

- [ ] **Full parameter space coverage**: Does every valid combination of configuration parameters have a test? Code that only works for the default/common configuration is buggy — if a parameter is configurable, all valid values must produce correct results.
- [ ] **Optimized path parity**: When specialized/optimized code paths exist alongside a generic path, do tests verify they produce identical results? A parity check (specialized output == generic output, byte-for-byte) catches bugs that value-level checks alone might miss.
- [ ] **Hardcoded parameter assumptions**: Does the code hardcode a value that should be derived from a configuration parameter? E.g., `<< 8` assuming a 1-byte field when the field size is configurable. These bugs are silent for the default configuration and only surface for other valid inputs.
- [ ] **Integer overflow / wrap-around**: For any counter that increments without bound, analyze: what type is it? When does it wrap? What happens at wrap?
- [ ] **Probability analysis**: How many increments per unit of work? How many units before wrap? What's the probability of a stale match?
- [ ] **Consequence analysis**: Is a false match benign or catastrophic?
- [ ] **Add guards for negligible probabilities**: If a guard is cheap, add it to make the code provably correct rather than probabilistically correct.

### 10. Coupling, Cohesion & Responsibility

**Cohesion**

- [ ] **Artificial type separation**: Is there a type that only exists as a thin wrapper around another it's always 1:1 with? Merge them.
- [ ] **Scattered knowledge**: Is the same domain concept encoded independently in multiple places? Extract to a single source of truth.
- [ ] **Function placement**: Does the function live near the data it operates on, or is it stranded far from its receiver type and the state it reads?
- [ ] **File cohesion**: Can you describe what a file is "about" in one phrase? If not, it may be doing too much.

**Coupling**

- [ ] **Minimal export surface**: Every exported symbol is a coupling point. Does each export have a consumer outside the package?
- [ ] **Shared mutable state**: When two subsystems share state, is the coupling documented and are the invariants clear? Could they be decoupled?
- [ ] **Unnecessary indirection**: Does calling function A always call function B with no additional logic? Inline it.
- [ ] **Always-paired calls**: Are two functions always called together in sequence (A then B, never independently)? Combine them into a single function.
- [ ] **Interface/contract width**: Are types exposing methods/fields that callers don't need?

**Single Responsibility**

- [ ] **Functions doing two things**: Does a function both compute something and mutate unrelated state? Can those be separated?
- [ ] **Variables serving multiple roles**: Does a variable change meaning mid-function?
- [ ] **Types with orthogonal concerns**: Does a type mix algorithm state with buffer management or configuration? If the concerns change independently, consider separation — but only if the separation is real, not artificial.

**Dependency Direction**

- [ ] **One-way dependencies**: Do files/types form a DAG, or are there circular references?
- [ ] **Stable dependencies**: Do volatile components depend on stable ones, or the reverse?

### 11. Efficiency & Standard Library Usage

- [ ] **Reimplemented standard library or well-established third-party library**: Is there hand-written code that duplicates something available in the standard library or a widely-used, well-maintained third-party package?
- [ ] **Suboptimal data structures**: Is a slice used where a map would give O(1) lookup? Is a map used where a slice with index access would avoid hashing overhead? Does the access pattern match the data structure?
- [ ] **Redundant allocations**: Are slices or maps allocated inside loops when they could be allocated once and reused or reset with `clear()`? Are temporary buffers created per-call when a pre-allocated field on the struct would suffice?
- [ ] **Unnecessary conversions**: Are values converted back and forth between types when the computation could stay in one type?
- [ ] **Quadratic patterns hiding in loops**: Is there an O(n) algorithm available where the code uses O(n²)?
- [ ] **Oversized allocations**: Are buffers pre-allocated for worst-case when a right-sized allocation based on actual input would be significantly smaller?
- [ ] **String building**: Is string concatenation in a loop used where `strings.Builder` or `fmt.Sprintf` would be more efficient?
- [ ] **Available in newer Go versions**: Has a recent Go release added a builtin or standard library function that replaces hand-written code?

### 12. What NOT to Change

- [ ] **Intentional hot-path duplication**: Unrolled/specialized functions that eliminate branches. The duplication is the optimization.
- [ ] **Unsafe operations on hot paths**: If properly bounds-checked, performance-critical, and internal-only, leave them.
- [ ] **Missing validation on query hot paths**: If correctness is ensured at a higher level, don't add per-query validation that would hurt throughput.
- [ ] **Performance-critical code patterns**: Don't refactor for readability if it would regress performance. Document the intent instead.
