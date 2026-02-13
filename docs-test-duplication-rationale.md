# Test duplication rationale (safety over reduction)

Some test files intentionally repeat similar scenarios across classes/files.

## Why we are keeping them
- They protect against regressions in different wiring paths (for example, class-level fixtures, setup styles, and command/query call shapes).
- They are used as a safety net while SQL compatibility and parser behavior are still evolving per provider.
- Removing them without full mutation/coverage baseline could create blind spots.

## Current policy
- Prefer **keeping** duplicates unless there is clear evidence they are redundant and coverage-equivalent.
- Before deduplicating, require:
  1. Coverage comparison (before/after),
  2. At least one full provider test run,
  3. Explicit reviewer approval.
