---
name: large-refactor
description: Break large refactors into safe incremental changes in this repository when the user asks for broad restructuring, shared helper extraction, test/benchmark service centralization, or multi-file cleanup without losing behavior.
---

# Large Refactor

## Overview
Break a broad refactor into small safe slices so the codebase stays stable while the structure improves.

## Workflow
1. Map the current duplication and identify the smallest safe boundary.
2. Extract shared behavior before moving callers.
3. Update consumers in narrow batches.
4. Keep behavior, coverage, and provider differences intact.
5. Remove the old duplicated path only after the new shared path is in place.
6. Note the point where the refactor paused so the next slice can continue cleanly.

## Guardrails
- Prefer the smallest safe boundary that delivers the refactor.
- Do not do a broad rewrite when a smaller extraction will do.
- Do not lose test coverage while moving code.
- Preserve SQL behavior and other external contracts.
