---
name: fix-tests
description: Diagnose and fix failing tests in this .NET/C# repository when the user reports regressions, assertion mismatches, or SQL behavior drift between the mock and the real provider.
---

# Fix Tests

## Overview
Diagnose failing tests, decide whether the test or the implementation is wrong, and fix the real defect without losing coverage.

## Workflow
1. Reproduce the failure from the reported output or artifact.
2. Decide whether the test expectation or the implementation is incorrect.
3. Use the real database/provider behavior as the source of truth when SQL behavior is involved.
4. Fix the expectation when the mock already matches the real provider.
5. Fix the implementation when the mock diverges from the real provider.
6. Prefer shared helper fixes when the same pattern repeats across providers.

## Guardrails
- Prefer the smallest change that restores the intended behavior.
- Do not assume production code is wrong just because a test fails.
- Do not assume the test is wrong just because the implementation differs.
- Preserve coverage and provider-specific differences.
