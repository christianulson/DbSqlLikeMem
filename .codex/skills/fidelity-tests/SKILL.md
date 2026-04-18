---
name: fidelity-tests
description: DbSqlLikeMem fidelity-test analysis and maintenance for mock-vs-container parity. Use when fixing or expanding fidelity tests, provider-specific wrappers, Dialect capabilities, SQL result shapes, parameter binding, parser behavior, function semantics, transaction rules, or exception fidelity.
---

# Fidelity Tests

## Purpose

Keep the mock and the real provider aligned on observable behavior.

## Rules

- Use the real provider as the source of truth.
- Prefer fixing the implementation when the mock diverges from the real provider.
- Prefer fixing the test when the expectation is wrong.
- Keep provider-specific decisions in `ProviderSqlDialect` or a provider override.
- Avoid `if (ProviderId == ...)` and `switch (ProviderId)` in test bases when a dialect capability can express the rule.
- Preserve full rowsets for rowset tests; do not reduce them to counts unless the contract is truly scalar.
- Never normalize input data, output values, or reader values inside a fidelity test just to make the mock resemble the container.
- If a value needs to look identical across mock and container, move that behavior to the core application or dialect, not the test.
- Treat unsupported features as explicit fidelity cases:
  - validate the failure contract when the real provider fails
  - do not silently skip unless the capability is intentionally outside the target matrix
- Keep exception behavior aligned:
  - same trigger rule as the real provider
  - richer diagnostic details are allowed when they do not change the contract

## What to check first

1. Parameter shape, type, null handling, size, direction, precision, and provider-specific binding.
2. Result shape, column order, aliases, row order, and `DBNull` normalization.
3. SQL syntax and parser acceptance or rejection.
4. Function semantics, especially date/time, JSON, string, window, join, and aggregate behavior.
5. Transaction behavior, including savepoints, rollback, and release semantics.
6. Exception type, message shape, and failure timing.

## Workflow

1. Identify whether the failure is in the test or the implementation.
2. Compare the mock with the real provider behavior.
3. Move provider rules into the dialect when possible.
4. Keep test bases generic and scenario-specific.
5. Use deep snapshots for rowset fidelity when the contract is relational.
6. Keep changes incremental and avoid unrelated reformatting.

## Validation

- Do not run `dotnet build` or `dotnet test` unless the user explicitly asks.
- Keep XML docs consistent when touching public test wrappers.
- Preserve CRLF line endings in this repository.
