---
name: create-tests
description: Create new tests or expand existing coverage in this .NET/C# repository when the user asks for additional assertions, new provider-specific wrappers, or shared test helpers in TestTools.
---

# Create Tests

## Overview
Create new tests or expand existing ones while preserving coverage, SQL semantics, and provider-specific behavior.

## Workflow
1. Find the smallest scenario that proves the requested behavior.
2. Reuse shared test bases and helpers before duplicating setup.
3. Place reusable setup, seeds, and query helpers in `TestTools` when multiple test projects need them.
4. Keep assertions focused on observable SQL or runtime behavior.
5. Preserve existing coverage when adding or moving tests.

## Guardrails
- Prefer the smallest change that adds the requested coverage.
- Do not remove coverage to make a refactor easier.
- Do not change SQL behavior unless the test exposes a real mismatch.
- Keep provider differences explicit when they matter.
