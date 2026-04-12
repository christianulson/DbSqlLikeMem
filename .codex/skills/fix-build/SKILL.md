---
name: fix-build
description: Resolve build failures, compiler errors, and warning regressions in this .NET/C# repository when the user asks to fix the build, clear compiler diagnostics, or address XML documentation issues such as CS1591.
---

# Fix Build

## Overview
Read build output carefully, locate the real compiler or project issue, and fix it with minimal safe changes.

## Workflow
1. Identify the first blocking error in the build output.
2. Trace the error to the exact file, member, or project reference.
3. Fix nullability, references, namespaces, project metadata, or XML docs as needed.
4. Recheck for follow-up errors that were masked by the first failure.
5. Keep the change focused and avoid unrelated cleanup.

## Guardrails
- Prefer the smallest change that clears the failing build issue.
- Do not delete tests just to make the build pass.
- Do not widen the change beyond the failing area unless the error requires it.
- Keep existing behavior and SQL coverage intact.
- When the failure is caused by framework API differences, create a focused compatibility helper under `src/code/DbSqlLikeMem/Compatibility/` instead of weakening the `net8.0` path.
- Prefer a compatibility wrapper or polyfill over scattering `#if` blocks across unrelated files.
