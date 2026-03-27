# Function registry migration final state

## Overview

This document records the end state of the function registry migration from the legacy helper-based model to `DbFunctionDef`.

## Completed

- `DbFunctionDef` is now the single contract used by the function registry flow.
- Legacy compatibility DTOs were removed from the normal runtime path.
- Provider registries were migrated to the new invocation style and direct definition factories.
- `SqlFunctionBodyFactory` was removed after losing all consumers.
- `SqlFunctionBodyParserHelper` now builds user-defined functions through `DbFunctionDef.CreateUserDefined(...)`.
- `SqlCreateFunctionQuery` carries `Definition` instead of the old split fields.
- XML documentation was added to the public types introduced by the migration.

## Behavioral result

- Scalar, aggregate, window, table, and temporal function definitions now share one richer model.
- The parser, execution layer, and provider registries now resolve functions through the same definition shape.
- The migration no longer depends on the legacy `DbScalarFunctionDef` or `DbTableFunctionDef` wrappers.

## Remaining follow-up

- Review any remaining nullability warnings in the broader codebase if they appear in future validation.
- Keep the tracker focused on future function work rather than the completed migration history.

## Notes

- This document is intentionally static and records the final migration outcome.
- The incremental history remains in `docs/features-backlog/function-registry-migration.md`.
