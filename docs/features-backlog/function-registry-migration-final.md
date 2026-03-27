# Function registry migration final state

## Overview

This document records the end state of the function registry migration from the legacy helper-based model to `DbFunctionDef`.
The final architecture keeps two entry points separate: provider-built functions live in the dialect registries, while user-created functions and procedures stay in `SchemaMock`.
Common built-in functions may stay in the main project when they are shared across providers, but the executor must still resolve them through the dialect registration path.
Provider-specific scalar handlers are kept out of the general core evaluator when the dialect project already provides its own evaluator and registration.
SQLite-specific date helpers such as `STRFTIME` are handled by the SQLite registry instead of the shared core evaluator.
Provider-specific aliases are removed from the shared evaluator once the dialect registry owns the mapping directly.
MySQL date helpers such as `MAKEDATE`, `MAKETIME`, `MONTHNAME`, `PERIOD_ADD`, `PERIOD_DIFF`, `QUARTER`, and `SEC_TO_TIME` are now owned by a MySQL-specific evaluator.
The shared date evaluator keeps only the truly common date construction helper.
SQLite date helpers such as `STRFTIME` and `JULIANDAY` are now owned by a SQLite-specific evaluator.
SQLite scalar helpers such as `GLOB`, `PRINTF`, `FORMAT`, and `SQLITE3_MPRINTF` now live in a SQLite-specific evaluator.
SQLite scalar helpers such as `UNISTR`, `UNISTR_QUOTE`, `LIKELY`, `UNLIKELY`, and `LIKELIHOOD` now live in a SQLite-specific evaluator.
SQLite scalar helpers such as `RANDOMBLOB`, `ZEROBLOB`, `SQLITE3_RESULT_ZEROBLOB`, and `TYPEOF` now live in a SQLite-specific evaluator.
SQLite system helpers such as `SQLITE_VERSION`, `SQLITE_SOURCE_ID`, `SQLITE_OFFSET`, `READFILE`, and `LAST_INSERT_ROWID` now live in a SQLite-specific evaluator.
SQLite JSON table helpers such as `JSON_EACH`, `JSON_TREE`, `JSONB_EACH`, `JSONB_TREE`, and `JSONB_EXTRACT` now live in a SQLite-specific evaluator.
The legacy SQLite JSON handler was removed from the shared core evaluator after the provider-specific evaluator took over.
The SQLite evaluator no longer owns MySQL `JSON_APPEND` or `JSON_ARRAY_INSERT`; those helpers belong to the MySQL evaluator.
`SESSION_CONTEXT` now lives in the SQL Server registry instead of the shared system evaluator.
SQL Server metadata helpers such as `GETANSINULL`, `HOST_ID`, `HOST_NAME`, `ISDATE`, `ISJSON`, and `ISNUMERIC` now live in the SQL Server utility evaluator.
SQL Server `SESSION_USER` and `SYSTEM_USER` now live in the SQL Server utility evaluator instead of the shared system evaluator.
MySQL `UUID_SHORT` now lives in a MySQL-specific evaluator instead of the shared system evaluator.
MySQL `IS_UUID` and the `IS_IPV4*` helpers now live in a MySQL-specific evaluator instead of the shared system evaluator.
`USER` now lives in the provider-specific evaluators instead of the shared system evaluator.
`FIELD` now lives in the shared MariaDB/MySQL helper instead of the shared core evaluator.
MySQL `UTC_DATE`, `UTC_TIME`, and `UTC_TIMESTAMP` now live in a MySQL-specific evaluator instead of the shared system evaluator.
`GROUPING` and `GROUPING_ID` now live in a shared grouping evaluator instead of the general system evaluator.
SQL Server `JSON_MODIFY` and `OPENJSON` now live in the SQL Server utility evaluator instead of the shared core evaluator.
MySQL `SUBDATE` now lives in the MySQL date evaluator instead of the shared core evaluator.
MySQL JSON helpers such as `JSON_APPEND`, `JSON_ARRAY_INSERT`, `JSON_STORAGE_SIZE`, and `JSON_OVERLAPS` now live in a MySQL-specific evaluator.
MySQL helper functions such as `JSON_ARRAY` and `JSON_DEPTH` now live in a MySQL-specific evaluator.
MySQL JSON comparison helpers such as `JSON_CONTAINS` and `JSON_OVERLAPS` now live in the MySQL JSON evaluator instead of the shared core evaluator.
MySQL `JSON_SEARCH` path collection now lives in the MySQL JSON evaluator instead of the shared core evaluator.
SQL Server `PATINDEX` now lives in the SQL Server evaluator instead of the shared core evaluator.
MySQL `NAME_CONST` now lives in the MySQL utility evaluator instead of the shared core evaluator.
`JSON_UNQUOTE` now lives in a shared helper instead of the general scalar evaluator.
SQL Server `DIFFERENCE` now lives in the SQL Server utility evaluator instead of the shared core evaluator.
SQL Server `SOUNDEX` now lives in the SQL Server utility evaluator instead of the shared core evaluator.
MySQL `QUOTE` now lives in the MySQL utility evaluator instead of the shared core evaluator.
MySQL `SUBSTRING_INDEX` now lives in the MySQL utility evaluator instead of the shared core evaluator.
MySQL `HEX` and `UNHEX` now live in the MySQL utility evaluator instead of the shared core evaluator.
MySQL `OCT` and `ORD` now live in the MySQL utility evaluator instead of the shared core evaluator.
PostgreSQL `RANDOM` now lives in the PostgreSQL registry instead of the shared core evaluator.
PostgreSQL `TO_ASCII` and the PostgreSQL `inet`/identifier helpers now live in PostgreSQL-specific evaluators instead of the shared core evaluator.
MySQL `BIT_COUNT` now lives in the MySQL utility evaluator instead of the shared core evaluator.
MySQL `LOG2` now lives in the MySQL utility evaluator instead of the shared core evaluator.
MySQL `SHA`, `SHA1`, and `SHA2` now live in the MySQL utility evaluator instead of the shared core evaluator.
Shared `HEX`, `UNHEX`, and `MD5` now live in the shared binary-text evaluator instead of the shared core evaluator.
Shared `ASCII`, `UNICODE`, and `SPACE` now live in the shared text evaluator instead of the shared core evaluator.
Shared `INSTR`, `LPAD`, `REPLACE`, and `REVERSE` now live in the shared text evaluator instead of the shared core evaluator.
Shared `REPEAT` now lives in the shared text evaluator instead of the shared core evaluator.
Shared `LEFT`, `RIGHT`, `RPAD`, `BIT_LENGTH`, `OCTET_LENGTH`, `POSITION`, and `MOD` now live in the shared text/numeric evaluators instead of the shared core evaluator.
Shared `ABS`, `ABSVAL`, and `BIN` now live in the shared numeric evaluator instead of the shared core evaluator.
Shared numeric helpers `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `CEIL`, `CEILING`, `COS`, `COT`, `LN`, `LOG`, and `LOG10` now live in the shared numeric evaluator instead of the shared core evaluator.
Shared `CHAR` and `NCHAR` now live in the shared text evaluator instead of the shared core evaluator.
Shared `LOWER`, `LCASE`, `UPPER`, `UCASE`, `TRIM`, `RTRIM`, `LTRIM`, `LENGTH`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, and `LEN` now live in the shared text evaluator instead of the shared core evaluator.
Shared numeric helpers `DEGREES`, `EXP`, `FLOOR`, `PI`, `POWER`, `POW`, `RADIANS`, `RAND`, `ROUND`, `SIGN`, `SIN`, `SQRT`, and `TAN` now live in the shared numeric evaluator instead of the shared core evaluator.
MySQL `JSON_QUOTE` now lives in the MySQL JSON evaluator instead of the shared core evaluator.
MySQL/MariaDB `JSON_PRETTY` now lives in the MySQL JSON evaluator instead of the shared core evaluator.
MySQL JSON mutation and search helpers now live in the MySQL JSON evaluator instead of the shared core evaluator.
MySQL `JSON_VALID`, `JSON_TYPE`, and `JSON_LENGTH` now live in the MySQL JSON evaluator instead of the shared core evaluator.
`JSON_EXTRACT`, `JSON_QUERY`, and `JSON_VALUE` now live in a shared JSON extraction helper instead of the general scalar evaluator.
`TryParseJsonElement` and `BuildJsonArray` now live in a shared JSON helper instead of the general scalar evaluator.
`CloneJsonNode` and `StripJsonNullProperties` now live in a shared JSON helper instead of the general scalar evaluator.
`TryParseJsonNode` and the shared JSON path mutation helpers now live in a shared JSON helper instead of the general scalar evaluator.
`TryParseJsonPathTokens` and `TryParseSqlServerJsonModifyPath` now live in a shared JSON path helper instead of the general scalar evaluator.
MySQL `TIME_FORMAT`, `TIME_TO_SEC`, `TIMEDIFF`, `TO_DAYS`, `TO_SECONDS`, `TRUNCATE`, `UNIX_TIMESTAMP`, `WEEK`, `WEEKDAY`, `WEEKOFYEAR`, and `YEARWEEK` now live in a MySQL-specific evaluator instead of the shared core evaluator.
MySQL `LAST_DAY` now lives in a MySQL-specific evaluator instead of the shared core evaluator.
SQL Server `EOMONTH` now lives in the SQL Server compatibility path instead of the shared core evaluator.
SQL Server `DATENAME` and `DATEPART` now live in a SQL Server-specific temporal accessor evaluator instead of the shared temporal accessor evaluator.
SQLite `UNIXEPOCH` now lives in a SQLite-specific evaluator instead of the shared core evaluator.

## Completed

- `DbFunctionDef` is now the single contract used by the function registry flow.
- Legacy compatibility DTOs were removed from the normal runtime path.
- Provider registries were migrated to the new invocation style and direct definition factories.
- `SqlFunctionBodyFactory` was removed after losing all consumers.
- Dead public factories `DbFunctionDef.CreateAggregate(...)` and `DbFunctionDef.CreateWindow(...)` were removed.
- The dead private `AddScalarFunctionsCore` helper was removed from `SqlDialectScalarFunctionRegistryExtensions`.
- The dead private `CreateScalarFunctionDefinition` and `WithScalarFunctionExecutor` helpers were removed from `SqlDialectScalarFunctionRegistryExtensions`.
- Conditional procedure helpers `AddProcedureIf` and `AddProceduresIf` were removed from the dialect layer.
- SQLite-specific scalar helpers `GLOB`, `PRINTF`, `FORMAT`, and `SQLITE3_MPRINTF` were moved to a SQLite-specific evaluator.
- SQLite-specific scalar helpers `UNISTR`, `UNISTR_QUOTE`, `LIKELY`, `UNLIKELY`, and `LIKELIHOOD` were moved to a SQLite-specific evaluator.
- SQLite-specific scalar helpers `RANDOMBLOB`, `ZEROBLOB`, `SQLITE3_RESULT_ZEROBLOB`, and `TYPEOF` were moved to a SQLite-specific evaluator.
- SQLite-specific system helpers were moved to a SQLite-specific evaluator.
- SQLite-specific JSON table helpers were moved to a SQLite-specific evaluator.
- The legacy SQLite JSON handler was removed from the shared core evaluator.
- The SQLite evaluator no longer owns MySQL `JSON_APPEND` or `JSON_ARRAY_INSERT`.
- `SESSION_CONTEXT` was moved to the SQL Server registry.
- SQL Server metadata helpers were moved to the SQL Server utility evaluator.
- SQL Server `SESSION_USER` and `SYSTEM_USER` were moved to the SQL Server utility evaluator.
- MySQL `UUID_SHORT` was moved to a MySQL-specific evaluator.
- MySQL `IS_UUID` and `IS_IPV4*` were moved to a MySQL-specific evaluator.
- `USER` was moved to the provider-specific evaluators that own it.
- `FIELD` was moved to the shared MariaDB/MySQL helper.
- MySQL `UTC_DATE`, `UTC_TIME`, and `UTC_TIMESTAMP` were moved to a MySQL-specific evaluator.
- `GROUPING` and `GROUPING_ID` were moved to a shared grouping evaluator.
- SQL Server `JSON_MODIFY` and `OPENJSON` were moved to the SQL Server utility evaluator.
- MySQL `SUBDATE` was moved to the MySQL date evaluator.
- MySQL JSON helpers were moved to a MySQL-specific evaluator.
- MySQL helper functions `JSON_ARRAY` and `JSON_DEPTH` were moved to a MySQL-specific evaluator.
- MySQL JSON comparison helpers were moved to the MySQL JSON evaluator.
- MySQL `JSON_SEARCH` path collection was moved to the MySQL JSON evaluator.
- SQL Server `PATINDEX` was moved to the SQL Server evaluator.
- MySQL `NAME_CONST` was moved to the MySQL utility evaluator.
- Shared `LEFT`, `RIGHT`, `RPAD`, `BIT_LENGTH`, `OCTET_LENGTH`, `POSITION`, and `MOD` were moved to the shared text/numeric evaluators.
- Shared `ABS`, `ABSVAL`, and `BIN` were moved to the shared numeric evaluator.
- Shared numeric helpers `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `CEIL`, `CEILING`, `COS`, `COT`, `LN`, `LOG`, and `LOG10` were moved to the shared numeric evaluator.
- Shared `CHAR` and `NCHAR` were moved to the shared text evaluator.
- Shared `LOWER`, `LCASE`, `UPPER`, `UCASE`, `TRIM`, `RTRIM`, `LTRIM`, `LENGTH`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, and `LEN` were moved to the shared text evaluator.
- `LEN`, `LTRIM`, `REVERSE`, and `RTRIM` were moved from the SQL Server/Auto compatibility registry to the shared text evaluator.
- The dead `AstQueryGeneralScalarFunctionEvaluator` wrapper was removed after the date-only routing moved to the dedicated general date evaluator and the handler delegate was split into its own contract file.
- Shared `SUBSTRING`, `SUBSTR`, and `MID` were moved to the shared text evaluator.
- Shared `LOCATE` was moved to the shared text evaluator.
- Shared `TRANSLATE` was moved to the shared text evaluator.
- Shared `LIKE` was moved to the shared text evaluator.
- Shared `GREATEST` and `LEAST` were moved to the shared numeric evaluator.
- `FORMATMESSAGE`/`PRINTF` moved to the shared formatting helper.
- PostgreSQL network helpers and numeric-scale helpers moved to the PostgreSQL evaluators.
- PostgreSQL `TO_ASCII` and the PostgreSQL `inet`/identifier helpers moved to PostgreSQL-specific evaluators.
- `JSON_OBJECT` was extracted into a shared helper and registered by the MySQL, Npgsql, and Auto registries.
- `JSON_EXTRACT`, `JSON_QUERY`, and `JSON_VALUE` were extracted into a shared helper instead of the general scalar evaluator.
- `TryParseJsonElement` and `BuildJsonArray` were extracted into a shared JSON helper instead of the general scalar evaluator.
- `CloneJsonNode` and `StripJsonNullProperties` were consolidated into a shared JSON helper instead of the general scalar evaluator.
- `TryParseJsonNode` and the shared JSON path mutation helpers were extracted into a shared JSON helper instead of the general scalar evaluator.
- `TryParseJsonPathTokens` and `TryParseSqlServerJsonModifyPath` were extracted into a shared JSON path helper instead of the general scalar evaluator.
- MySQL `TIME_FORMAT`, `TIME_TO_SEC`, `TIMEDIFF`, `TO_DAYS`, `TO_SECONDS`, `TRUNCATE`, `UNIX_TIMESTAMP`, `WEEK`, `WEEKDAY`, `WEEKOFYEAR`, and `YEARWEEK` were moved to a MySQL-specific evaluator.
- MySQL `LAST_DAY` was moved to a MySQL-specific evaluator.
- SQL Server `EOMONTH` was moved to the SQL Server compatibility path.
- SQL Server `DATENAME` and `DATEPART` were moved to a SQL Server-specific temporal accessor evaluator.
- SQLite `UNIXEPOCH` was moved to a SQLite-specific evaluator.
- The DB2 date helper was moved out of the general folder into a DB2-specific path.
- MySQL `DATE_ADD` and `DATE_SUB` were moved out of the general temporal evaluator into a MySQL-specific path.
- The remaining MySQL, Npgsql, and SQL Server registry references to the removed general scalar evaluator were redirected to shared helpers or provider-specific evaluators.
- The general date/time evaluator was removed after those provider-specific helpers were extracted.
- The stale shared `STRCMP` registry entry was removed so the MySQL helper owns it again.
- The dead general JSON utility wrapper was removed after `JSON_OBJECT` moved to the shared helper.
- `TO_NUMBER` was extracted into a shared cast helper and removed from the shared core evaluator.
- `JSON_ARRAY` was extracted into a shared helper and the no-op MySQL/Npgsql fallbacks were removed.
- The MySQL/Npgsql general `JSON_ARRAY` wrapper was removed after the shared helper was registered directly.
- The executor no longer hardcodes SQLite system/json fallbacks and now keeps only the shared `JSON_OBJECT` helper.
- `TO_CHAR` was removed from the shared core evaluator and remains owned by the provider registries that register it explicitly.
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
