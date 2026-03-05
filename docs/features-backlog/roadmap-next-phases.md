DbSqlLikeMem — Implementation Roadmap (LLM/Codex Ready)
Context

DbSqlLikeMem is an in-memory SQL execution engine for .NET unit tests that emulates:

SQL dialects

ADO.NET behavior

relational execution

database compatibility

The goal is to allow unit tests to run without requiring a real database.

Current implemented providers include:

SQL Server

SQL Azure

MySQL

PostgreSQL (Npgsql)

Oracle

SQLite

DB2

The engine already contains:

SQL parser

AST representation

execution engine

execution plan

schema builder API

table mocks

column definitions

in-memory data storage

This roadmap defines the next evolution phases.

The order MUST be respected because later features depend on earlier ones.

Architectural Principles

The following rules MUST be respected.

1. Dialects must inherit a base dialect
SqlDialect

Every database dialect must extend it.

Example:

SqlServerDialect : SqlDialect
MySqlDialect : SqlDialect
PostgresDialect : SqlDialect

New dialects MUST follow the same structure.

2. Parser MUST remain dialect-agnostic

The parser should NOT contain database-specific logic.

Dialect differences must be handled in:

DialectNormalizer
DialectTranslator
3. Execution engine MUST operate on normalized AST

Execution logic must not depend on SQL syntax.

All syntax differences must be normalized before execution.

4. New providers must reuse existing dialect families

Do not duplicate logic.

Reuse existing dialects whenever possible.

Example:

MariaDbDialect : MySqlDialect
DuckDbDialect : PostgresDialect
SQL Dialect Families

Instead of implementing databases individually, dialects must be grouped.

Family	Databases
SQL Server	SQL Server, Azure SQL
MySQL	MySQL, MariaDB
PostgreSQL	PostgreSQL, DuckDB
Oracle	Oracle
IBM	DB2, Informix
Firebird	Firebird

Current coverage:

SQL Server family ✔
MySQL family ✔
PostgreSQL family ✔
Oracle family ✔
IBM family ✔
SQLite ✔
Phase 1 — SqlDialect.Auto
Objective

Allow the engine to automatically accept multiple SQL syntaxes without explicitly selecting a dialect.

Example queries must work simultaneously:

SELECT TOP 10 * FROM users
SELECT * FROM users LIMIT 10
SELECT * FROM users FETCH FIRST 10 ROWS
SELECT * FROM users WHERE ROWNUM <= 10
Implementation Steps
Step 1 — Add new dialect enum
SqlDialect.Auto
Step 2 — Create Syntax Detection Module

Create:

SqlSyntaxDetector

Responsibilities:

Detect SQL syntax patterns.

Examples:

Pattern	Dialect
TOP	SQL Server
LIMIT	MySQL/Postgres
ROWNUM	Oracle
FETCH FIRST	ANSI

Example implementation:

if(tokens.Contains("LIMIT"))
    detectedDialect = SqlDialect.MySql;

if(tokens.Contains("TOP"))
    detectedDialect = SqlDialect.SqlServer;
Step 3 — Normalize Syntax

Create:

DialectNormalizer

Example normalization:

TOP 10 → LIMIT_INTERNAL 10
ROWNUM <= 10 → LIMIT_INTERNAL 10
FETCH FIRST 10 ROWS → LIMIT_INTERNAL 10

Internal AST must use only:

LimitNode
OffsetNode
Step 4 — Execution Engine Update

Execution engine must use normalized nodes only.

No syntax-specific logic allowed.

Acceptance Criteria

The following queries must produce identical results:

SELECT TOP 5 * FROM table
SELECT * FROM table LIMIT 5
SELECT * FROM table FETCH FIRST 5 ROWS
Phase 2 — Query Plan Debugger
Objective

Expose execution internals to help debugging SQL execution.

This is not the execution plan, but a runtime trace.

Required API
db.DebugSql(query)

Returns:

QueryDebugTrace
Structure
QueryDebugTrace
 ├─ Steps
 │   ├─ Operator
 │   ├─ InputRows
 │   ├─ OutputRows
 │   ├─ ExecutionTime
 │   ├─ Details
Example Output
TableScan(users)
 rows produced: 120

Filter(age > 18)
 rows in: 120
 rows out: 80

Sort(name)
 rows in: 80
 rows out: 80

Limit(10)
 rows in: 80
 rows out: 10
Implementation Steps

Add tracing to execution operators

Capture row counts

Capture execution time

Store trace in memory

Expose through API

Phase 3 — MariaDB Dialect
Objective

Add MariaDB compatibility.

Implementation

Create:

MariaDbDialect : MySqlDialect

Differences to support:

RETURNING
SEQUENCE
JSON_TABLE
Acceptance

All MySQL queries must remain compatible.

MariaDB-specific features must parse correctly.

Phase 4 — Firebird Dialect

Create:

FirebirdDialect : SqlDialect

Features to support:

SELECT FIRST 10
ROWS 10
GENERATORS
Phase 5 — DuckDB Dialect

DuckDB SQL is very similar to PostgreSQL.

Implementation:

DuckDbDialect : PostgresDialect

Additional features:

STRUCT
LIST
UNNEST
Phase 6 — Schema Snapshot

Allow capturing schema definitions from ADO.NET connections.

API:

SchemaSnapshot.Export(connection)

Output:

schema.json

Load:

SchemaSnapshot.Load(schema.json)
Phase 7 — Cross Dialect Validator

Test SQL compatibility across dialects.

API:

db.ValidateAcrossDialects(query)

Example output:

SQL Server ✔
MySQL ✔
PostgreSQL ✔
Oracle ✖ (ROWNUM required)
Final Roadmap Order

Codex MUST implement features in this order:

1 SqlDialect.Auto
2 Query Plan Debugger
3 MariaDB Dialect
4 Firebird Dialect
5 DuckDB Dialect
6 Schema Snapshot
7 Cross Dialect Validator
Important Rules

Codex MUST follow:

Do not modify existing dialect behavior

Do not introduce syntax in execution engine

Normalize syntax before execution

Reuse dialect implementations

Keep parser dialect-neutral

Future Extensions (not required yet)

Possible future dialects:

ClickHouse
Snowflake
Informix
Expected Outcome

After this roadmap the engine will support:

automatic SQL dialect detection

SQL execution debugging

extended database compatibility

schema import

cross-dialect validation

This will significantly improve the usability of DbSqlLikeMem for real-world unit testing scenarios.