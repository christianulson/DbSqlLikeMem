# AI_CONTEXT.md

## Repository Snapshot

`DbSqlLikeMem` is a .NET repository centered on in-memory database mocks and provider-specific test coverage.

The codebase contains:

- core database mock engine
- provider-specific adapters and mocks:
  - `SqlServer`
  - `SqlAzure`
  - `Sqlite`
  - `MySql`
  - `Npgsql`
  - `Oracle`
  - `Db2`
- integration-style test projects for:
  - Dapper
  - EF Core
  - LinqToDb
  - NHibernate

## Active Maintenance Theme

The current recurring task is XML documentation cleanup for compiler warning and error `CS1591`.

This includes:

- public API members in provider mock projects
- public/protected members in shared test bases
- public concrete test wrappers in provider-specific test projects

## Documentation Conventions

### Mandatory order

Always document in this order:

1. `EN`
2. `PT`

### Mandatory content

Every summary must explain what the member does.

Avoid weak summaries such as:

- `Defines the class ...`
- `Tests ... behavior`

Prefer:

- what the class represents
- what the property exposes
- what the method creates, validates, restores, or executes

### Override rule

For members marked `override`:

- default to `/// <inheritdoc />`
- write a custom summary only if the override introduces provider-specific behavior that is not already clear from the base contract

### Concrete examples

For a property:

```csharp
/// <summary>
/// EN: Exposes the SQL batch text used to validate paginated execution.
/// PT: Expoe o texto SQL do batch usado para validar a execucao paginada.
/// </summary>
```

For a test method:

```csharp
/// <summary>
/// EN: Verifies rollback to savepoint restores the snapshot of connection temporary tables.
/// PT: Verifica se rollback para savepoint restaura o snapshot das tabelas temporarias da conexao.
/// </summary>
```

For an override:

```csharp
/// <inheritdoc />
protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);
```

## Provider Test Pattern

Many provider-specific Dapper test projects mirror the same structure:

- `Extended*MockTests`
- `FluentTest`
- `*AdditionalBehaviorCoverageTests`
- `*JoinTests`
- `*TransactionReliabilityTests`
- `*TransactionTests`
- `*UnionLimitAndJsonCompatibilityTests`
- `Query/QueryExecutorExtrasTests`
- `StoredProcedureExecutionTests`

When updating one provider, the same documentation approach usually applies to the others.

## Practical Guidance For Future Agents

When a new `CS1591` batch appears:

1. identify whether members are concrete or `override`
2. use `inheritdoc` on overrides
3. add EN/PT summaries to concrete members
4. keep wording behavior-based
5. preserve existing naming and test structure

## Important User Preference

The user explicitly wants:

- summaries filled consistently
- `EN` first, then `PT`
- every summary to explain what the method, property, or class does
- `inheritdoc` used on overrides when appropriate
