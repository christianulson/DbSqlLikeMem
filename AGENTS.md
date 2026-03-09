# AGENTS.md

## Purpose

This repository uses AI assistants and coding agents to help with implementation, refactoring, documentation, and test maintenance.

This file defines the minimum operating rules for any agent working in this repo.

## Repo Context

- Repository: `DbSqlLikeMem`
- Main stack: `.NET / C#`
- Main concern in the current maintenance cycle:
  - remove `CS1591` warnings and errors
  - standardize XML documentation
  - keep test helper hierarchies consistent

## XML Documentation Rules

When adding or updating XML comments, follow these rules exactly:

1. Use `EN` first, then `PT`.
2. Always explain what the member does.
3. Prefer concise and direct wording.
4. For `override` members, use `/// <inheritdoc />` when the base member already documents the contract.
5. Only write a new `<summary>` on `override` members when inheritance is not enough or when the override adds relevant behavior that must be documented explicitly.
6. Keep wording factual. Do not use generic text such as:
   - `Defines the class ...`
   - `Tests ... behavior`
7. Public methods, properties, fields, classes, constructors, and test wrappers exposed to the compiler's XML-doc validation must be documented.

## Summary Pattern

Use this structure:

```csharp
/// <summary>
/// EN: Explains what the method, property, or class does.
/// PT: Explica o que o metodo, propriedade ou classe faz.
/// </summary>
```

Example for a concrete method:

```csharp
/// <summary>
/// EN: Creates and opens the mock connection used by this test suite.
/// PT: Cria e abre a conexao simulada usada por esta suite de testes.
/// </summary>
```

Example for an override:

```csharp
/// <inheritdoc />
protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);
```

## Test Wrapper Rules

For public test wrapper methods that call base test helpers:

- describe the behavior being validated
- do not document them with placeholder wording
- preserve the same EN/PT order

Example:

```csharp
/// <summary>
/// EN: Verifies left joins keep all rows from the left table.
/// PT: Verifica se left joins mantem todas as linhas da tabela da esquerda.
/// </summary>
```

## File Editing Rules

- Prefer minimal diffs.
- Do not reformat unrelated code.
- Do not replace meaningful summaries with generic summaries.
- Do not remove existing valid XML comments just to restyle them unless consistency is the goal of the current task.

## Validation Guidance

When touching XML docs:

- check whether the target member is `override`
- if yes, prefer `inheritdoc`
- if no, add a proper EN/PT summary
- verify nested public classes and helper contract members too
- if a provider-specific test project mirrors another provider, reuse the same documentation pattern adapted to the provider type

## Execution Policy

For the current TDD workflow in this repository:

- do not run `dotnet build`
- do not run `dotnet test`
- do not trigger restores implicitly through validation commands
- only execute builds or tests when the user explicitly asks for execution
- assume the user runs validation manually unless they say otherwise

## Test Failure Triage

When fixing a failing test:

- first determine whether the defect is in the test or in the implementation
- do not assume the production code is wrong just because a new test fails
- do not assume the test is wrong just because the implementation currently behaves differently
- use the behavior of the real database/provider/version being simulated as the source of truth
- preserve provider-specific differences across `SqlServer`, `SqlAzure`, `Sqlite`, `MySql`, `Npgsql`, `Oracle`, and `Db2`
- prefer correcting the test when the implementation already matches the real database behavior
- prefer correcting the implementation when the test reflects the real database behavior and the mock does not

This project is used to validate application behavior before production, so the mock must follow the same behavior as the real database as closely as possible.

## Current Practical Rule

If the user asks to "preencher os summarys", the expected behavior is:

- fill missing XML docs
- use `EN` first and `PT` second
- explain what the member does
- use `inheritdoc` on overrides whenever appropriate
