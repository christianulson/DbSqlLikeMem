# DbSqlLikeMem

**EN:** Core package of the **DbSqlLikeMem** ecosystem: an in-memory SQL-like engine that helps you test data access code in C# with speed, repeatability, and confidence.
**PT-BR:** Pacote base do ecossistema **DbSqlLikeMem**: um motor SQL-like em memória para testar acesso a dados em C# com velocidade, previsibilidade e confiança.

## What this package provides | O que este pacote entrega

- **EN:** In-memory database structures (schema, tables, columns, indexes).
  **PT-BR:** Estruturas de banco em memória (schema, tabelas, colunas, índices).
- **EN:** SQL parser and executor for common test scenarios (DDL and DML subsets).
  **PT-BR:** Parser e executor SQL para cenários comuns de teste (subconjuntos de DDL e DML).
- **EN:** Data seeding helpers and fluent builders for setup.
  **PT-BR:** Helpers de seed e builders fluentes para setup.
- **EN:** Schema-level sequence registration plus optional identity override helpers for deterministic scenarios.
  **PT-BR:** Registro de sequences em nível de schema e helpers opcionais de sobrescrita de identity para cenários determinísticos.
- **EN:** ADO.NET-friendly behavior used by provider packages.
  **PT-BR:** Comportamento compatível com ADO.NET usado pelos pacotes de provedor.
- **EN:** Mock execution plans with lightweight runtime metrics and per-connection history.
  **PT-BR:** Planos de execução mock com métricas de runtime simplificadas e histórico por conexão.

## Typical use cases | Quando usar

Use `DbSqlLikeMem` when you want to / Use `DbSqlLikeMem` quando quiser:

- **EN:** Reduce test costs that currently rely on a real database server.
  **PT-BR:** Reduzir custo de testes que hoje dependem de banco real.
- **EN:** Build reproducible QA scenarios with deterministic data setup.
  **PT-BR:** Criar cenários de QA reproduzíveis com setup determinístico.
- **EN:** Validate query and transformation logic with fast feedback loops.
  **PT-BR:** Validar regras de query e transformação de dados com ciclo rápido.
- **EN:** Inspect query behavior in tests with simplified execution-plan metrics.
  **PT-BR:** Investigar comportamento de queries em testes com métricas simplificadas de plano.

## Installation | Instalação

```bash
dotnet add package DbSqlLikeMem
```

## Quick example | Exemplo rápido

```csharp
var db = new DbMock();
var users = db.AddTable("Users");
users.AddColumn("Id", DbType.Int32, false);
users.AddColumn("Name", DbType.String, false);
users.AddPrimaryKeyIndexes("Id");
```

## Next step: choose a provider package | Próximo passo: escolha um pacote de provedor

**EN:** Add at least one provider package to emulate your database dialect in tests (`DbSqlLikeMem.MySql`, `DbSqlLikeMem.SqlServer`, `DbSqlLikeMem.SqlAzure`, `DbSqlLikeMem.Oracle`, `DbSqlLikeMem.Npgsql`, `DbSqlLikeMem.Sqlite`, or `DbSqlLikeMem.Db2`).
**PT-BR:** Adicione ao menos um pacote de provedor para simular o dialeto do seu banco nos testes (`DbSqlLikeMem.MySql`, `DbSqlLikeMem.SqlServer`, `DbSqlLikeMem.SqlAzure`, `DbSqlLikeMem.Oracle`, `DbSqlLikeMem.Npgsql`, `DbSqlLikeMem.Sqlite` ou `DbSqlLikeMem.Db2`).

## One-call test factory | Factory de uma chamada para testes

**EN:** You can create `DbMock` + `IDbConnection` in a single call:
**PT-BR:** Você pode criar `DbMock` + `IDbConnection` com uma chamada única:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateSqliteWithTables(
    d => d.AddTable("Users",
        [new Col("Id", DataTypeDef.Int32()), new Col("Name", DataTypeDef.String())],
        [new Dictionary<int, object?> { [0] = 1, [1] = "Ana" }]));
```

**EN:** There are provider shortcuts as well: `CreateOracleWithTables`, `CreateSqlServerWithTables`, `CreateSqlAzureWithTables`, `CreateMySqlWithTables`, `CreateSqliteWithTables`, `CreateDb2WithTables`, and `CreateNpgsqlWithTables`.
**PT-BR:** Também existem atalhos por banco: `CreateOracleWithTables`, `CreateSqlServerWithTables`, `CreateSqlAzureWithTables`, `CreateMySqlWithTables`, `CreateSqliteWithTables`, `CreateDb2WithTables` e `CreateNpgsqlWithTables`.

**EN:** If preferred, use the generic string-based entry point:
**PT-BR:** Se preferir, use a entrada genérica por string:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateWithTables("SqlServer", d => { /* mapeamentos */ });
```

> **EN:** Tip: the factory resolves connection types via reflection, so it works best when the provider package is already referenced and loaded by your test project.
> **PT-BR:** Dica: a factory resolve os tipos de conexão via reflexão, então funciona melhor quando o pacote de provedor já está referenciado e carregado no projeto de teste.

## Sequence quick reference | Referência rápida de sequence

- **EN:** SQL Server uses `NEXT VALUE FOR schema.seq_name`.
  **PT-BR:** SQL Server usa `NEXT VALUE FOR schema.seq_name`.
- **EN:** PostgreSQL uses `nextval`, `currval`, `setval`, and `lastval`.
  **PT-BR:** PostgreSQL usa `nextval`, `currval`, `setval` e `lastval`.
- **EN:** Oracle uses `schema.seq_name.NEXTVAL` and `schema.seq_name.CURRVAL`.
  **PT-BR:** Oracle usa `schema.seq_name.NEXTVAL` e `schema.seq_name.CURRVAL`.
- **EN:** DB2 uses `NEXT VALUE FOR schema.seq_name` and `PREVIOUS VALUE FOR schema.seq_name`.
  **PT-BR:** DB2 usa `NEXT VALUE FOR schema.seq_name` e `PREVIOUS VALUE FOR schema.seq_name`.

**EN:** See the full examples in [Getting Started](../../docs/getting-started.md).
**PT-BR:** Veja os exemplos completos em [Getting Started](../../docs/getting-started.md).

## Scope and expectations | Escopo e expectativas

- **EN:** This package is intended for tests and local validation, not as a production database replacement.
  **PT-BR:** Este pacote é voltado para testes e validação local, não para substituir banco em produção.
- **EN:** SQL support is incremental and dialect-aware through provider-specific packages.
  **PT-BR:** O suporte SQL é incremental e sensível a dialeto por meio de pacotes específicos por provedor.
- **EN:** Unsupported SQL constructs should fail fast with clear exceptions.
  **PT-BR:** Construções SQL não suportadas devem falhar rapidamente com exceções claras.
- **EN:** Execution-plan metrics are diagnostic and relative; do not use mock timings as production performance benchmarks.
  **PT-BR:** As métricas de plano de execução são diagnósticas e relativas; não use tempos do mock como benchmark de performance de produção.

## Learn more | Saiba mais

- **EN:** Full docs and guides: [Repository README](../../README.md) and [Getting Started](../../docs/getting-started.md).
  **PT-BR:** Documentação completa e guias: [README do repositório](../../README.md) e [Guia de início](../../docs/getting-started.md).

## Contributing | Contribuindo

Contributions are very welcome / Contribuições são muito bem-vindas 💙

- **EN:** Open issues with real SQL samples and expected behavior.
  **PT-BR:** Abra issues com exemplos reais de SQL e comportamento esperado.
- **EN:** Submit PRs with focused tests and clear intent.
  **PT-BR:** Envie PRs com testes objetivos e intenção clara.
- **EN:** Help improve docs and examples for new users.
  **PT-BR:** Ajude a melhorar documentação e exemplos para novos usuários.
