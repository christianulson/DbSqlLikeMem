# Plano executável — P7 a P14

Documento gerado por `scripts/generate_p7_p10_plan.py` para orientar implementação técnica dos itens P7 a P14 com base nos testes existentes por provider.

## Como usar

1. Escolha um pilar (P7–P14).
2. Implemente provider por provider, priorizando os arquivos mapeados abaixo.
3. Rode os testes por provider e atualize o status.

## P7 (DML avançado)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/Strategy/SqlServerInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/Strategy/OracleInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/Strategy/PostgreSqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2InsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2UpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2DeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |

## P8 (Paginação/ordenação)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |

## P9 (JSON)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |

## P10 (Procedures/OUT params)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |

## P11 (Confiabilidade transacional e concorrência)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlTransactionTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/MySqlTransactionTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/MySqlTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerTransactionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerTransactionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Strategy/OracleTransactionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/OracleTransactionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/OracleTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlTransactionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlTransactionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteTransactionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteTransactionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2TransactionTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/Db2TransactionTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/Db2TransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |

## P12 (Observabilidade, diagnóstico e ergonomia de erro)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2AdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |

## P13 (Performance e escala do engine em memória)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Performance/MySqlPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Performance/SqlServerPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Performance/OraclePerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Performance/PostgreSqlPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Performance/SqlitePerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Performance/Db2PerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |

## P14 (Conformidade de ecossistema .NET/ORM/tooling)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.MySql.Test/MySqlLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/SqlServerLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Oracle.Test/OracleLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/PostgreSqlLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/SqliteLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Db2.Test/Db2LinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |

## Checklist de saída por PR

- [ ] Parser e Dialect atualizados para o pilar.
- [ ] Executor atualizado para os casos do pilar.
- [ ] Testes do provider alterado verdes.
- [ ] Smoke tests dos demais providers sem regressão.
- [ ] Documentação de compatibilidade atualizada.



## Melhorias práticas para o plano de execução (Index Advisor)

- [ ] Incluir seção `IndexRecommendations` no plano para queries SELECT com alto `EstimatedRowsRead`.
- [ ] Sugerir índice composto com colunas de `WHERE/JOIN` e complementar com `ORDER BY` quando aplicável.
- [ ] Exibir `Confidence` por recomendação para facilitar priorização técnica.
- [ ] Cobrir cenários com e sem índice nos testes `ExecutionPlanTests` dos providers.

