# Plano executável — P7 a P10

Documento gerado por `scripts/generate_p7_p10_plan.py` para orientar implementação técnica dos itens P7, P8, P9 e P10 com base nos testes existentes por provider.

## Como usar

1. Escolha um pilar (P7–P10).
2. Implemente provider por provider, priorizando os arquivos mapeados abaixo.
3. Rode os testes por provider e atualize o status.

## P7 (DML avançado)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlDeleteStrategyTests.cs` | ⬜ Pending |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerDeleteStrategyTests.cs` | ⬜ Pending |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Strategy/OracleInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleDeleteStrategyTests.cs` | ⬜ Pending |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlDeleteStrategyTests.cs` | ⬜ Pending |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteDeleteStrategyTests.cs` | ⬜ Pending |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2InsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2UpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2DeleteStrategyTests.cs` | ⬜ Pending |

## P8 (Paginação/ordenação)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |

## P9 (JSON)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | ⬜ Pending |

## P10 (Procedures/OUT params)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |
| DB2 | `src/DbSqlLikeMem.Db2.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/StoredProcedureSignatureTests.cs` | ⬜ Pending |

## Checklist de saída por PR

- [ ] Parser e Dialect atualizados para o pilar.
- [ ] Executor atualizado para os casos do pilar.
- [ ] Testes do provider alterado verdes.
- [ ] Smoke tests dos demais providers sem regressão.
- [ ] Documentação de compatibilidade atualizada.

