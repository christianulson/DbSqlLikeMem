# Plano executável — P7 a P10

Documento gerado por `scripts/generate_p7_p10_plan.py` para orientar implementação técnica dos itens P7, P8, P9 e P10 com base nos testes existentes por provider.

## Como usar

1. Escolha um pilar (P7–P10).
2. Implemente provider por provider, priorizando os arquivos mapeados abaixo.
3. Rode os testes por provider e atualize o status.

## P7 (DML avançado)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlDeleteStrategyTests.cs` | ✅ Done |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerDeleteStrategyTests.cs` | ✅ Done |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Strategy/OracleInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleDeleteStrategyTests.cs` | ✅ Done |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlDeleteStrategyTests.cs` | ✅ Done |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteDeleteStrategyTests.cs` | ✅ Done |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2InsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2UpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2DeleteStrategyTests.cs` | ✅ Done |

## P8 (Paginação/ordenação)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |

## P9 (JSON)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | ✅ Done |

## P10 (Procedures/OUT params)

| Provider | Arquivos de teste-alvo | Status |
| --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/StoredProcedureSignatureTests.cs` | ✅ Done |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/StoredProcedureSignatureTests.cs` | ✅ Done |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/StoredProcedureSignatureTests.cs` | ✅ Done |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/StoredProcedureSignatureTests.cs` | ✅ Done |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/StoredProcedureSignatureTests.cs` | ✅ Done |
| DB2 | `src/DbSqlLikeMem.Db2.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/StoredProcedureSignatureTests.cs` | ✅ Done |

## Checklist de saída por PR

- [x] Parser e Dialect atualizados para o pilar.
- [x] Executor atualizado para os casos do pilar.
- [ ] Testes do provider alterado verdes.
- [ ] Smoke tests dos demais providers sem regressão.
- [x] Documentação de compatibilidade atualizada.

