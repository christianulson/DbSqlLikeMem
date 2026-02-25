# Matriz de compatibilidade SQL (feature x dialeto)

> Status consolidado para os dialetos principais: **MySQL / SQL Server / Oracle / Npgsql / DB2 / SQLite**.
> 
> Legenda: ✅ suportado, ⚠️ suportado parcialmente/condicional, ❌ não suportado.

## Matriz simplificada

| Feature SQL | MySQL | SQL Server | Oracle | Npgsql | DB2 | SQLite | Testes de referência |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `WITH` / CTE básica | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | [`SubqueryFromAndJoinsTests`](../src/DbSqlLikeMem.MySql.Test/SubqueryFromAndJoinsTests.cs) |
| `WITH RECURSIVE` | ⚠️ (versão mínima do dialeto) | ❌ | ❌ | ✅ | ❌ | ✅ | [`*DialectFeatureParserTests`](../src/DbSqlLikeMem.MySql.Test/Parser/MySqlDialectFeatureParserTests.cs) |
| `WITH ... AS MATERIALIZED` | ❌ | ❌ | ❌ | ✅ | ❌ | ⚠️ (`NOT MATERIALIZED` em cenários suportados) | [`NpgsqlDialectFeatureParserTests`](../src/DbSqlLikeMem.Npgsql.Test/Parser/NpgsqlDialectFeatureParserTests.cs) |
| `LIMIT/OFFSET` | ✅ | ❌ (`OFFSET/FETCH`) | ❌ (`FETCH FIRST/NEXT`) | ✅ | ❌ (`FETCH FIRST`) | ✅ | [`*SqlCompatibilityGapTests`](../src/DbSqlLikeMem.MySql.Dapper.Test/MySqlSqlCompatibilityGapTests.cs) |
| `OFFSET ... FETCH` | ❌ | ✅ (>= versão mínima) | ❌ | ❌ | ❌ | ❌ | [`SqlServerDialectFeatureParserTests`](../src/DbSqlLikeMem.SqlServer.Test/Parser/SqlServerDialectFeatureParserTests.cs) |
| `FETCH FIRST/NEXT` | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ | [`Db2DialectFeatureParserTests`](../src/DbSqlLikeMem.Db2.Test/Parser/Db2DialectFeatureParserTests.cs) |
| `INSERT ... ON DUPLICATE KEY UPDATE` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | [`InsertOnDuplicateTests`](../src/DbSqlLikeMem.MySql.Test/Strategy/MySqlInsertOnDuplicateTests.cs) |
| `INSERT ... ON CONFLICT` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | [`ExtendedPostgreSqlMockTests`](../src/DbSqlLikeMem.Npgsql.Dapper.Test/ExtendedPostgreSqlMockTests.cs) |
| Table hints SQL Server `WITH (...)` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ | [`SqlServerDialectFeatureParserTests`](../src/DbSqlLikeMem.SqlServer.Test/Parser/SqlServerDialectFeatureParserTests.cs) |
| Index hints MySQL (`USE/IGNORE/FORCE INDEX`) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | [`MySqlDialectFeatureParserTests`](../src/DbSqlLikeMem.MySql.Test/Parser/MySqlDialectFeatureParserTests.cs) |
| Operadores JSON `->` / `->>` | ⚠️ (dependente de parser/executor por cenário) | ❌ | ❌ | ✅ | ❌ | ✅ | [`*AdvancedSqlGapTests`](../src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlAdvancedSqlGapTests.cs) |
| Triggers em tabelas não temporárias (`TableMock`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | [`*MockTests`](../src/DbSqlLikeMem.MySql.Test/MySqlMockTests.cs) |

## Notas rápidas

- Esta matriz resume o comportamento esperado após os hardenings de parser/testes. Em caso de divergência, os testes por provider têm prioridade como fonte de verdade.
- Recursos marcados como ⚠️ indicam suporte com gate de versão do dialeto ou cobertura parcial.
- Para evoluções planejadas, consulte também o checklist de gaps conhecidos.

## Referências

- [Provedores e features](providers-and-features.md)
- [Checklist de known gaps](known-gaps-checklist.md)
- [Matriz versionada vCurrent](sql-compatibility-matrix-vcurrent.md)
- [Matriz versionada vNext](sql-compatibility-matrix-vnext.md)
