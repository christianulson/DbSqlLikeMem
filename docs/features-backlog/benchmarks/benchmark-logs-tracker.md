# Benchmark Logs Tracker

Registro dos erros encontrados em `src/benchmark/DbSqlLikeMem.Benchmarks/Logs`.
Todos os arquivos de log desta pasta foram corrigidos e removidos.

| Log | Problema principal | Status |
| --- | --- | --- |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.Db2DbSqlLikeMemSession-DB2-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.FirebirdDbSqlLikeMemSession-Firebird-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.MariaDbDbSqlLikeMemSession-MariaDB-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.MySqlDbSqlLikeMemSession-MySql-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.NpgsqlDbSqlLikeMemSession-PostgreSQL _ Npgsql-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.OracleDbSqlLikeMemSession-Oracle-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.SqlAzureDbSqlLikeMemSession-SQL Azure-errors.log` | `ApplyWindowTemporalComposite` reutiliza o mesmo cache key de `UsersOrders` com seeds diferentes e o benchmark de insert nulo usa o cenário errado. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.SqliteDbSqlLikeMemSession-SQLite-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem.SqlServerDbSqlLikeMemSession-SQL Server-errors.log` | `ApplyWindowTemporalComposite` reutiliza o mesmo cache key de `UsersOrders` com seeds diferentes e o benchmark de insert nulo usa o cenário errado. | Corrigido e removido |
| `DbSqlLikeMem.Benchmarks.Sessions.External.SqliteNativeSession-SQLite-errors.log` | Joins agregados com seeds de pedidos sem `Amount`/`Quantity` compatíveis com os cenários esperados. | Corrigido e removido |
