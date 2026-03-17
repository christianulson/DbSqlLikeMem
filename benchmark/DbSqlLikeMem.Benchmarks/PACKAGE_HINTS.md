# Package hints

Você disse que já tem o `.csproj`, então deixei só o checklist de dependências.

## Base

- `BenchmarkDotNet`

## DbSqlLikeMem (um provider por vez ou todos, se quiser a matriz completa)

- `DbSqlLikeMem.MySql`
- `DbSqlLikeMem.SqlServer`
- `DbSqlLikeMem.SqlAzure`
- `DbSqlLikeMem.Oracle`
- `DbSqlLikeMem.Npgsql`
- `DbSqlLikeMem.Sqlite`
- `DbSqlLikeMem.Db2`

## Banco real / clientes ADO.NET

- MySQL: `MySqlConnector`
- PostgreSQL: `Npgsql`
- SQL Server / SQL Azure proxy: `Microsoft.Data.SqlClient`
- Oracle: `Oracle.ManagedDataAccess.Core`
- SQLite: `Microsoft.Data.Sqlite`
- DB2:
  - Windows: `Net.IBM.Data.Db2`
  - Linux: `Net.IBM.Data.Db2-lnx`
  - macOS: `Net.IBM.Data.Db2-osx`

## Testcontainers

- `Testcontainers.MySql`
- `Testcontainers.PostgreSql`
- `Testcontainers.MsSql`
- `Testcontainers.Oracle`
- `Testcontainers.Db2`

## Dica prática

Se o objetivo for manter o benchmark simples no começo, rode primeiro só:

- `MySql`
- `Npgsql`
- `SqlServer`
- `Sqlite`

Depois habilite `Oracle` e `Db2`, que são os provedores com setup externo mais pesado.
