# Começando rápido

## Instalação

### Opção 1: referência de projeto (recomendado para desenvolvimento local)

```xml
<ItemGroup>
  <ProjectReference Include="../src/DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../src/DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

### Opção 2: referência a DLLs compiladas

```bash
dotnet build src/DbSqlLikeMem.slnx
```

Depois referencie no projeto de testes:

- `DbSqlLikeMem.dll`
- uma DLL de provider (ex.: `DbSqlLikeMem.SqlServer.dll`)

### Opção 3: pacote NuGet (recomendado para consumo em times)

Instale apenas o provider que representa o banco que você quer simular. O pacote core (`DbSqlLikeMem`) entra como dependência transitiva.

Exemplos:

```bash
dotnet add package DbSqlLikeMem.SqlServer
dotnet add package DbSqlLikeMem.Npgsql
dotnet add package DbSqlLikeMem.MySql
dotnet add package DbSqlLikeMem.Oracle
dotnet add package DbSqlLikeMem.Sqlite
dotnet add package DbSqlLikeMem.Db2
```

## NuGet e dependências

Cada provider é empacotado separadamente (ex.: `DbSqlLikeMem.MySql`, `DbSqlLikeMem.Npgsql`).

Durante o `dotnet pack`, cada provider inclui dependência de `DbSqlLikeMem`.
Assim, ao instalar um provider via nuget.org, o núcleo é instalado automaticamente.

Dica: escolha **um provider por projeto de teste** (ou por suíte), conforme o dialeto que você precisa validar.

## Seleção de provider em runtime

Quando o banco é escolhido em tempo de execução, use uma factory:

```csharp
using DbSqlLikeMem.MySql;
using DbSqlLikeMem.Npgsql;
using DbSqlLikeMem.Oracle;
using DbSqlLikeMem.Sqlite;
using DbSqlLikeMem.SqlServer;
using DbSqlLikeMem.Db2;

public static class DbSqlLikeMemFactory
{
    public static DbConnection Create(string provider)
    {
        return provider.ToLowerInvariant() switch
        {
            "mysql" => new MySqlConnectionMock(new MySqlDbMock()),
            "sqlserver" => new SqlServerConnectionMock(new SqlServerDbMock()),
            "oracle" => new OracleConnectionMock(new OracleDbMock()),
            "postgres" or "postgresql" or "npgsql" => new NpgsqlConnectionMock(new NpgsqlDbMock()),
            "sqlite" or "sqlite3" => new SqliteConnectionMock(new SqliteDbMock()),
            "db2" => new Db2ConnectionMock(new Db2DbMock()),
            _ => throw new ArgumentException($"Unsupported provider: {provider}")
        };
    }
}
```

Se carregar DLLs dinamicamente, garanta que o assembly do provider esteja disponível ao test runner.

## Setup para testes (InternalsVisibleTo)

Se um assembly de testes customizado precisar acessar `internal`, adicione `InternalsVisibleTo` no core ou via `AssemblyInfo.cs`:

```csharp
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("MyCustomTestAssembly")]
```

## Exemplos de uso

### Exemplo 1: schema fluente + execução compatível com Dapper (SQL Server)

```csharp
using DbSqlLikeMem.SqlServer;

var db = new SqlServerDbMock { ThreadSafe = true };
using var connection = new SqlServerConnectionMock(db);

connection.DefineTable("user")
    .Column<int>("id", pk: true, identity: true)
    .Column<string>("name")
    .Column<string>("email", nullable: true)
    .Column<DateTime>("created", nullable: false);

connection.Open();

connection.Execute(
    "INSERT INTO user (name, email, created) VALUES (@name, @email, @created)",
    new { name = "Alice", email = "alice@mail.com", created = DateTime.UtcNow });

var users = connection.Query("SELECT * FROM user").ToList();
```

### Exemplo 2: schema manual + seed (PostgreSQL)

```csharp
using DbSqlLikeMem.Npgsql;

var db = new NpgsqlDbMock();
var table = db.AddTable("Users");

table.Columns["Id"] = new(0, DbType.Int32, false);
table.Columns["Name"] = new(1, DbType.String, false);

table.Add(new Dictionary<int, object?>
{
    { 0, 1 },
    { 1, "John Doe" }
});

using var connection = new NpgsqlConnectionMock(db);
var result = connection.Query("SELECT * FROM Users WHERE Id = @Id", new { Id = 1 });
```

### Exemplo 3: inspeção de plano de execução e métricas

```csharp
using DbSqlLikeMem.MySql;

using var cnn = new MySqlConnectionMock();
cnn.Define("users");
cnn.Column<int>("users", "Id");
cnn.Column<int>("users", "Active");
cnn.Seed("users", null, [1, 1], [2, 0], [3, 1]);

using var cmd = new MySqlCommandMock(cnn)
{
    CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
};

using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    // consume rows
}

Console.WriteLine(cnn.LastExecutionPlan);
// Campos úteis no plano:
// - EstimatedCost
// - InputTables
// - EstimatedRowsRead
// - ActualRows
// - SelectivityPct
// - RowsPerMs
// - ElapsedMs
```

Observações:

- `LastExecutionPlan` traz o último plano gerado para a conexão.
- `LastExecutionPlans` mantém o histórico da última execução do comando (útil para SQL com múltiplos SELECTs).
- O plano também fica disponível no resultado (`TableResultMock.ExecutionPlan`) internamente no executor AST.

## Checklist rápido de revisão de documentação

Use esta lista quando fizer alterações grandes no código:

- Atualize a tabela de provedores/versões em `README.md` e `docs/providers-and-features.md`.
- Verifique se exemplos de `factory` cobrem todos os providers suportados.
- Confirme se novos recursos aparecem em pelo menos um guia de uso (`docs/getting-started.md`) e um guia de referência (`docs/providers-and-features.md`).
- Se houver impacto de distribuição, revise `docs/publishing.md`.

## Testes

```bash
dotnet test src/DbSqlLikeMem.slnx
```

## Links relacionados

- [Provedores, versões e compatibilidade](providers-and-features.md)
- [Publicação](publishing.md)
- [Wiki do GitHub](wiki/README.md)
