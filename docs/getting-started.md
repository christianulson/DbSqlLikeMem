# Começando rápido

## Instalação

### Opção 1: referência de projeto (recomendado para desenvolvimento local)

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

### Opção 2: referência a DLLs compiladas

```bash
dotnet build src/DbSqlLikeMem.slnx
```

Depois referencie no projeto de testes:

- `DbSqlLikeMem.dll`
- uma DLL de provider (ex.: `DbSqlLikeMem.SqlServer.dll`)

## NuGet e dependências

Cada provider é empacotado separadamente (ex.: `DbSqlLikeMem.MySql`, `DbSqlLikeMem.Npgsql`).

Durante o `dotnet pack`, cada provider inclui dependência de `DbSqlLikeMem`.
Assim, ao instalar um provider via nuget.org, o núcleo é instalado automaticamente.

## Seleção de provider em runtime

Quando o banco é escolhido em tempo de execução, use uma factory:

```csharp
using DbSqlLikeMem.MySql;
using DbSqlLikeMem.Npgsql;
using DbSqlLikeMem.Oracle;
using DbSqlLikeMem.SqlServer;

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

## Testes

```bash
dotnet test src/DbSqlLikeMem.slnx
```

## Links relacionados

- [Provedores, versões e compatibilidade](providers-and-features.md)
- [Publicação](publishing.md)
- [Wiki do GitHub](wiki/README.md)
