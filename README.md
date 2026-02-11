# DbSqlLikeMem

In-memory C# database engine for unit tests that emulates SQL dialects and ADO.NET behavior for **MySQL**, **SQL Server**, **Oracle**, and **PostgreSQL (Npgsql)**.

This project lets you test data access code without a real database by using provider-specific connection mocks and a SQL parser/executor built into the library.

## Features

- Provider-specific mocks: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql)
- SQL parsing + execution for common DDL/DML
- Fluent schema definition and seeding helpers
- Dapper-friendly execution (implements common ADO.NET behaviors)
- Dialect-aware behavior differences

## Requirements

- Bibliotecas de provider: .NET Framework 4.8, .NET 6.0 e .NET 8.0.
- Núcleo `DbSqlLikeMem`: .NET Standard 2.0 + .NET Framework 4.8, .NET 6.0 e .NET 8.0.

## Supported Providers

| Provider | Package/Project |
| --- | --- |
| MySQL | `DbSqlLikeMem.MySql` |
| SQL Server | `DbSqlLikeMem.SqlServer` |
| Oracle | `DbSqlLikeMem.Oracle` |
| PostgreSQL | `DbSqlLikeMem.Npgsql` |

## Installation

### Option 1: Project reference (recommended for local development)

Add a reference to the core library and the provider you want:

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

### Option 2: Reference compiled DLLs

Build the solution and reference the DLLs from your test project:

```bash
dotnet build src/DbSqlLikeMem.slnx
```

Then add references to:

- `DbSqlLikeMem.dll`
- One provider DLL (e.g., `DbSqlLikeMem.SqlServer.dll`)

### NuGet packages and dependencies

Each provider ships as its own NuGet package (e.g., `DbSqlLikeMem.MySql`, `DbSqlLikeMem.Npgsql`, etc.). The provider projects reference the core project (`DbSqlLikeMem`) via `ProjectReference`, so when you run `dotnet pack` the resulting provider `.nupkg` includes a dependency on the core package. When you install a provider package from nuget.org, NuGet will automatically install `DbSqlLikeMem` as a dependency.

## Registering the provider DLL (choosing a database at runtime)

When the database is chosen by the user at runtime, load the corresponding provider assembly and create its connection mock. A simple factory approach looks like this:

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

If you load DLLs dynamically, ensure the provider assembly is available to your test runner (e.g., by referencing the DLL or using `AssemblyLoadContext` to load it from disk before creating the connection).

## Test assembly setup (InternalsVisibleTo)

Some internal members are used by the provider projects and tests. The core project already declares `InternalsVisibleTo` for all provider and test assemblies. If you create your own test assembly and need access to internals, add an `InternalsVisibleTo` entry in `DbSqlLikeMem.csproj` **or** create an `AssemblyInfo.cs` in the core project with:

```csharp
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("MyCustomTestAssembly")]
```

Then reference the core and provider DLLs in your test project.

## Usage Examples

### 1) Fluent schema + Dapper-compatible execution (SQL Server example)

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

### 2) Manual schema + seed data (PostgreSQL example)

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

### 3) Using the right provider for each database

```csharp
// MySQL
using DbSqlLikeMem.MySql;
var mysql = new MySqlConnectionMock(new MySqlDbMock());

// Oracle
using DbSqlLikeMem.Oracle;
var oracle = new OracleConnectionMock(new OracleDbMock());

// SQL Server
using DbSqlLikeMem.SqlServer;
var sqlServer = new SqlServerConnectionMock(new SqlServerDbMock());

// PostgreSQL
using DbSqlLikeMem.Npgsql;
var postgres = new NpgsqlConnectionMock(new NpgsqlDbMock());
```

## Running Tests

```bash
dotnet test src/DbSqlLikeMem.slnx
```

## Publicando no NuGet (nuget.org)

### Publicação via GitHub Actions (recomendado)

1. Crie uma API key em https://www.nuget.org/ (Account settings → API Keys).
2. No repositório do GitHub, adicione o segredo `NUGET_API_KEY` com essa chave.
3. Atualize a versão em `src/Directory.Build.props` (propriedade `Version`).
4. Crie e envie uma tag de release, por exemplo:

```bash
git tag v0.1.0
git push origin v0.1.0
```

O workflow `.github/workflows/nuget-publish.yml` empacota e publica todos os projetos do solution no nuget.org. 

### Publicação manual (local)

```bash
dotnet pack src/DbSqlLikeMem.slnx -c Release -o ./artifacts
dotnet nuget push "./artifacts/*.nupkg" --api-key "<SUA_API_KEY>" --source "https://api.nuget.org/v3/index.json"
```


## Publicando a extensão do Visual Studio (VSIX)

Quando o projeto VSIX estiver maduro, o repositório já fica preparado para publicar no Marketplace do Visual Studio com o workflow:

- `.github/workflows/vsix-publish.yml`

### Pré-requisitos

1. Criar um Personal Access Token para publicação no Marketplace do Visual Studio.
2. Salvar o token no GitHub como secret `VS_MARKETPLACE_TOKEN`.
3. Ajustar os placeholders em `eng/visualstudio/PublishManifest.json` (`publisher`, `repo`, `identity.internalName`, etc.).
4. Garantir que exista um projeto VSIX (por padrão o workflow usa `src/DbSqlLikeMem.VisualStudioExtension/DbSqlLikeMem.VisualStudioExtension.csproj`).

### Como publicar

- **Manual (recomendado para validação):**
  - Execute o workflow **Publish Visual Studio Extension (VSIX)** via `workflow_dispatch`.
  - Defina `publish = true` para realmente publicar.
- **Automático por tag:**
  - Crie uma tag no formato `vsix-v*` (ex.: `vsix-v1.0.0`).

O pipeline compila o projeto VSIX, gera artifact com o `.vsix` e, quando habilitado, executa o `VsixPublisher.exe` para publicação.


## Publicando a extensão do VS Code (Marketplace)

A extensão em `src/DbSqlLikeMem.VsCodeExtension` também está preparada para empacotamento/publicação no Marketplace do VS Code.

- Workflow: `.github/workflows/vscode-extension-publish.yml`
- Secret necessário: `VSCE_PAT`
- Tag para publicação automática: `vscode-v*`

Publicação manual local:

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm install
npm run compile
npm run package
# ou publicação direta
npm run publish
```

> Antes de publicar, ajuste no `package.json` os placeholders de URL (`repository`, `bugs`, `homepage`) e confirme o `publisher` final.

## Contributing

Contributions are welcome! If you want to help improve DbSqlLikeMem, please open an issue to discuss your idea or submit a pull request. Areas where help is especially valuable:

- Expanding SQL compatibility for each dialect
- Adding more examples or documentation
- Improving performance and error diagnostics
- Increasing test coverage

Thank you for helping the project evolve.
