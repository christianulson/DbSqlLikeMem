# DbSqlLikeMem Packages

## English

`DbSqlLikeMem` is an in-memory SQL testing ecosystem for .NET. It helps you test repositories, services, and data-access code without requiring a real database server for each test run.

### Why use it

- Faster and deterministic tests
- No container or external database required for most test scenarios
- SQL dialect coverage across multiple providers
- ADO.NET-friendly behavior, with support layers for common testing stacks (for example, Dapper, EF Core, NHibernate, LinqToDB)

### Main provider packages

- `DbSqlLikeMem` (core)
- `DbSqlLikeMem.MySql`
- `DbSqlLikeMem.SqlServer`
- `DbSqlLikeMem.SqlAzure`
- `DbSqlLikeMem.Oracle`
- `DbSqlLikeMem.Npgsql`
- `DbSqlLikeMem.Sqlite`
- `DbSqlLikeMem.Db2`

### Install

```bash
dotnet add package DbSqlLikeMem
```

Add the provider package that matches your SQL dialect, for example:

```bash
dotnet add package DbSqlLikeMem.SqlServer
```

### Quick usage example

```csharp
using DbSqlLikeMem.MySql;

var db = new MySqlDbMock(version: 8);
var users = db.AddTable("users");
users.AddColumn("Id", DbType.Int32, false);
users.AddColumn("Name", DbType.String, false);
users.AddPrimaryKeyIndexes("id");

using var cnn = new MySqlConnectionMock(db);
cnn.Open();

using var cmd = cnn.CreateCommand();
cmd.CommandText = "INSERT INTO users (Id, Name) VALUES (1, 'Alice')";
cmd.ExecuteNonQuery();
```

### Target frameworks

Core and provider packages target modern .NET and legacy enterprise scenarios, including `net462`, `netstandard2.0`, `net8.0` (with package-specific variations where applicable).

### Documentation and contribution

- Repository docs: `README.md` (project root)
- Getting started: `docs/getting-started.md`
- Compatibility notes: `docs/old/providers-and-features.md`

Contributions are welcome through issues and pull requests.

---

## Português

`DbSqlLikeMem` é um ecossistema de testes SQL em memória para .NET. Ele permite testar repositórios, serviços e código de acesso a dados sem precisar subir um banco real em cada execução de teste.

### Por que usar

- Testes mais rápidos e determinísticos
- Sem necessidade de container ou banco externo na maioria dos cenários
- Cobertura de dialetos SQL em múltiplos provedores
- Comportamento compatível com ADO.NET, com camadas de suporte para stacks comuns de teste (por exemplo, Dapper, EF Core, NHibernate, LinqToDB)

### Pacotes principais de provedor

- `DbSqlLikeMem` (core)
- `DbSqlLikeMem.MySql`
- `DbSqlLikeMem.SqlServer`
- `DbSqlLikeMem.SqlAzure`
- `DbSqlLikeMem.Oracle`
- `DbSqlLikeMem.Npgsql`
- `DbSqlLikeMem.Sqlite`
- `DbSqlLikeMem.Db2`

### Instalação

```bash
dotnet add package DbSqlLikeMem
```

Adicione também o pacote de provedor correspondente ao seu dialeto SQL, por exemplo:

```bash
dotnet add package DbSqlLikeMem.SqlServer
```

### Exemplo rápido de uso

```csharp
using DbSqlLikeMem.MySql;

var db = new MySqlDbMock(version: 8);
var users = db.AddTable("users");
users.AddColumn("Id", DbType.Int32, false);
users.AddColumn("Name", DbType.String, false);
users.AddPrimaryKeyIndexes("id");

using var cnn = new MySqlConnectionMock(db);
cnn.Open();

using var cmd = cnn.CreateCommand();
cmd.CommandText = "INSERT INTO users (Id, Name) VALUES (1, 'Alice')";
cmd.ExecuteNonQuery();
```

### Frameworks alvo

Os pacotes core e de provedores cobrem cenários modernos e legados do .NET, incluindo `net462`, `netstandard2.0`, `net8.0` (com variações específicas por pacote quando aplicável).

### Documentação e contribuição

- Documentação principal: `README.md` (raiz do projeto)
- Guia de início: `docs/getting-started.md`
- Notas de compatibilidade: `docs/old/providers-and-features.md`

Contribuições são bem-vindas por meio de issues e pull requests.
