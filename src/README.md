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

var db = new MySqlDbMock(version: 80);
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

Core and provider packages follow the central production targets from `src/Directory.Build.props`: `net462`, `netstandard2.0`, and `net8.0`.
Test and test-tools projects use the dedicated override target set: `net462`, `net6.0`, and `net8.0`.

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

MySQL documentation uses dotted versions such as `8.0` and `8.4`, while the provider mock API uses integer values such as `80` and `84`.
A documentação de MySQL usa versões com ponto, como `8.0` e `8.4`, enquanto a API do provider mock usa valores inteiros, como `80` e `84`.

```csharp
using DbSqlLikeMem.MySql;

var db = new MySqlDbMock(version: 80);
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

Os pacotes core e de provedores seguem os alvos de produção centrais de `src/Directory.Build.props`: `net462`, `netstandard2.0` e `net8.0`.
Os projetos de teste e test-tools usam o conjunto de override dedicado: `net462`, `net6.0` e `net8.0`.

### Documentação e contribuição

- Documentação principal: `README.md` (raiz do projeto)
- Guia de início: `docs/getting-started.md`
- Notas de compatibilidade: `docs/old/providers-and-features.md`

Contribuições são bem-vindas por meio de issues e pull requests.
