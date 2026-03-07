# Getting Started

## Installation

### Project reference

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

You can switch the provider reference to `DbSqlLikeMem.SqlAzure` when validating Azure SQL compatibility.

### Build the solution

```bash
dotnet build src/DbSqlLikeMem.slnx
```

## Runtime provider selection

Use a factory to choose `MySql`, `SqlServer`, `SqlAzure`, `Oracle`, `Npgsql`, `Sqlite`, or `Db2` dynamically.

## Framework compatibility

Production packages follow the central targets declared in `src/Directory.Build.props`: `net462`, `netstandard2.0`, and `net8.0`.

Test and test-tools projects use the dedicated override: `net462`, `net6.0`, and `net8.0`.

If distribution or versioning is part of the rollout, review `docs/publishing.md` together with the repository guides.

## Sequence quick reference

- SQL Server / SQL Azure: `NEXT VALUE FOR schema.seq_name`
- PostgreSQL: `nextval`, `currval`, `setval`, `lastval`
- Oracle: `schema.seq_name.NEXTVAL`, `schema.seq_name.CURRVAL`
- DB2: `NEXT VALUE FOR schema.seq_name`, `PREVIOUS VALUE FOR schema.seq_name`

## Tests

```bash
dotnet test src/DbSqlLikeMem.slnx
```

---

## Português

### Instalação

#### Referência de projeto

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

Você pode trocar a referência do provider para `DbSqlLikeMem.SqlAzure` ao validar compatibilidade com Azure SQL.

#### Build da solution

```bash
dotnet build src/DbSqlLikeMem.slnx
```

### Seleção de provider em runtime

Use uma factory para escolher `MySql`, `SqlServer`, `SqlAzure`, `Oracle`, `Npgsql`, `Sqlite` ou `Db2` dinamicamente.

### Compatibilidade de frameworks

Os pacotes de produção seguem os alvos centrais declarados em `src/Directory.Build.props`: `net462`, `netstandard2.0` e `net8.0`.

Os projetos de teste e test-tools usam o override dedicado: `net462`, `net6.0` e `net8.0`.

Se distribuição ou versionamento fizerem parte do rollout, revise `docs/publishing.md` junto com os guias principais do repositório.

### Referência rápida de sequence

- SQL Server / SQL Azure: `NEXT VALUE FOR schema.seq_name`
- PostgreSQL: `nextval`, `currval`, `setval`, `lastval`
- Oracle: `schema.seq_name.NEXTVAL`, `schema.seq_name.CURRVAL`
- DB2: `NEXT VALUE FOR schema.seq_name`, `PREVIOUS VALUE FOR schema.seq_name`
