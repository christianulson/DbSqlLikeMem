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

### Referência rápida de sequence

- SQL Server / SQL Azure: `NEXT VALUE FOR schema.seq_name`
- PostgreSQL: `nextval`, `currval`, `setval`, `lastval`
- Oracle: `schema.seq_name.NEXTVAL`, `schema.seq_name.CURRVAL`
- DB2: `NEXT VALUE FOR schema.seq_name`, `PREVIOUS VALUE FOR schema.seq_name`
