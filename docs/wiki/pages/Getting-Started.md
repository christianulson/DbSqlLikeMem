# Getting Started

## Installation

### Project reference

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

### Build the solution

```bash
dotnet build src/DbSqlLikeMem.slnx
```

## Runtime provider selection

Use a factory to choose `MySql`, `SqlServer`, `Oracle`, or `Npgsql` dynamically.

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

#### Build da solution

```bash
dotnet build src/DbSqlLikeMem.slnx
```

### Seleção de provider em runtime

Use uma factory para escolher `MySql`, `SqlServer`, `Oracle` ou `Npgsql` dinamicamente.
