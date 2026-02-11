# Getting Started

## Instalação

### Referência de projeto

```xml
<ItemGroup>
  <ProjectReference Include="../DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
</ItemGroup>
```

### Build da solution

```bash
dotnet build src/DbSqlLikeMem.slnx
```

## Seleção de provider em runtime

Use uma factory para escolher `MySql`, `SqlServer`, `Oracle` ou `Npgsql` dinamicamente.

## Testes

```bash
dotnet test src/DbSqlLikeMem.slnx
```
