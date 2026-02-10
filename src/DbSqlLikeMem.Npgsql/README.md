# DbSqlLikeMem.Npgsql

**`DbSqlLikeMem.Npgsql`** oferece um ambiente de teste em memória para aplicações que usam PostgreSQL com Npgsql.

## Principais ganhos

- Menos fricção para testar queries PostgreSQL
- Sem custo de infraestrutura para cada execução
- Maior produtividade para times que usam Dapper/ADO.NET

## Instalação

```bash
dotnet add package DbSqlLikeMem.Npgsql
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.Npgsql;

var conn = new NpgsqlConnectionMock(new NpgsqlDbMock());
conn.Open();
```

## Como ajudar

A evolução de compatibilidade PostgreSQL depende de casos reais. Abra issues com exemplos e participe com PRs para cobrir novos comandos e comportamentos.
