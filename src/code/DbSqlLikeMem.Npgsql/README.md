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

## Sequence syntax

Use PostgreSQL-style sequence functions in the mock:

```sql
SELECT nextval('sales.seq_orders');
SELECT currval('sales.seq_orders');
SELECT setval('sales.seq_orders', 30, false);
SELECT lastval();
```

See `docs/getting-started.md` for end-to-end setup examples.

## Como ajudar

A evolução de compatibilidade PostgreSQL depende de casos reais. Abra issues com exemplos e participe com PRs para cobrir novos comandos e comportamentos.
