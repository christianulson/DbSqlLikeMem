# DbSqlLikeMem.SqlServer

Leve a experiência de testes SQL Server para memória com **`DbSqlLikeMem.SqlServer`**.

Ideal para validar repositórios, serviços e queries sem depender de infraestrutura pesada.

## Por que este pacote?

- Foco em comportamento de SQL Server para testes unitários/integrados
- Mock de conexão, comando, transação e leitura de dados
- Feedback rápido para times que trabalham com T-SQL

## Instalação

```bash
dotnet add package DbSqlLikeMem.SqlServer
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.SqlServer;

var conn = new SqlServerConnectionMock(new SqlServerDbMock());
conn.Open();
```

## Sequence syntax

Use SQL Server-style sequence access in the mock:

```sql
SELECT NEXT VALUE FOR sales.seq_orders;
INSERT INTO sales.orders (id) VALUES (NEXT VALUE FOR sales.seq_orders);
```

See `docs/getting-started.md` for end-to-end setup examples.

## Contribua

Ajude a ampliar compatibilidade T-SQL com cenários reais, testes e documentação. Toda contribuição conta para tornar o pacote mais útil para a comunidade .NET.
