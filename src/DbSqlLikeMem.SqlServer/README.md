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

## Contribua

Ajude a ampliar compatibilidade T-SQL com cenários reais, testes e documentação. Toda contribuição conta para tornar o pacote mais útil para a comunidade .NET.
