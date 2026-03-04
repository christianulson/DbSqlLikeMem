# DbSqlLikeMem

A base do ecossistema **DbSqlLikeMem**: um motor SQL-like em memória para você testar acesso a dados em C# com mais velocidade, previsibilidade e confiança.

## O que este pacote entrega

- Estruturas de banco em memória (schema, tabelas, colunas, índices)
- Parser e executor SQL para cenários comuns de testes
- Helpers de seed e construção de dados
- Integração amigável com ADO.NET/Dapper
- Plano de execução mock com métricas de runtime e histórico por conexão

## Quando usar

Use `DbSqlLikeMem` quando quiser:

- reduzir custo de testes que hoje dependem de banco real
- criar cenários de QA de forma reproduzível
- validar regras de query e transformação de dados com ciclo rápido
- investigar custo/impacto de queries em testes através de métricas simplificadas do plano

## Instalação

```bash
dotnet add package DbSqlLikeMem
```

## Próximo passo

Escolha também um pacote de provedor (MySql, SqlServer, SqlAzure, Oracle, Npgsql, Sqlite ou Db2) para simular o dialeto do seu sistema.


## Factory rápida para testes

Para facilitar o uso no dia a dia, você pode criar `DbMock` + `IDbConnection` com uma chamada única:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateSqliteWithTables(
    d => d.AddTable("Users",
        [new Col("Id", DataTypeDef.Int32()), new Col("Name", DataTypeDef.String())],
        [new Dictionary<int, object?> { [0] = 1, [1] = "Ana" }]));
```

Também existem atalhos por banco: `CreateOracleWithTables`, `CreateSqlServerWithTables`, `CreateSqlAzureWithTables`, `CreateMySqlWithTables`, `CreateSqliteWithTables`, `CreateDb2WithTables` e `CreateNpgsqlWithTables`.

Se preferir, use a versão genérica por string:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateWithTables("SqlServer", d => { /* mapeamentos */ });
```

Para SQL Azure, os aliases `SqlAzure`, `sqlazure`, `AzureSql`, `azure-sql` e `azure_sql` também são aceitos no `CreateWithTables(...)`.

> Dica: a factory resolve a conexão automaticamente via reflexão, então ela funciona melhor quando o pacote do provedor já está referenciado e carregado no seu projeto de teste.

## Contribuindo

Contribuições são super bem-vindas 💙

- Abra issues com exemplos reais de SQL
- Envie PR com testes cobrindo o comportamento esperado
- Ajude a melhorar documentação e exemplos para novos usuários
