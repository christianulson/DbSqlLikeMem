# DbSqlLikeMem.SqlAzure

Leve cenários de **SQL Azure** para memória com **`DbSqlLikeMem.SqlAzure`**.

Ideal para testes unitários e de integração que precisam manter semântica de compatibilidade SQL Azure sem subir infraestrutura externa.

## Instalação

```bash
dotnet add package DbSqlLikeMem.SqlAzure
```

## O que este pacote entrega

- `SqlAzureDbMock` com níveis de compatibilidade SQL Azure (`SqlAzureDbCompatibilityLevels`/`SqlAzureDbVersions`)
- Mocks ADO.NET (`SqlAzureConnectionMock`, `SqlAzureCommandMock`, `SqlAzureDataAdapterMock`, `SqlAzureConnectorFactoryMock`)
- Integração com `DbDataSource` e `DbBatch` (quando disponível por TFM)
- Extensões DI: `AddSqlAzureDbMockSingleton` e `AddSqlAzureDbMockScoped`
- Superfície compatível com semântica SQL Server no mock atual (ex.: `TOP`, `OUTPUT`, `@@ROWCOUNT`, table hints)

## Alias de provider

Ao selecionar provider por string/factory dinâmica, use:

- `SqlAzure` (canônico)
- `sqlazure`
- `AzureSql`
- `azure-sql`
- `azure_sql`

## Níveis de compatibilidade suportados

- `100` (`SqlServer2008`)
- `110` (`SqlServer2012`)
- `120` (`SqlServer2014`)
- `130` (`SqlServer2016`)
- `140` (`SqlServer2017`)
- `150` (`SqlServer2019`)
- `160` (`SqlServer2022`) - padrão
- `170` (`SqlServer2025`)

## Exemplo rapido

```csharp
using DbSqlLikeMem.SqlAzure;

var db = new SqlAzureDbMock(SqlAzureDbCompatibilityLevels.SqlServer2022);
using var connection = new SqlAzureConnectionMock(db);
connection.Open();
```
