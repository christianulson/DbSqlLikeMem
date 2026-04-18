# Fase 1 - Inventário dos tipos nativos

## Status

DONE

## Percentual de entrega

100%

## O que foi feito

- Inventariados os tipos nativos relevantes por provider.
- Separado o que é tipo próprio do provider do que é herança de outro provider.
- Identificados os pontos de fidelidade que hoje já têm cobertura por `DbMockConnectionFactoryContractTestsBase` e pelos testes de superfície.

## Inventário Por Provider

### SQLite

- Tipos nativos próprios:
  - `SqliteDbMock`
  - `SqliteConnectionMock`
  - `SqliteCommandMock`
  - `SqliteTransactionMock`
  - `SqliteDataReaderMock`
  - `SqliteDataSourceMock`
  - `SqliteDataAdapterMock`
  - `SqliteDataParameterCollectionMock`
  - `SqliteBatchMock`
  - `SqliteBatchCommandMock`
  - `SqliteBatchCommandCollectionMock`
  - `SqliteConnectorFactoryMock`
  - `SqliteMockException`
  - `SqliteSchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `SqliteConnectorFactoryMockTests`
  - `DataSource` e `Batch` também aparecem em `SqliteProviderSurfaceMocksTests`
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### SQL Server

- Tipos nativos próprios:
  - `SqlServerDbMock`
  - `SqlServerConnectionMock`
  - `SqlServerCommandMock`
  - `SqlServerTransactionMock`
  - `SqlServerDataReaderMock`
  - `SqlServerDataSourceMock`
  - `SqlServerDataAdapterMock`
  - `SqlServerDataParameterCollectionMock`
  - `SqlServerBatchMock`
  - `SqlServerBatchCommandMock`
  - `SqlServerBatchCommandCollectionMock`
  - `SqlServerConnectorFactoryMock`
  - `SqlServerMockException`
  - `SqlServerSchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `SqlServerConnectorFactoryMockTests`
  - `DataSource` e `Batch` aparecem nos testes de superfície
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### SQL Azure

- Tipos nativos próprios:
  - `SqlAzureDbMock`
  - `SqlAzureConnectionMock`
  - `SqlAzureCommandMock`
  - `SqlAzureDataReaderMock`
  - `SqlAzureDataSourceMock`
  - `SqlAzureDataAdapterMock`
  - `SqlAzureDataParameterCollectionMock`
  - `SqlAzureBatchMock`
  - `SqlAzureBatchCommandMock`
  - `SqlAzureBatchCommandCollectionMock`
  - `SqlAzureConnectorFactoryMock`
  - `SqlAzureMockException`
  - `SqlAzureSchemaMock`
  - `SqlAzureTableMock`
- Herança relevante:
  - `SqlAzureDbMock` herda `SqlServerDbMock`
  - `SqlAzureConnectionMock` herda `SqlServerConnectionMock`
  - `SqlAzureDataParameterCollectionMock` herda `SqlServerDataParameterCollectionMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `SqlAzureConnectorFactoryMockTests` e `SqlAzureProviderSurfaceMocksTests`
- Lacunas observadas:
  - contrato explícito de `TransactionMock` no provider Azure
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock` próprio do Azure

### MySQL

- Tipos nativos próprios:
  - `MySqlDbMock`
  - `MySqlConnectionMock`
  - `MySqlCommandMock`
  - `MySqlTransactionMock`
  - `MySqlDataReaderMock`
  - `MySqlDataSourceMock`
  - `MySqlDataAdapterMock`
  - `MySqlDataParameterCollectionMock`
  - `MySqlBatchMock`
  - `MySqlBatchCommandMock`
  - `MySqlBatchCommandCollectionMock`
  - `MySqlConnectorFactoryMock`
  - `MySqlMockException`
  - `MySqlSchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `MySqlConnectorFactoryMockTests`
  - `DataSource` e `Connection` também aparecem nos testes de superfície
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### MariaDB

- Tipos nativos próprios:
  - `MariaDbDbMock`
  - `MariaDbConnectionMock`
- Herança relevante:
  - `MariaDbDbMock` herda `MySqlDbMock`
  - `MariaDbConnectionMock` herda `MySqlConnectionMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - regras de alias e roteamento MariaDB em testes dedicados de parser/factory
- Lacunas observadas:
  - contratos explícitos para `Command`, `Transaction`, `DataReader`, `DataSource`, `DataAdapter`, `DataParameterCollection`, `Batch` e `BatchCommand` dependem da herança MySQL e ainda não têm suíte própria de fidelidade MariaDB

### Npgsql

- Tipos nativos próprios:
  - `NpgsqlDbMock`
  - `NpgsqlConnectionMock`
  - `NpgsqlCommandMock`
  - `NpgsqlTransactionMock`
  - `NpgsqlDataReaderMock`
  - `NpgsqlDataSourceMock`
  - `NpgsqlDataAdapterMock`
  - `NpgsqlDataParameterCollectionMock`
  - `NpgsqlBatchMock`
  - `NpgsqlBatchCommandMock`
  - `NpgsqlBatchCommandCollectionMock`
  - `NpgsqlConnectorFactoryMock`
  - `NpgsqlMockException`
  - `NpgsqlSchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `NpgsqlConnectorFactoryMockTests`
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### Oracle

- Tipos nativos próprios:
  - `OracleDbMock`
  - `OracleConnectionMock`
  - `OracleCommandMock`
  - `OracleTransactionMock`
  - `OracleDataReaderMock`
  - `OracleDataSourceMock`
  - `OracleDataAdapterMock`
  - `OracleDataParameterCollectionMock`
  - `OracleBatchMock`
  - `OracleBatchCommandMock`
  - `OracleBatchCommandCollectionMock`
  - `OracleConnectorFactoryMock`
  - `OracleMockException`
  - `OracleSchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `OracleConnectorFactoryMockTests`
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### Db2

- Tipos nativos próprios:
  - `Db2DbMock`
  - `Db2ConnectionMock`
  - `Db2CommandMock`
  - `Db2TransactionMock`
  - `Db2DataReaderMock`
  - `Db2DataSourceMock`
  - `Db2DataAdapterMock`
  - `Db2DataParameterCollectionMock`
  - `Db2BatchMock`
  - `Db2BatchCommandMock`
  - `Db2BatchCommandCollectionMock`
  - `Db2ConnectorFactoryMock`
  - `Db2MockException`
  - `Db2SchemaMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand` e `DataSource` via `Db2ConnectorFactoryMockTests`
- Lacunas observadas:
  - contrato explícito de `TransactionMock`
  - contrato explícito de `DataReaderMock`
  - contrato explícito de `DataParameterCollectionMock`

### Firebird

- Tipos nativos próprios:
  - `FirebirdDbMock`
  - `FirebirdConnectionMock`
  - `FirebirdCommandMock`
  - `FirebirdTransactionMock`
  - `FirebirdDataReaderMock`
  - `FirebirdDataSourceMock`
  - `FirebirdDataAdapterMock`
  - `FirebirdDataParameterCollectionMock`
  - `FirebirdBatchMock`
  - `FirebirdBatchCommandMock`
  - `FirebirdBatchCommandCollectionMock`
  - `FirebirdConnectorFactoryMock`
  - `FirebirdMockException`
  - `FirebirdSchemaMock`
  - `FirebirdTableMock`
- Cobertura atual:
  - `DbMock` e `Connection` via `DbMockConnectionFactoryContractTestsBase`
  - `Command`, `DataAdapter`, `Batch`, `BatchCommand`, `DataSource`, `Connection` e `Transaction` têm testes dedicados de superfície
  - `DataReaderMock` tem teste dedicado próprio
- Lacunas observadas:
  - contrato explícito de `DataParameterCollectionMock`

## Próximos Passos

- Iniciar a Fase 2 com contratos de tipo por provider.
- Priorizar `DbConnection`, `DbCommand`, `DbTransaction`, `DbDataReader` e `DbParameter` nativos.
- Expandir depois para `DataSource`, `DataAdapter`, `Batch`, `BatchCommand` e `BatchCommandCollection`.
