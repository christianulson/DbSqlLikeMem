# Percentual de Fidelidade por Provider

> Valores estimados atuais. Atualizar conforme as fases do plano avancem e os testes de fidelidade forem medidos.

## SQLite

- Percentual atual: 98%
- Pontos garantidos:
  - `SqliteDbMock` e `SqliteConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para parâmetros, funções e capacidades.
  - Comparação de rowset via snapshot quando o contrato for relacional.
  - Falhas explícitas para recursos que o provider real não suporta.
  - Não normalizar input, output ou reader dentro do teste.

## SQL Server

- Percentual atual: 99%
- Pontos garantidos:
  - `SqlServerDbMock` e `SqlServerConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para `APPLY`, parâmetros e transações.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para sintaxe ou recurso não suportado.
  - Não normalizar input, output ou reader dentro do teste.

## SQL Azure

- Percentual atual: 99%
- Pontos garantidos:
  - `SqlAzureDbMock` e `SqlAzureConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` alinhadas ao comportamento do provider.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para recursos não suportados.
  - Não normalizar input, output ou reader dentro do teste.

## MySQL

- Percentual atual: 100%
- Pontos garantidos:
  - `MySqlDbMock` e `MySqlConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para binding e SQL específico.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para diferenças de sintaxe e semântica.
  - Não normalizar input, output ou reader dentro do teste.

## MariaDB

- Percentual atual: 100%
- Pontos garantidos:
  - `MariaDbDbMock` e `MariaDbConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para SQL específico e capacidades.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para diferenças de sintaxe e semântica.
  - Não normalizar input, output ou reader dentro do teste.

## Npgsql

- Percentual atual: 100%
- Pontos garantidos:
  - `NpgsqlDbMock` e `NpgsqlConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para `LATERAL`, temporais e `DateTimeOffset`.
  - Teste dedicado para `ALTER SEQUENCE ... RESTART WITH` na suíte de fidelidade.
  - Teste dedicado para `currval` e `lastval` após restart de sequence.
  - Teste dedicado para `setval(..., false)` e `lastval` estável.
  - Teste dedicado para `currval` e `lastval` locais à sessão em duas conexões.
  - Teste dedicado para sequence qualificada por schema.
  - Teste dedicado para `DROP SEQUENCE` transacional com rollback.
  - Teste dedicado para `CREATE SEQUENCE IF NOT EXISTS` idempotente.
- Rowset completo quando o contrato for relacional.
  - Falhas explícitas para recursos não suportados.
  - Não normalizar input, output ou reader dentro do teste.

## Oracle

- Percentual atual: 99%
- Pontos garantidos:
  - `OracleDbMock` e `OracleConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para normalização de parâmetros e SQL específico.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para recursos não suportados.
  - Não normalizar input, output ou reader dentro do teste.

## Db2

- Percentual atual: 95%
- Pontos garantidos:
  - `Db2DbMock` e `Db2ConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para binding, normalização e SQL específico.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para recursos não suportados.
  - Não normalizar input, output ou reader dentro do teste.

## Firebird

- Percentual atual: 99%
- Pontos garantidos:
  - `FirebirdDbMock` e `FirebirdConnectionMock` resolvidos corretamente.
  - `DbMockConnectionFactory` valida o `DbParameter` concreto do provider.
  - Regras de `Dialect` para binding, normalização e SQL específico.
  - Rowset completo quando o contrato for relacional.
  - Falhas explícitas para recursos não suportados.
  - Não normalizar input, output ou reader dentro do teste.
