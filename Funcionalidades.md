# Funcionalidades por Banco e Versão

> Looking for English? See [Features.md](Features.md).

> Este arquivo é mantido para navegação rápida no NuGet/GitHub. A versão canônica e completa está em [`docs/old/providers-and-features.md`](docs/old/providers-and-features.md).

## Links rápidos

- [Versão em inglês](Features.md)
- [Documentação canônica de provedores e features](docs/old/providers-and-features.md)
- [Guia de início rápido](docs/getting-started.md)
- [Visão geral do repositório](README.md)

## Matriz de provedores e versões simuladas

| Banco | Pacote | Versões simuladas |
| --- | --- | --- |
| MySQL | `DbSqlLikeMem.MySql` | 3, 4, 5, 8 |
| SQL Server | `DbSqlLikeMem.SqlServer` | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| SQL Azure | `DbSqlLikeMem.SqlAzure` | 100, 110, 120, 130, 140, 150, 160, 170 |
| Oracle | `DbSqlLikeMem.Oracle` | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite | `DbSqlLikeMem.Sqlite` | 3 |
| DB2 | `DbSqlLikeMem.Db2` | 8, 9, 10, 11 |

## Capacidades comuns (todos os providers)

- Mocks ADO.NET de conexão/comando/transação por provedor.
- Parser e executor SQL para fluxos DDL/DML comuns.
- Expressões `WHERE` com `AND`/`OR`, `IN`, `LIKE`, `IS NULL` e parâmetros.
- `GROUP BY`/`HAVING` com agregações (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) e caminhos com aliases.
- `CASE WHEN` em projeções e cenários agrupados.
- Suporte a `CREATE VIEW` / `CREATE OR REPLACE VIEW`.
- Suporte a `CREATE TEMPORARY TABLE`, incluindo variantes `AS SELECT`.
- Definição fluente de schema e helpers determinísticos de seed.
- Sequences como objetos de schema, com registro via `DbMock`/`DbConnectionMockBase`, extração por generators compatíveis e geração de código equivalente.
- Consumo SQL de sequences em caminhos validados de `INSERT` e `SELECT`, incluindo `NEXT VALUE FOR`, `nextval(...)`, `currval(...)`, `setval(...)`, `lastval()` e nomes qualificados por schema nos providers cobertos.
- Sobrescrita opcional de colunas `identity` em cenários e inserts, preservando o comportamento atual por padrão.
- Regras padronizadas de collation/coerção no mock para comparações textuais e numéricas por string.

## Camadas de integração usadas em stacks reais de teste

- Fluxo compatível com consultas e comandos via Dapper.
- Pacotes de integração EF Core com fábricas de conexão aberta por provedor.
- Pacotes de integração LinqToDB com fábricas de conexão aberta por provedor.
- Compatibilidade NHibernate via `UserSuppliedConnectionProvider` e suítes de contrato por provedor.

## Diagnóstico e telemetria de plano de execução (mock)

- Planos por comando disponíveis em `LastExecutionPlan` e `LastExecutionPlans`.
- Métricas centrais incluem `EstimatedCost`, `InputTables`, `EstimatedRowsRead`, `ActualRows`, `SelectivityPct`, `RowsPerMs` e `ElapsedMs`.
- A saída do plano inclui metadados de alerta e recomendação para troubleshooting de testes (por exemplo, códigos de warning e recomendações de índice).

## Recursos SQL analíticos (já implementados no parser/executor)

- Funções de ranking/distribuição em janela como `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK` e `CUME_DIST`.
- Funções de valor em janela como `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` e `NTH_VALUE`.
- Cláusulas de frame `ROWS`, `RANGE` e `GROUPS` nos caminhos de dialeto suportados.

## Modelo transacional e concorrência (mock determinístico)

| Provider | Savepoint | Release savepoint | Níveis de isolamento |
| --- | --- | --- | --- |
| MySQL | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQL Server | Sim | Não (comportamento explícito de não suportado) | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQL Azure | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| Oracle | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| PostgreSQL (Npgsql) | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQLite | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |
| DB2 | Sim | Sim | `ReadCommitted`, `RepeatableRead`, `Serializable` |

- Operações de savepoint usam snapshots para rollback intermediário consistente.
- `Commit` e `Rollback` seguem comportamento determinístico de limpeza/restauração de snapshots.
- Operações concorrentes usam proteção por sync-root quando `ThreadSafe = true`.

## Execução de stored procedures (contrato mock)

- Execução de `CommandType.StoredProcedure` com validação de assinatura.
- Direções de parâmetro: `Input`, `Output`, `InputOutput` e `ReturnValue`.
- Validações explícitas para direção obrigatória e nulabilidade de parâmetros de entrada.
- Fluxo compatível com execução de stored procedure via Dapper.

## Destaques de funcionalidades por banco

### MySQL

- `INSERT ... ON DUPLICATE KEY UPDATE`: suportado.
- Index hints (`USE/IGNORE/FORCE INDEX`) parseados, com semântica e validações suportadas no executor.

### SQL Server

- Comportamento de dialeto por versão no pacote do provedor.
- `RELEASE SAVEPOINT` intencionalmente padronizado como não suportado.
- `NEXT VALUE FOR` com sequence registrada no schema: suportado nos fluxos validados de `SELECT` e `INSERT`.

### SQL Azure

- Comportamento de Azure SQL por versão no pacote do provedor.
- Regras de compatibilidade dedicadas para fluxos de comando/transação no SQL Azure.
- `NEXT VALUE FOR` segue a mesma base de compatibilidade do SQL Server no mock atual, incluindo sequences qualificadas por schema.

### Oracle

- Comportamento de dialeto por versão no pacote do provedor.
- Sintaxe Oracle-style `seq.NEXTVAL` e `seq.CURRVAL`: implementada no parser/runtime, incluindo nome qualificado por schema.

### PostgreSQL (Npgsql)

- Comportamento de dialeto por versão no pacote do provedor.
- `nextval(...)`, `currval(...)`, `setval(...)` e `lastval()` com sequence registrada no schema: suportados nos fluxos validados do provider.

### SQLite

- `WITH`/CTE: disponível (>= 3).
- `ON DUPLICATE KEY UPDATE`: não suportado (SQLite usa `ON CONFLICT`).
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: suportados no parser do dialeto.

### DB2

- `WITH`/CTE: disponível (>= 8).
- `MERGE`: disponível (>= 9).
- `FETCH FIRST`: suportado.
- `NEXT VALUE FOR` e `PREVIOUS VALUE FOR`: implementados no parser/runtime, incluindo names qualificados por schema.
- `LIMIT/OFFSET`: não suportado no dialeto DB2.
- `ON DUPLICATE KEY UPDATE`: não suportado.
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: não suportados.
- Triggers em tabelas não temporárias são suportadas via `TableMock` (before/after insert, update e delete).
- Tabelas temporárias (connection/global) não executam triggers.

## Extensões (VS Code e Visual Studio)

As extensões suportam tanto a geração tradicional de testes quanto fluxos para artefatos de aplicação:

- Gerar classes de teste (ação principal existente).
- Gerar classes de modelos.
- Gerar classes de repositório.
- Configurar templates pelos botões de ação no topo.
- Check de consistência com status visual para artefatos ausentes/divergentes/sincronizados.

### Tokens de template

- `{{ClassName}}`, `{{ObjectName}}`, `{{Schema}}`, `{{ObjectType}}`, `{{DatabaseType}}`, `{{DatabaseName}}`.

## Limitações conhecidas (atuais)

- `RETURNING` / `OUTPUT` ainda não materializam conjuntos retornados completos em todos os dialetos.
- `OPENJSON` está em subset escalar simplificado (sem projeção tabular completa com `WITH (...)`).
- `NULLS FIRST/LAST` segue gate por dialeto e pode lançar erro explícito de não suportado quando indisponível.
