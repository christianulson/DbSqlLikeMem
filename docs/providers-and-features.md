# Provedores, versões e compatibilidade SQL

> Este arquivo centraliza a matriz de compatibilidade por banco e as capacidades mais relevantes do parser/executor.

## Visão geral

| Banco | Projeto | Versões simuladas |
| --- | --- | --- |
| MySQL | `DbSqlLikeMem.MySql` | 3, 4, 5, 8 |
| SQL Server | `DbSqlLikeMem.SqlServer` | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| Oracle | `DbSqlLikeMem.Oracle` | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite (Sqlite) | `DbSqlLikeMem.Sqlite` | 3 |
| DB2 | `DbSqlLikeMem.Db2` | 8, 9, 10, 11 |

## Capacidades comuns (todos os providers)

- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- Dialeto com diferenças por banco (parser e compatibilidade).
- Expressões `WHERE` (`AND`/`OR`, `IN`, `LIKE`, `IS NULL`, parâmetros).
- `GROUP BY`/`HAVING` com agregações (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) e suporte a aliases no `HAVING`.
- `HAVING` por ordinal em caminhos agrupados (ex.: `HAVING 2 > 0`), incluindo reescrita em expressões `CASE`/`BETWEEN`/`IN` e validação de ordinal inválido.
- `CREATE VIEW` / `CREATE OR REPLACE VIEW`.
- `CREATE TEMPORARY TABLE` (incluindo variantes `AS SELECT`).
- Definição de schema via API fluente.
- Seed de dados e consultas compatíveis com Dapper.
- Suíte de contrato compartilhada para Dapper (`DapperSupportTestsBase`) herdada por MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2 para validar agregações com `CASE WHEN` (incluindo `HAVING` parametrizado), composição e variações de `LIKE` parametrizado, filtros compostos (`IS NULL` + `IN` + `OR`, incluindo parâmetro nulo), subquery escalar com múltiplas linhas internas (comportamento mock atual) e sem correspondência (nulo), sequência transacional (`INSERT`/`UPDATE`/`DELETE`) com rollback+commit e leitura transacional read-after-write, além de paginação determinística em páginas consecutivas com repetibilidade entre execuções.
- Compatibilidade NHibernate via `UserSuppliedConnectionProvider` com suíte de contrato por provider usando dialeto NHibernate específico por banco. Cobertura atual: SQL nativo com parâmetros, save/get/update/delete de entidade mapeada, rollback transacional, paginação (`FirstResult`/`MaxResults`), consulta HQL e Criteria simples, além de parâmetros nulos e tipos básicos (`string`/`int`/`datetime`/`decimal`) com validação de binding em `INSERT` e `WHERE`, e concorrência otimista com entidade versionada, além de relacionamento many-to-one/one-to-many mapeado com consulta HQL de associação e agregação por relacionamento.
- Inicialização de integração EF Core por provider via fábricas de conexão abertas (`DbSqlLikeMem.<Provider>.EfCore`), cobrindo MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2 para uso com providers relacionais do EF Core; com suíte de contrato EF Core segmentada por provider (`DbSqlLikeMem.<Provider>.EfCore.Test`) para validar conexão aberta, fluxo SQL parametrizado, commit/rollback transacional (incluindo consistência em múltiplos comandos no mesmo escopo, sequência `INSERT/UPDATE/DELETE` com rollback+commit e restauração integral no rollback), mutações (`UPDATE`/`DELETE`), binding de parâmetros nulos/tipados (`decimal`/`datetime`), escalares de agregação (`SUM`/`MAX`) e agregações com `CASE WHEN` multirramos com `ELSE` (inclusive cenários agrupados), consultas com `IN`/`ORDER BY`, `INNER JOIN`, `EXISTS`, `LEFT JOIN ... IS NULL`, `LIKE` parametrizado com curingas (`%`, `_`, prefix/suffix), incluindo composição por `OR`, filtros compostos com `IS NULL` + `IN` + `OR` (incluindo parâmetro nulo, estabilidade com ordem invertida dos parâmetros de `IN` e validação de precedência SQL com/sem parênteses), subquery escalar em `SELECT` (incluindo retorno nulo sem correspondência, mistura de linhas nulas/não nulas no mesmo resultset e comportamento simplificado para múltiplas linhas internas por linha externa), leituras transacionais read-after-write no mesmo escopo e validação intermediária em sequência `INSERT/UPDATE/DELETE` antes de rollback e após commit posterior, além de paginação `OFFSET/FETCH` com ordenação estável por critério determinístico, estabilidade em execuções repetidas da mesma janela e repetibilidade entre páginas múltiplas (1, 2, 3 e cauda remanescente) sem sobreposição/lacunas inesperadas.
- Inicialização de integração LinqToDB por provider via fábricas de conexão abertas (`DbSqlLikeMem.<Provider>.LinqToDb`), cobrindo MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2; com suíte de contrato LinqToDB segmentada por provider (`DbSqlLikeMem.<Provider>.LinqToDb.Test`) para validar conexão aberta, fluxo SQL parametrizado, commit/rollback transacional (incluindo consistência em múltiplos comandos no mesmo escopo, sequência `INSERT/UPDATE/DELETE` com rollback+commit e restauração integral no rollback), mutações (`UPDATE`/`DELETE`), binding de parâmetros nulos/tipados (`decimal`/`datetime`), escalares de agregação (`SUM`/`MAX`) e agregações com `CASE WHEN` multirramos com `ELSE` (inclusive cenários agrupados), consultas com `IN`/`ORDER BY`, `INNER JOIN`, `EXISTS`, `LEFT JOIN ... IS NULL`, `LIKE` parametrizado com curingas (`%`, `_`, prefix/suffix), incluindo composição por `OR`, filtros compostos com `IS NULL` + `IN` + `OR` (incluindo parâmetro nulo, estabilidade com ordem invertida dos parâmetros de `IN` e validação de precedência SQL com/sem parênteses), subquery escalar em `SELECT` (incluindo retorno nulo sem correspondência, mistura de linhas nulas/não nulas no mesmo resultset e comportamento simplificado para múltiplas linhas internas por linha externa), leituras transacionais read-after-write no mesmo escopo e validação intermediária em sequência `INSERT/UPDATE/DELETE` antes de rollback e após commit posterior, além de paginação `OFFSET/FETCH` com ordenação estável por critério determinístico, estabilidade em execuções repetidas da mesma janela e repetibilidade entre páginas múltiplas (1, 2, 3 e cauda remanescente) sem sobreposição/lacunas inesperadas.
- Plano de execução mock para consultas AST (`SELECT`/`UNION`) com histórico por conexão.

## Plano de execução mock e métricas para usuário final

O executor AST registra um plano textual por consulta para facilitar troubleshooting e telemetria de testes.

Métricas disponíveis no plano:

- `EstimatedCost`: custo heurístico simplificado baseado na forma da query.
- `InputTables`: quantidade de fontes físicas conhecidas no plano (`FROM` + `JOIN`).
- `EstimatedRowsRead`: soma de linhas estimadas lidas nas fontes físicas conhecidas.
- `ActualRows`: linhas efetivamente retornadas.
- `SelectivityPct`: relação entre linhas retornadas e linhas estimadas lidas.
- `RowsPerMs`: throughput simplificado do resultado (`ActualRows / ElapsedMs`).
- `ElapsedMs`: tempo total medido no executor.

APIs principais para consumo:

- `DbConnectionMockBase.LastExecutionPlan`: último plano gerado.
- `DbConnectionMockBase.LastExecutionPlans`: planos da última execução do comando (inclui multi-select).
- `TableResultMock.ExecutionPlan`: plano associado ao resultado.

Notas:

- As métricas são intencionalmente simplificadas e determinísticas para uso em teste.
- O valor de `EstimatedRowsRead` considera apenas fontes físicas conhecidas; subqueries/derivações podem não entrar na estimativa.

## Particularidades por banco

### MySQL
- `INSERT ... ON DUPLICATE KEY UPDATE`: suportado.
- `USE/IGNORE/FORCE INDEX`: parser + semântica inicial no executor para seleção de índice em predicados de igualdade.
  - `FOR JOIN` e sem escopo: afetam candidatos de índice no plano de acesso.
  - `FOR ORDER BY` / `FOR GROUP BY`: comportamento mínimo inicial (parseados, sem otimização dedicada de sort/group no executor).
  - `FORCE INDEX` em escopos `FOR ORDER BY` / `FOR GROUP BY` valida existência de índices quando a query usa a cláusula correspondente (`ORDER BY` / `GROUP BY`), com fail-fast para índice inexistente.
  - Em `FOR ORDER BY` / `FOR GROUP BY`, quando o índice hint existe, o plano de acesso a linhas permanece no modo mínimo atual (sem otimização dedicada de ordenação/agrupamento).

### SQLite
- `WITH`/CTE: disponível (>= 3).
- `ON DUPLICATE KEY UPDATE`: não suportado (SQLite usa `ON CONFLICT`).
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: suportados pelo parser do dialeto.

### DB2
- `WITH`/CTE: disponível (>= 8).
- `MERGE`: disponível (>= 9).
- `FETCH FIRST`: suportado.
- `LIMIT/OFFSET`: não suportado pelo dialeto DB2.
- `ON DUPLICATE KEY UPDATE`: não suportado.
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: não suportados.

### Regras padronizadas de collation e coerção implícita (mock)

Para reduzir ambiguidades entre dialetos e manter testes determinísticos (`Typing_ImplicitCasts_And_Collation*` e `Collation_CaseSensitivity*`), o projeto adota regras explícitas no executor em memória.

#### 1) Comparação textual (`=`, `<>`, `IN`, `CASE`, fallback textual em `ORDER BY`)

| Provider | Regra no mock |
| --- | --- |
| MySQL | `StringComparison.OrdinalIgnoreCase` |
| SQL Server | `StringComparison.OrdinalIgnoreCase` |
| Oracle | `StringComparison.OrdinalIgnoreCase` |
| PostgreSQL (Npgsql) | `StringComparison.OrdinalIgnoreCase` |
| SQLite | `StringComparison.OrdinalIgnoreCase` |
| DB2 | `StringComparison.OrdinalIgnoreCase` |

> Observação: em bancos reais, esse comportamento depende de collation da instância/database/coluna. No mock, a regra acima é fixa por provider para garantir previsibilidade de teste.

#### 2) `LIKE`

| Provider | Regra no mock |
| --- | --- |
| MySQL | case-insensitive por padrão |
| SQL Server | case-insensitive por padrão |
| Oracle | case-insensitive por padrão |
| PostgreSQL (Npgsql) | case-insensitive por padrão |
| SQLite | case-insensitive por padrão |
| DB2 | case-insensitive por padrão |

#### 3) Coerção implícita número vs string

- A coerção implícita só ocorre quando **ambos os lados** podem ser convertidos para número (`decimal`) com `CultureInfo.InvariantCulture`.
- Exemplo suportado: `id = '2'`.
- Exemplo sem coerção numérica (cai para comparação textual): `id = '2x'`.
- A regra vale para operadores de comparação (`=`, `<>`, `>`, `>=`, `<`, `<=`) e para os caminhos internos de ordenação/comparação usados pelo executor AST.

## Fase 3 — recursos analíticos (todos os providers)

Decisões de compatibilidade implementadas para cobrir os cenários de relatório mais comuns:

- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`: habilitado no executor AST para todos os dialetos que passam por `AstQueryExecutorBase`, incluindo SQLite e DB2.
- Subquery correlacionada em `SELECT` list: avaliada como subconsulta escalar com acesso ao `outer row` (primeira célula da primeira linha; `null` se vazio).
- `CAST` string->número (casos básicos): suporte para `SIGNED`/`UNSIGNED`/`INT*` e `DECIMAL`/`NUMERIC` com parsing `InvariantCulture` e fallback previsível (`0`/`0m` em `CAST`, `null` em `TRY_CAST`).
- Operações de data com regra explícita por dialeto (`SupportsDateAddFunction`):
  - **MySQL**: `DATE_ADD` e `TIMESTAMPADD`.
  - **SQLite**: `DATE_ADD` (além de `DATE(...)`/`DATETIME(...)` com modificadores simples como `'+1 day'`).
  - **SQL Server**: `DATEADD`.
  - **DB2**: `DATE_ADD` e `TIMESTAMPADD`.
  - **PostgreSQL / Oracle**: sem função `DATE_ADD/DATEADD/TIMESTAMPADD` no mock; o caminho suportado é aritmética com `INTERVAL` (ex.: `created + INTERVAL ...`).

### Limitações conhecidas (próxima fase)

- Window functions além de `ROW_NUMBER` (ex.: `RANK`, `DENSE_RANK`, `LAG`, frames `ROWS/RANGE`) ainda não foram implementadas.
- `CAST` numérico ainda não cobre formatações locais complexas, notação científica avançada e tipos de alta precisão específicos por provedor.
- Data/time cobre unidades comuns (`year/month/day/hour/minute/second`), mas não trata timezone explícito, calendário ISO avançado nem regras específicas de cada engine real.
- Subquery escalar retorna sempre a primeira célula da primeira linha, sem erro para múltiplas linhas (comportamento simplificado de mock).


## Triggers em tabelas não temporárias

- O runtime em memória executa triggers registradas via `TableMock.AddTrigger(...)` para eventos `Before/After Insert|Update|Delete`.
- A execução ocorre apenas quando o dialeto expõe `SupportsTriggers = true`.
- **Tabelas temporárias** (escopo conexão/global temporary) **não** executam triggers no executor, por regra explícita de compatibilidade.

## Regras candidatas para extrair do parser para os Dialects

Para deixar o parser mais fiel por banco/versão, estas regras costumam dar bom ganho quando saem de `if` no parser e passam a ser capacidade do dialeto:

- **CTE recursiva e sintaxe de materialização**
  - Flags separadas para `WITH RECURSIVE`, `MATERIALIZED` e `NOT MATERIALIZED`.
- **UPSERT por dialeto**
  - Distinguir `ON DUPLICATE KEY UPDATE` (MySQL), `ON CONFLICT` (PostgreSQL/SQLite) e `MERGE` (SQL Server/Oracle/DB2, por versão).
- **Semântica de paginação por versão**
  - Diferenciar `LIMIT ... OFFSET`, `OFFSET ... FETCH`, `FETCH FIRST ... ROWS ONLY`.
- **Hints de tabela/query**
  - Controle por dialeto para `WITH (NOLOCK)`, `OPTION(...)`, `/*+ hint */`, `STRAIGHT_JOIN`.
- **`RETURNING` / `OUTPUT` / `RETURNING INTO`**
  - Tratar famílias distintas por banco.
- **Tipos e literais específicos**
  - Regras para cast, literais binários/hex e booleanos.
- **`DELETE`/`UPDATE` multi-tabela**
  - Capacidades distintas por banco/versão.
- **JSON e operadores especializados**
  - Separar suporte a `->`, `->>`, `#>`, `#>>`, `JSON_EXTRACT`, `JSON_VALUE`, `OPENJSON`.
- **Conflitos de palavras reservadas por versão**
  - Lista de keywords versionada no dialeto.
- **Colação, `NULLS FIRST/LAST` e ordenação**
  - Regras por dialeto/versão.

### Heurística prática

Se a diferença altera **validade sintática** ou **interpretação semântica**, ela deve viver no dialeto (idealmente com flag/version gate), e o parser apenas consome essas capacidades.

## Links relacionados

- [Começando rápido](getting-started.md)
- [Publicação](publishing.md)
- [Matriz SQL (feature x dialeto)](sql-compatibility-matrix.md)
- [Checklist de known gaps](known-gaps-checklist.md)
- [Wiki do GitHub](wiki/README.md)

## P7–P10 — estado consolidado por provider

### P7 — DML avançado

- **UPSERT**:
  - MySQL: `ON DUPLICATE KEY UPDATE`.
  - PostgreSQL / SQLite: `ON CONFLICT ... DO UPDATE|DO NOTHING`.
  - SQL Server / Oracle / DB2: `MERGE` (subset seguro de parser + execução mock).
- **UPDATE/DELETE com JOIN/subquery**: aceitos em subset comum do parser e avaliados pelo executor/strategies por dialeto.
- **RETURNING/OUTPUT/RETURNING INTO**:
  - PostgreSQL: `RETURNING` aceito no parser para `INSERT ... ON CONFLICT` (consumo sintático mínimo).
  - Demais providers: sem materialização de payload no executor; mensagens de não suportado seguem padrão `SqlUnsupported` quando aplicável.

### P8 — paginação e ordenação por versão

- `LIMIT/OFFSET`: MySQL, PostgreSQL e SQLite.
- `OFFSET ... FETCH`: SQL Server (>= 2012), Oracle (>= 12), PostgreSQL.
- `FETCH FIRST/NEXT`: Oracle (>= 12), PostgreSQL e DB2.
- `TOP`: SQL Server.
- `ORDER BY ... NULLS FIRST/LAST` (gate de dialeto): PostgreSQL, Oracle e SQLite.

### P9 — JSON por provider

- PostgreSQL: operadores `->`, `->>`, `#>`, `#>>`.
- MySQL / SQLite: `JSON_EXTRACT` (aliases via parser expression/function handling).
- SQL Server / Oracle: `JSON_VALUE` (subset escalar no executor).
- SQL Server: `OPENJSON` (subset mínimo, retorno escalar mock).
- DB2: fallback explícito de **não suportado** para operadores/funções JSON avançadas.

### P10 — procedures e parâmetros de saída

- Execução de `CommandType.StoredProcedure` compatível com fluxo de validação de assinatura.
- Suporte a `Input`, `Output`, `InputOutput` e `ReturnValue` (status default `0` quando não preenchido).
- Validação explícita de direção de parâmetros obrigatórios (`IN` exige Input/InputOutput).
- Fluxo compatível com uso via Dapper (`Execute` com `commandType: StoredProcedure`).


### P11 — confiabilidade transacional e concorrência

| Provider | Savepoint | Release savepoint | Isolation (mock simplificado) |
| --- | --- | --- | --- |
| MySQL | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ✅ | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQL Server | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ❌ não suportado (mensagem explícita) | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |
| Oracle | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ✅ | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |
| PostgreSQL (Npgsql) | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ✅ | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |
| SQLite (Sqlite) | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ✅ | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |
| DB2 | ✅ `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` | ✅ | ✅ `ReadCommitted`, `RepeatableRead`, `Serializable` |

Notas do modelo simplificado (determinístico):
- O mock mantém snapshots por savepoint para garantir rollback intermediário consistente em múltiplos comandos DML.
- `Commit` descarta snapshots ativos; `Rollback` restaura o snapshot inicial da transação.
- Operações concorrentes continuam protegidas por `Db.SyncRoot` quando `ThreadSafe = true`.

### Limitações conhecidas (P7–P10)

- `RETURNING`/`OUTPUT` ainda não materializa conjunto retornado completo no executor para todos os dialetos.
- `OPENJSON` está em modo simplificado (subset escalar), sem projeção tabular completa com `WITH (...)`.
- `NULLS FIRST/LAST` depende de gate por dialeto; quando não suportado, erro de não suportado é intencional.
