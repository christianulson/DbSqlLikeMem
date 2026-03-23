# Plano de Ataque: Regressao de Performance dos Benchmarks

## Objetivo

Recuperar a velocidade do `DbSqlLikeMem` nos benchmarks principais e usar o `SQLite` como referencia maxima para os caminhos equivalentes de leitura, escrita e setup.

## Leitura das matrizes

- [docs/Wiki/performance-matrix.md](../Wiki/performance-matrix.md) mostra regressao ampla no lado `DbSqlLikeMem` em `core`, `batch`, `query`, `setup` e `transactions`.
- [docs/Wiki/performance-matrix-app-specific.md](../Wiki/performance-matrix-app-specific.md) mostra que `diagnostics`, `temp tables` e `schema snapshots` ainda carregam custo alto o suficiente para contaminar o caminho quente.
- O `SQLite` continua sendo o teto de referencia para as trilhas de suporte e deve servir como baseline de tempo para qualquer caminho comparavel.

## Sinal da regressao

- `Select by PK`, `Select join`, `Insert single row` e `Batch insert 100` pioraram de forma visivel na ultima matriz.
- `Create schema`, `Connection open` e `Row count` tambem subiram, indicando overhead estrutural no setup e no ciclo de vida da sessao.
- Os cenarios de consulta avancada, agregacao e transacao tambem ficaram mais caros, o que sugere regressao no caminho compartilhado e nao apenas em um provider isolado.
- Na matriz app-specific, `Debug trace`, `Execution plan`, `LastExecutionPlansHistory`, `Temp table create/use`, `Temp table isolation` e `Schema snapshot compare` continuam sendo os pontos que mais podem inflar a coleta se entrarem no caminho medido.

## Hipoteses principais

- O benchmark principal esta pagando setup demais dentro do trecho medido.
- Alguns helpers compartilhados ainda fazem trabalho extra que nao deveria entrar no hot path.
- O custo de diagnostico e de suporte ainda pode estar misturando coleta funcional com instrumentacao.
- A regressao atual parece ser mais ampla do que um unico provider, entao a primeira investigacao deve focar em infraestrutura compartilhada de benchmark.

## Progresso atual

- `SelectByPk`, `SelectJoin`, `ExecutionPlan`, `LastExecutionPlansHistory`, `DebugTraceSelect`, `InsertSingle`, `InsertBatch10`, `InsertBatch100`, `InsertBatch100Parallel` e `RowCountAfterInsert` agora reutilizam estados preparados.
- `ExecutionPlanDml` e `DebugTraceBatch` tambem passaram a reutilizar o cenário preparado e a variar apenas os ids de insert.
- `SchemaSnapshot` tambem passou a reutilizar o service preparado além da conexão, reduzindo setup repetido no trecho medido.
- `BatchInsert10` e `BatchInsert100` agora reutilizam os vetores de linhas pre-montados dentro do estado preparado, evitando alocacao repetida no trecho medido.
- `WindowRowNumber` e `WindowLag` agora usam o mesmo caminho preparado de query com seed fixo, eliminando recriacao de conexao e serviço por chamada.
- `UpdateByPk`, `DeleteByPk`, `RowCountAfterUpdate`, `Upsert`, `TransactionCommit`, `TransactionRollback`, `RollbackToSavepoint`, `NestedSavepointFlow`, `SavepointCreate` e `ReleaseSavepoint` agora usam estados preparados dedicados.
- `DateScalar`, `JsonScalarRead`, `TemporalCurrentTimestamp`, `TemporalDateAdd`, `JsonPathRead`, `JsonInsertCast`, `PivotCount` e `SequenceNextValue` agora reutilizam services preparados.
- `ReturningInsert` do MariaDB agora também reutiliza estado preparado e só executa o `RETURNING` no trecho medido.
- `CreateSchema` agora reutiliza conexao e service preparados, removendo setup direto do trecho medido sem alterar o DDL executado.
- A refatoracao de estados preparados voltou a compilar depois do ajuste das overloads de `ExecuteNonQuery`/`ExecuteScalar` e do helper de temp table no benchmark compartilhado.
- Os `CreateConnection()` restantes ficaram restritos aos benchmarks de lifecycle de conexão, que intencionalmente medem o custo de criar, reabrir e resetar a conexão.
- O proximo bloco continua em `BatchMixedReadWrite`, `BatchScalar`, `BatchNonQuery`, `TempTable` e `SchemaSnapshot`, que ainda podem carregar setup ou instrumentacao para o trecho medido.

## Ordem de ataque

### 1. Separar setup do trecho medido

- Auditar `BenchmarkSessionBase`, `UsersScenario`, `UsersOrdersScenario`, `SelectTableScenario`, `TemporaryTableServiceOpsTest` e os helpers de `BenchmarkScenarioFactory`.
- Garantir que o que for setup de dados fique fora do caminho medido sempre que isso nao alterar a fidelidade.
- Revisar qualquer validacao, lookup de tabela ou montagem de SQL que esteja sendo repetida em cada execucao.

### 2. Recuperar os benchmarks de core

- Priorizar `ConnectionOpen`, `InsertSingle`, `SelectByPk`, `SelectJoin`, `UpdateByPk`, `DeleteByPk` e `CreateSchema`.
- Confirmar se o SQL gerado continua o mais direto possivel para cada provider.
- Remover qualquer indirecao ou branch desnecessario do caminho quente.

### 3. Atacar batch e query avancada

- Priorizar `BatchInsert100`, `BatchInsert100Parallel`, `BatchMixedReadWrite`, `RowCountInBatch` e os cenarios de `CTE`, `IN`, `EXISTS`, `scalar subquery`, `JOIN`, `UNION ALL` e `GROUP BY`.
- Verificar se os helpers compartilham seed e schema de forma eficiente.
- Comparar a quantidade de comandos executados antes e depois das mudancas recentes.

### 4. Reduzir overhead de suporte

- Revisar `Diagnostics`, `ExecutionPlan`, `LastExecutionPlansHistory`, `TempTable` e `SchemaSnapshot`.
- Remover console, JSON, reflexao e acessos repetidos em caminhos quentes.
- Tratar `SQLite` como limite superior para custos equivalentes de suporte.

### 5. Fechar a distancia para o SQLite

- Para cada ajuste, comparar o tempo do `DbSqlLikeMem` com o `SQLite` nos cenarios analogos.
- Se um caminho equivalente ficar acima do `SQLite`, ele continua aberto.
- A meta e manter o `DbSqlLikeMem` abaixo do `SQLite` nos pontos em que o compare faz sentido e nao perde fidelidade.

## Foco inicial

1. `SelectByPk` e `SelectJoin`, porque a regressao foi grande e esses cenarios exercitam o caminho mais comum.
2. `InsertSingle` e `BatchInsert100`, porque medem o custo basico de escrita e costumam revelar overhead de setup.
3. `CreateSchema` e `ConnectionOpen`, porque qualquer custo extra aqui afeta todos os demais testes.
4. `TempTable` e `SchemaSnapshot`, porque o custo de suporte pode mascarar o ganho real do motor.
5. `Diagnostics`, porque qualquer instrumentacao que entre no caminho quente atrapalha a leitura da matriz.

## Criterio de sucesso

- A matriz nova volta a mostrar ganho claro do `DbSqlLikeMem` versus a versao anterior.
- Os benchmarks de core deixam de regredir em ordem de grandeza.
- Os cenarios de suporte continuam corretos, mas sem inflar a coleta normal.
- O `SQLite` segue como teto de referencia para qualquer caminho comparavel.

## Fora do escopo

- As flags `RUN_*_CONTAINER_TESTS` sao opcionais e servem apenas para comparacoes de fidelidade; elas nao definem o caminho principal de performance.
- Este plano nao altera a regra de executar a suite completa quando o objetivo e validar regressao geral.
