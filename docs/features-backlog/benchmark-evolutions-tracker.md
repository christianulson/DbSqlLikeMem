# Plano de evolucao dos benchmarks

Este documento acompanha as proximas evolucoes da malha de benchmarks e da matriz de desempenho.

## Objetivo

- Manter a matriz de `docs/Wiki/performance-matrix.md` alinhada com o que o benchmark realmente executa.
- Registrar novos providers, novas familias de dialeto e novos cenarios comparaveis antes de expandir a documentacao de resultados.
- Separar o que e cobertura funcional do que e cobertura de performance, para evitar backlog misturado.

## Escopo atual

- Benchmark principal: `benchmark/DbSqlLikeMem.Benchmarks`.
- Matriz publicada: `docs/Wiki/performance-matrix.md`.
- Catálogo fonte: `benchmark/DbSqlLikeMem.Benchmarks/benchmark-feature-map.json`.
- Providers que devem aparecer no benchmark:
  - MySQL
  - MariaDB
  - SQL Server
  - SQL Azure
  - Oracle
  - PostgreSQL / Npgsql
  - SQLite
  - DB2

## Estado desejado

### Curto prazo

- MariaDB deve aparecer como provider completo no benchmark, com:
  - session in-memory `DbSqlLikeMem`
  - session `Testcontainers`
  - dialeto de benchmark proprio
  - entrada no catalogo de providers
- A matriz deve refletir MariaDB como familia MySQL com os extras que o dialeto realmente suporta.
- Os cenarios "parse only" devem permanecer como itens do catalogo, mas com testes unitarios claros no lugar certo.

### Medio prazo

- Fechar os gaps de SQLite que aparecem como regressao de comparabilidade na matriz.
- Separar em backlog explicito os cenarios que ainda nao tem suporte real e os que so precisam de cobertura de teste.
- Padronizar a criacao de novos benchmarks por provider para nao depender de edicao manual em varios arquivos.

### Longo prazo

- Cobrir `Diagnostics`, `Snapshot` e `Setup` com benchmarks dedicados, para medir custo de plan/debug/snapshot e nao so CRUD.
- Evoluir o benchmark para acompanhar novas features de parser/executor sem quebrar o catalogo existente.

## MariaDB no benchmark

### O que precisa existir

1. `BenchmarkProviderId.MariaDb`.
2. `ProviderCatalog` com MariaDB e metadata de imagem.
3. `benchmark-feature-map.json` com MariaDB.
4. `MariaDbDialect` em `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Dialects`.
5. `MariaDbDbSqlLikeMemSession`.
6. `MariaDbTestcontainersSession`.
7. Suítes:
   - `MariaDb_DbSqlLikeMem_Benchmarks`
   - `MariaDb_Testcontainers_Benchmarks`

### Features que merecem prioridade em MariaDB

- `ConnectionOpen`
- `CreateSchema`
- `InsertSingle`
- `InsertBatch10`
- `InsertBatch100`
- `InsertBatch100Parallel`
- `SelectByPk`
- `SelectJoin`
- `UpdateByPk`
- `DeleteByPk`
- `TransactionCommit`
- `TransactionRollback`
- `Upsert`
- `SequenceNextValue`
- `StringAggregate`
- `StringAggregateOrdered`
- `StringAggregateDistinct`
- `StringAggregateCustomSeparator`
- `StringAggregateLargeGroup`
- `DateScalar`
- `BatchInsert10`
- `BatchInsert100`
- `BatchMixedReadWrite`
- `BatchReaderMultiResult`
- `BatchScalar`
- `BatchNonQuery`
- `BatchTransactionControl`
- `SavepointCreate`
- `RollbackToSavepoint`
- `ReleaseSavepoint`
- `NestedSavepointFlow`
- `JsonScalarRead`
- `JsonPathRead`
- `JsonInsertCast`
- `TemporalCurrentTimestamp`
- `TemporalDateAdd`
- `TemporalNowWhere`
- `TemporalNowOrderBy`
- `RowCountAfterInsert`
- `RowCountAfterUpdate`
- `RowCountAfterSelect`
- `RowCountInBatch`
- `CteSimple`
- `WindowRowNumber`
- `WindowLag`
- `ReturningInsert`
- `SelectExistsPredicate`
- `SelectCorrelatedCount`
- `GroupByHaving`
- `UnionAllProjection`
- `DistinctProjection`
- `MultiJoinAggregate`
- `SelectScalarSubquery`
- `SelectInSubquery`
- `CrossApplyProjection`
- `OuterApplyProjection`

### Limites que devem continuar visiveis

- `ReturningUpdate` continua bloqueado no contrato atual do MariaDB.
- `MergeBasic` nao deve ser tratado como supportivo sem confirmacao do runtime.
- `PartitionPruningSelect` deve ser adicionado apenas se houver query real e comportamento replicavel.
- Features de parser puro continuam fora do benchmark comparativo e devem viver em testes de parser.

## Trilhas de evolucao

### Fase 1 - Integracao do provider

- [x] Registrar MariaDB no catalogo de providers.
- [x] Criar a session in-memory para MariaDB.
- [x] Criar a session externa com Testcontainers.
- [x] Adicionar o dialeto de benchmark para MariaDB.
- [x] Adicionar o provider na feature map.

### Fase 2 - Comparabilidade basica

- [ ] Confirmar a saida da matriz com MariaDB em todas as suites baseline.
- [ ] Verificar se os cenarios de string aggregate e sequence aparecem com status coerente.
- [ ] Garantir que o MariaDB nao compartilhe nomes de artefato com MySQL na exportacao da wiki.

### Fase 3 - Cobertura avancada

- [ ] Expandir MariaDB com cenarios de window functions.
- [ ] Expandir MariaDB com `RETURNING` somente onde o runtime realmente suportar.
- [ ] Criar testes de parser e executor para os caminhos MariaDB-especificos em `src/DbSqlLikeMem.MariaDb.Test`.

### Fase 4 - Matriz e backlog

- [ ] Regerar `docs/Wiki/performance-matrix.md` com o novo provider.
- [ ] Atualizar o backlog dos providers com os gaps que sobrarem.
- [ ] Separar o que e "N/A por contrato" do que e "falta implementar".

## Checklist de acompanhamento

- Provider adicionado no benchmark.
- Suite criada para `DbSqlLikeMem`.
- Suite criada para `Testcontainers`.
- Dialeto benchmark criado.
- Feature map atualizado.
- Documento de backlog atualizado.
- Matriz wiki regenerada.

## Arquivos alvo

- `benchmark/DbSqlLikeMem.Benchmarks/benchmark-feature-map.json`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Core/BenchmarkProviderId.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Core/ProviderCatalog.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Core/ExternalBenchmarkSessionBase.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Dialects/MariaDbDialect.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/DbSqlLikeMem/MariaDbDbSqlLikeMemSession.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/External/MariaDbTestcontainersSession.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Suites/MariaDb_DbSqlLikeMem_Benchmarks.cs`
- `benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Suites/MariaDb_Testcontainers_Benchmarks.cs`

## Observacoes

- O catalogo deve continuar coerente com o suporte real do dialeto, mesmo quando a suite de benchmark exista antes do ajuste fino da feature.
- Quando um item for `N/A`, o backlog deve registrar se isso e limitação do banco real, da implementacao atual ou apenas falta de benchmark.
