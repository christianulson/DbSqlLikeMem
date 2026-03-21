# Plano de evolucao dos benchmarks

Este documento acompanha as proximas evolucoes da malha de benchmarks e da matriz de desempenho.

## Progresso recente

- MariaDB foi integrado ao benchmark com provider, catalogo, dialeto, sessoes in-memory e Testcontainers, alem das suias correspondentes.
- Os scripts operacionais tambem passaram a conhecer MariaDB: runner da matriz, provisionamento de containers e variaveis preprovisionadas.
- O gerador da wiki ja e generico e passa a exibir MariaDB assim que houver relatórios publicados para o provider, inclusive na variante `app-specific`.
- A wiki publica agora tem pagina de provider para MariaDB e a Home ja referencia a familia corretamente.
- O resumo da matriz de compatibilidade agora referencia MariaDB explicitamente como atalho de triagem.
- As paginas de escolha, receitas e troubleshooting agora tratam MariaDB como caminho de primeira classe.
- O README do benchmark agora inclui MariaDB nos exemplos de execucao e nas observacoes operacionais.
- O arquivo `PACKAGE_HINTS.md` tambem foi alinhado com MariaDB para orientar a instalacao inicial do pacote de benchmark.
- O arquivo `benchmark.env.example` agora inclui a variavel de conexao do MariaDB para orientar execucao preprovisionada.
- O plano de `runsettings` em duplo modo ja foi registrado para separar execucao rapida e comparacao opcional contra base real.
- A separacao de `runsettings` ja tem arquivo novo e seletor por propriedade `DbSqlLikeMemRunSettingsFileName` no MSBuild.
- Os artefatos gerados de matriz ainda dependem de regeneracao real; por enquanto o foco e manter o catalogo, o README e os scripts alinhados enquanto os relatórios nao sao refeitos.
- Os campos `indexRefs` foram removidos dos dois catálogos `benchmark-feature-map*.json`, porque essas referencias antigas não fazem mais sentido depois da migracao dos conteudos.
- O backlog agora serve como trilha viva para a proxima etapa: regenerar a matriz wiki, validar a comparabilidade basica e decidir quais gaps avancados merecem cobertura adicional.
- A validacao executavel ainda nao foi rodada nesta etapa; o foco ficou na integracao estrutural e no registro de andamento.
- A API publica de `Testcontainers.MariaDb` ja foi confirmada no pacote em cache local; a pendencia agora e validar o comportamento em execucao real da sessao externa.
- O catalogo de benchmark de MariaDB ainda mantem `N/A` para recursos fora do contrato atual, como `ReturningUpdate`, `MergeBasic` e `PartitionPruningSelect`, ate que o runtime real ou o benchmark sejam expandidos.
- O benchmark `ReturningInsert` agora usa um caminho real de MariaDB com `RETURNING`; `ReturningUpdate` continua em caminho generico ate ter uma instrumentacao separada.

## Em andamento

- [x] MariaDB ja foi incluido no catalogo do benchmark e nas suites correspondentes.
- [x] MariaDB ja foi incluido no catalogo `app-specific` do benchmark.
- [~] Validar a API real de `Testcontainers.MariaDb` usada por `MariaDbTestcontainersSession`.
- [ ] Regenerar `docs/Wiki/performance-matrix.md` com o provider novo e revisar a saida gerada.
- [ ] Regenerar `docs/Wiki/performance-matrix-app-specific.md` com MariaDB e revisar a saida gerada.
- [x] Confirmar que os scripts de matriz e provisionamento carregam MariaDB em todos os caminhos de execucao.
- [x] Confirmar que os hints de pacote do benchmark carregam MariaDB.
- [x] Atualizar `benchmark.env.example` com a variavel de conexao do MariaDB.
- [x] Atualizar o resumo de compatibilidade para citar MariaDB na triagem inicial.
- [x] Atualizar as paginas de escolha, receitas e troubleshooting para citar MariaDB.
- [x] Remover `indexRefs` dos catálogos do benchmark.
- [~] Confirmar quais itens de MariaDB permanecem `N/A` por contrato e quais precisam de implementacao.
- [~] Revisar o benchmark `ReturningUpdate` para garantir que o nome e o caminho executado representem a mesma coisa.
- [ ] Separar os proximos testes em tres trilhas claras:
  - benchmark e comparabilidade
  - parser e sintaxe
  - executor e compatibilidade por provider
- [x] Criar `src/real-db.runsettings` como perfil opt-in para base real.
- [x] Permitir selecao do arquivo de settings por `DbSqlLikeMemRunSettingsFileName`.

## Proximas atividades

1. Fechar a validacao da sessao externa de MariaDB com o `Testcontainers` real.
2. Regenerar a matriz wiki e conferir se MariaDB aparece com nome, familia e artefatos corretos.
3. Validar os scripts `run-core-matrix.ps1`, `run-benchmarks-preprovisioned.ps1` e `start-benchmark-databases*.ps1` com MariaDB incluido.
4. Marcar no backlog os `N/A` que sao limitação do banco e os que ainda representam lacuna de codigo.
5. Selecionar os primeiros testes extras para MariaDB, priorizando `StringAggregate`, `SequenceNextValue`, `WindowRowNumber` e `ReturningInsert` apenas quando houver suporte real.
6. Planejar as proximas frentes paralelas para os agentes iniciados, mantendo uma frente para matriz, outra para API e outra para triagem de gaps.
7. Ajustar os scripts para aceitar o perfil por parametro, sem mexer no fluxo rapido padrao.

## Proxima fase global

Assim que MariaDB estiver no mesmo nivel operacional dos demais providers, a proxima expansao deve abrir novos cenarios de benchmark para todos os bancos de dados, com foco em:

- `Diagnostics`: `ExecutionPlan`, `DebugTraceSelect`, `DebugTraceBatch`, `DebugTraceJson`, `LastExecutionPlansHistory`
- `Snapshot`: `SchemaSnapshotExport`, `SchemaSnapshotLoadJson`, `SchemaSnapshotApply`, `SchemaSnapshotRoundTrip`, `SchemaSnapshotCompare`
- `Setup`: `ConnectionReopenAfterClose`, `TempTableCreateAndUse`, `TempTableCrossConnectionIsolation`, `ResetVolatileData`, `ResetAllVolatileData`
- `Transactions`: `TempTableRollback`, `SavepointCreate`, `RollbackToSavepoint`, `ReleaseSavepoint`, `NestedSavepointFlow`
- `Comparabilidade`: cenarios de `RETURNING`, `MERGE`, `window functions` e `batch` que façam sentido para a familia real de cada provider

### Regra de abertura

- Primeiro fechar MariaDB e validar a matriz gerada.
- Depois abrir os novos cenarios por familia, evitando adicionar benchmark novo sem cobertura equivalente na matriz e no tracker.
- Manter cada novo cenário com regra clara de suporte, para não misturar `N/A por contrato` com falta de implementação.

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
- [ ] Validar a API de `Testcontainers.MariaDb` usada por `MariaDbTestcontainersSession`.
- [ ] Checar se o provider aparece com o nome correto nas exportacoes e nos artefatos gerados.

### Fase 3 - Cobertura avancada

- [ ] Expandir MariaDB com cenarios de window functions.
- [ ] Expandir MariaDB com `RETURNING` somente onde o runtime realmente suportar.
- [ ] Criar testes de parser e executor para os caminhos MariaDB-especificos em `src/DbSqlLikeMem.MariaDb.Test`.

### Fase 4 - Matriz e backlog

- [ ] Regerar `docs/Wiki/performance-matrix.md` com o novo provider.
- [ ] Atualizar o backlog dos providers com os gaps que sobrarem.
- [ ] Separar o que e "N/A por contrato" do que e "falta implementar".
- [ ] Marcar no tracker quais gaps foram fechados com evidencia concreta e quais continuam apenas mapeados.

### Fase 5 - Runsettings em duplo modo

- [x] Criar `src/real-db.runsettings` como perfil opt-in para base real.
- [x] Manter `src/.runsettings` como perfil rapido para cobertura e feedback curto.
- [x] Permitir selecao do perfil por parametro no MSBuild com `DbSqlLikeMemRunSettingsFileName`.
- [ ] Documentar quando usar cada perfil no README e no backlog.
- [ ] Validar que a descoberta das suites permanece igual nos dois modos.

## Checklist de acompanhamento

- Provider adicionado no benchmark.
- Suite criada para `DbSqlLikeMem`.
- Suite criada para `Testcontainers`.
- Dialeto benchmark criado.
- Feature map atualizado.
- Documento de backlog atualizado.
- Matriz wiki regenerada.
- Validação executavel pendente.
- Tracker atualizado com status vivo e proximas atividades.
- Separacao de runsettings registrada e planejada para implementacao.
- Seletor de runsettings implementado via `DbSqlLikeMemRunSettingsFileName`.

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
