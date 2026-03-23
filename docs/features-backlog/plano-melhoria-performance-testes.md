# Plano de melhoria de performance dos testes

## Objetivo

Reduzir o tempo total da coleta de benchmark e dos testes de fidelidade sem perder cobertura funcional.

## Verificação feita

- A sessão mock de SQLite já desliga a captura de plano e de métricas por padrão em [benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/DbSqlLikeMem/SqliteDbSqlLikeMemSession.cs](C:/Projects/DbSqlLikeMem/benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/DbSqlLikeMem/SqliteDbSqlLikeMemSession.cs#L16).
- A mesma sessão só liga `CaptureExecutionPlans` e `DbMetrics.Enabled` nas features de diagnóstico em [benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/DbSqlLikeMem/SqliteDbSqlLikeMemSession.cs](C:/Projects/DbSqlLikeMem/benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Sessions/DbSqlLikeMem/SqliteDbSqlLikeMemSession.cs#L47).
- A suíte de providers reais herda `BenchmarkSessionBase` ou `ExternalBenchmarkSessionBase`, e não encontrei flags de captura de plano/métrica ligadas nesses sessions.
- Conclusão prática: a regressão mais provável não vem de diagnóstico ligado por engano nos benchmarks normais, e sim de custo extra nos helpers e no setup dos testes.
- Os `Console.WriteLine` que ainda restam em `benchmark/DbSqlLikeMem.Benchmarks` estão limitados a erro e relatório final em `BenchmarkSessionBase`, `BenchmarkSuiteBase` e `Program.cs`, portanto não fazem parte do hot path.

## Leitura da matriz

- O grupo `Diagnostics` é caro na matriz app-specific, com `Debug trace`, `Execution plan` e `Last execution plans history` concentrados na faixa de milissegundos em [docs/Wiki/performance-matrix-app-specific.md](C:/Projects/DbSqlLikeMem/docs/Wiki/performance-matrix-app-specific.md#L8).
- O grupo `Setup` também tem pontos caros, principalmente `Temp table create and use` e `Temp table cross-connection isolation` em [docs/Wiki/performance-matrix-app-specific.md](C:/Projects/DbSqlLikeMem/docs/Wiki/performance-matrix-app-specific.md#L25).
- `Schema snapshot compare` aparece com outlier forte no Oracle em [docs/Wiki/performance-matrix-app-specific.md](C:/Projects/DbSqlLikeMem/docs/Wiki/performance-matrix-app-specific.md#L29).

## Hipóteses principais

- Logging em caminho quente está adicionando custo visível, porque a base de teste redireciona `Console` para o output do xUnit em [src/DbSqlLikeMem.TestTools/XUnitTestBase.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/XUnitTestBase.cs#L46).
- `TemporaryUsersScenario` faz várias escritas de console e imprime branches/SQL em [src/DbSqlLikeMem.TestTools/TemporaryTable/TemporaryUsersScenario.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/TemporaryTable/TemporaryUsersScenario.cs#L23).
- `SelectTableScenario` tem o mesmo padrão no ramo Oracle, inclusive com consultas extras só para confirmar seed em [src/DbSqlLikeMem.TestTools/Query/SelectTableScenario.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/Query/SelectTableScenario.cs#L29).
- `SchemaSnapshotServiceOpsTest` serializa snapshot para log no ramo Oracle em [src/DbSqlLikeMem.TestTools/Schema/SchemaSnapshotServiceOpsTest.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/Schema/SchemaSnapshotServiceOpsTest.cs#L86).
- `PerformanceServiceBase` usa reflexão a cada leitura/invocação de diagnóstico em [src/DbSqlLikeMem.TestTools/Performance/PerformanceServiceBase.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/Performance/PerformanceServiceBase.cs#L18).
- Os testes de fidelidade podem rodar mock e container no mesmo caso quando `RUN_CONTAINER_TESTS=true` e a flag específica da suíte está habilitada, dobrando o trabalho em [src/DbSqlLikeMem.TestTools/Tests/Performance/PerformanceTestsBase.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/Tests/Performance/PerformanceTestsBase.cs#L90) e [src/DbSqlLikeMem.TestTools/Tests/TemporaryTable/TemporaryTableTestsBase.cs](C:/Projects/DbSqlLikeMem/src/DbSqlLikeMem.TestTools/Tests/TemporaryTable/TemporaryTableTestsBase.cs#L36).

## Plano de ação

### Fase 1 - Cortes de baixo risco

- Remover ou guardar atrás de flag os `Console.WriteLine` dos helpers quentes.
- Evitar `snapshot.ToJson()` apenas para log quando o valor não for usado na asserção.
- Revisar se as consultas extras de verificação em branches Oracle são realmente necessárias para a cobertura.

### Fase 2 - Reduzir overhead estrutural

- Cachear `PropertyInfo`, `FieldInfo` e `MethodInfo` em `PerformanceServiceBase`.
- Evitar reflexão repetida para `LastExecutionPlan`, `DebugSql`, `ResetVolatileData` e `ResetAllVolatileData`.
- Reforçar o uso de `NoopScenario` nos benchmarks que só precisam de conexão aberta.

### Fase 3 - Isolar caminhos caros

- Manter a coleta de benchmark padrão sem container.
- Separar os testes com container em execução explícita ou categoria própria.
- Garantir que os benchmarks de diagnóstico continuem separados dos benchmarks de CRUD e setup.

## Flags de execução

- `RUN_CONTAINER_TESTS=true` habilita a base global de comparação com container.
- `RUN_PERFORMANCE_CONTAINER_TESTS=true` habilita a comparação com container na suíte de performance.
- `RUN_TEMPORARY_TABLE_CONTAINER_TESTS=true` habilita a comparação com container na suíte de tabela temporária.
- `RUN_TABLE_CONTAINER_TESTS=true` habilita a comparação com container na suíte de DDL.
- `RUN_INSERT_CONTAINER_TESTS=true` habilita a comparação com container na suíte de DML.
- `RUN_SELECT_CONTAINER_TESTS=true` habilita a comparação com container na suíte de Query.
- `RUN_SCHEMA_CONTAINER_TESTS=true` habilita a comparação com container na suíte de Schema.
- `RUN_TRANSACTION_CONTAINER_TESTS=true` habilita a comparação com container na suíte de Transactions.
- `RUN_MARIADB_CONTAINER_TESTS=true` habilita a comparação com container nas suítes MariaDB.
- `RUN_DB2_CONTAINER_TESTS=true` habilita a comparação com container nas suítes Db2.
- Essas flags são opcionais e servem para a camada de fidelidade; o benchmark principal continua sendo executado por completo para comparar a matriz de performance.

## Uso esperado

- Execução rápida local: não definir as flags específicas de container.
- Execução de comparação: definir `RUN_CONTAINER_TESTS=true`, a flag específica da suíte que se quer comparar e, quando aplicável, a flag específica do provedor.
- Benchmarks normais: manter os flags de diagnóstico desligados, exceto nas features que medem diagnóstico.

### Fase 4 - Guardrails

- Registrar no README ou wiki que o custo de `Diagnostics` é esperado e não deve contaminar a coleta normal.
- Manter as flags de captura desligadas fora das features `ExecutionPlan`, `DebugTrace` e `LastExecutionPlansHistory`.
- Evitar novas escritas de console em helpers que entram no caminho de benchmark.

## Priorização sugerida

1. Remover logs e serializações de debug do caminho quente.
2. Cachear reflexão nos helpers de diagnóstico.
3. Separar claramente benchmark rápido de benchmark com container.
4. Validar se as branches Oracle precisam das confirmações extras em tempo de execução.

## Critério de sucesso

- A coleta de benchmark normal volta a ficar previsivelmente mais rápida.
- Os benchmarks de diagnóstico continuam corretos, mas ficam restritos aos cenários que realmente medem diagnóstico.
- A matriz de performance mantém o mesmo comportamento funcional, com menos custo de infraestrutura de teste.

## Progresso atual

- Fases 1, 2 e 3 concluídas no escopo atual.
- Removidos logs e verificacoes extras de debug dos helpers quentes:
  - `TemporaryUsersScenario`
  - `SelectTableScenario`
  - `SchemaSnapshotServiceOpsTest`
  - `SelectByPKServiceTest`
- Cache de reflexao aplicado em `PerformanceServiceBase` para leitura de membros e invocacao de metodos de diagnostico.
- Separacao do fluxo rapido e container aplicada na suite de performance com a nova flag `RUN_PERFORMANCE_CONTAINER_TESTS`.
- Separacao do fluxo rapido e container aplicada na suite de tabela temporaria com a nova flag `RUN_TEMPORARY_TABLE_CONTAINER_TESTS`.
- Separacao do fluxo rapido e container aplicada nas suites `DDL`, `DML`, `Query`, `Schema` e `Transactions` com flags proprias por area.
- Comparacao com container de MariaDB e Db2 agora exige flags específicas do provedor além da flag da suíte.
- Flags específicas de container foram centralizadas no helper `IsContainerComparisonEnabled` em `XUnitTestBase`.
- As suítes agora usam helpers nomeados por área em `XUnitTestBase` para evitar string literals espalhadas.
- Nao ha mais `Console.WriteLine` no caminho quente de `DbSqlLikeMem.TestTools`.
- Os `Console.WriteLine` remanescentes do projeto de benchmark ficaram restritos a erro/relatorio final e nao entram no caminho quente.
- A superficie publica do projeto `DbSqlLikeMem.Benchmarks` foi documentada com summaries EN/PT nos tipos e wrappers publicos que ainda estavam em branco.
- Helpers e providers de extensao em `DbSqlLikeMem.*` tambem foram normalizados com summaries mais descritivos em EN/PT.
- O README raiz agora explicita o uso das flags específicas de container e mantém os diagnósticos fora do caminho rápido por padrão.
- Proximo passo recomendado: validar a matriz publicada depois da proxima execucao controlada e manter as novas flags apenas para comparacoes intencionais.
