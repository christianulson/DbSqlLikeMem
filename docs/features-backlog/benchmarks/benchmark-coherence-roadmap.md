# Benchmark Coherence Roadmap

Objetivo: organizar o projeto de benchmark para manter tres regras ao mesmo tempo:
- benchmarks reais so executam quando o provider real suporta a feature;
- benchmarks do mock executam em todos os providers;
- o fluxo final publica duas matrizes geradas por script: comparativo por banco e matriz do mock-only.

## Avaliacao atual

### O que esta coerente

- A separacao entre `benchmark-feature-map.json` e `benchmark-feature-map.app-specific.json` esta alinhada com a ideia de banco real versus mock-only.
- A base de `Sequence` foi extraida para `SequenceBenchmarkSuiteBase`, e os providers sem sequence nao herdam mais esse grupo.
- O validador de catalogo agora resolve aliases reais dos wrappers e nao depende mais de nome igual ao `BenchmarkFeatureId`.
- As suites de benchmark seguem um padrao consistente de nomeacao por provider e engine.
- O fluxo de geracao de wiki ja existe nos scripts separados e no orquestrador unico:
  - `Scripts/export-wiki.ps1` para a matriz comparativa;
  - `Scripts/export-wiki-app-specific.ps1` para a matriz do mock-only.
  - `Scripts/export-wiki-all.ps1` para orquestrar as duas saidas em uma unica chamada.

### O que ainda merece melhoria

- O arquivo `README.Benchmarks.md` ainda pode ganhar uma explicacao mais curta sobre o mapeamento por alias e wrappers.
- Parte da organizacao continua muito concentrada em `BenchmarkSuiteBase` e em `Core/Prepared`, o que aumenta o risco de acoplamento quando um grupo novo precisa de suporte por provider.
- Os scripts de exportacao contam com um orquestrador unico para a publicacao normal, mas ainda mantem variantes especializadas para suporte legado e execucao manual.
- A regra de suporte por provider real existe no dominio, mas a estrutura ainda pode explicitar melhor isso no codigo e na documentacao gerada.
- A matriz final ja pode ser produzida em uma unica execucao para os dois artefatos publicados em `docs/Wiki`, restando apenas manter o fluxo simples e consistente.

## Andamento da execucao

### Iteracao 1 - inventario inicial

- Data: 2026-05-01.
- Estado: concluido.
- Levantamento fechado nesta iteracao:
  - 17 arquivos de suite em `src/benchmark/DbSqlLikeMem.Benchmarks/Benchmarks/Suites`.
  - bases centrais confirmadas: `BenchmarkSuiteBase` e `SequenceBenchmarkSuiteBase`.
  - o validador atual ainda resolve o mapeamento por IL e compara as features `Comparable` do catalogo com a superficie publica das suites.
  - a separacao entre `benchmark-feature-map.json` e `benchmark-feature-map.app-specific.json` continua sendo a divisao principal entre comparavel e mock-only.
  - os scripts de publicacao atualizados seguem divididos entre comparativo e app-specific, com variantes single-table ainda presentes.
- Ponto de parada:
  - a proxima iteracao deve mapear, suite por suite, quais grupos permanecem comparaveis, quais sao mock-only e quais sao apenas legado/fallback estrutural.
  - depois disso, a proxima fatia deve consolidar a lista de bases intermediarias que realmente justificam extracao.

### Iteracao 2 - inventario detalhado das suites e do catalogo

- Data: 2026-05-01.
- Estado: concluido.
- Mapa das suites confirmado:
  - `SequenceBenchmarkSuiteBase` ativa em 13 suites:
    - `SqlServer_Testcontainers_Benchmarks`;
    - `SqlServer_DbSqlLikeMem_Benchmarks`;
    - `MariaDb_Testcontainers_Benchmarks`;
    - `MariaDb_DbSqlLikeMem_Benchmarks`;
    - `Oracle_Testcontainers_Benchmarks`;
    - `Oracle_DbSqlLikeMem_Benchmarks`;
    - `Npgsql_Testcontainers_Benchmarks`;
    - `Npgsql_DbSqlLikeMem_Benchmarks`;
    - `Firebird_Testcontainers_Benchmarks`;
    - `Firebird_DbSqlLikeMem_Benchmarks`;
    - `Db2_Testcontainers_Benchmarks`;
    - `Db2_DbSqlLikeMem_Benchmarks`;
    - `SqlAzure_DbSqlLikeMem_Benchmarks`.
  - `BenchmarkSuiteBase` ativa em 4 suites:
    - `Sqlite_Native_Benchmarks`;
    - `Sqlite_DbSqlLikeMem_Benchmarks`;
    - `MySql_Testcontainers_Benchmarks`;
    - `MySql_DbSqlLikeMem_Benchmarks`.
  - legado ainda visivel no codigo, mas fora da superficie ativa:
    - `BenchmarkSuiteBaseNew` permanece apenas comentado.
  - caso especial confirmado:
    - `SqlServer_Testcontainers_Benchmarks` e `SqlServer_DbSqlLikeMem_Benchmarks` sobrescrevem `SequenceNextValue` com `BenchmarkCategory("dialect")`.
- Catalogo confirmado por parse:
  - mapa principal: 9 providers e 236 features;
  - 220 features comparaveis;
  - 16 features nao comparaveis no mapa principal;
  - mapa app-specific: 9 providers e 24 features;
  - todas as 24 features app-specific sao mock-only.
- Ponto de parada:
  - a proxima iteracao deve revisar `README.Benchmarks.md` e os scripts de exportacao para alinhar o texto com a divisao real entre comparavel, mock-only e legado.
  - depois disso, a proxima fatia deve apontar quais ajustes de estrutura realmente merecem abrir base intermediaria ou orquestrador unico.

### Iteracao 3 - revisao da documentacao de entrada e exportacao

- Data: 2026-05-01.
- Estado: concluido.
- README revisado:
  - ainda afirma que os metodos seguem exatamente o nome da feature, mas o catalogo e o validador aceitam alias e mapeamento por wrapper;
  - ainda descreve a wiki como uma conversao direta do catalogo principal, sem explicitar com a mesma clareza a divisao entre comparativo e app-specific;
  - ainda mistura instrucoes de execucao, validacao e notas de dominio no mesmo bloco, o que dificulta separar o que e regra estrutural do que e comando operacional;
  - ja menciona `SqlAzure` como mock-only/proxy operacional, mas o texto ainda nao reflete com a mesma nitidez a politica atual de catalogo e matriz separada.
- Scripts de exportacao revisados:
  - `Scripts/export-wiki.ps1` continua como gerador do comparativo principal por provider, consumindo `benchmark-feature-map.json`;
  - `Scripts/export-wiki-app-specific.ps1` continua como gerador da matriz app-specific por provider, consumindo `benchmark-feature-map.app-specific.json`;
  - `Scripts/export-wiki-app-specific.single-table.ps1` permanece como variante de layout unico, ainda presente como alternativa legada de saida;
  - nao existe ainda um orquestrador unico de publicacao para encapsular as duas saidas e a variante single-table como um fluxo unico de alto nivel;
  - a conversao ainda depende de convencoes fixas de nome de arquivo dos relatórios `*-report-github.md`.
- Ponto de parada:
  - a proxima iteracao deve consolidar o primeiro bloco de ajustes estruturais realmente uteis: reduzir acoplamento de `BenchmarkSuiteBase`, separar capacidades por base intermediaria e decidir se `Core/Prepared` merece migrar para uma arvore mais orientada a dominio.
  - antes de qualquer refactor maior, a documentacao de entrada e os scripts ja ficaram identificados como area de atualização obrigatoria.

### Iteracao 4 - leitura estrutural de bases e dominio

- Data: 2026-05-01.
- Estado: concluido.
- Concentracao atual em `BenchmarkSuiteBase`:
  - `advancedquery`: 141 entradas;
  - `core`: 56 entradas;
  - `dialect`: 50 entradas;
  - `json`: 14 entradas;
  - `setup`: 13 entradas;
  - `temporal`: 12 entradas;
  - `batch`: 11 entradas;
  - `transactions`: 10 entradas;
  - `diagnostics`: 8 entradas;
  - `snapshot`: 6 entradas;
  - `advanced`: 4 entradas.
- Leitura estrutural derivada:
  - a base comum continua carregando muito mais do que o minimo comum;
  - a superficie realmente padronizada entre suites tende a ficar em `core` e parte de `advancedquery`, mas as capacidades de `dialect`, `json`, `setup`, `diagnostics` e `snapshot` ja justificam separacao por capacidade;
  - os blocos de `batch` e `transactions` tambem ja aparecem como familias distintas e nao apenas como detalhe de implementacao.
- `Core/Prepared` confirmado:
  - 30 estados preparados ativos sob a arvore `Benchmarks/Core/Prepared`;
  - os nomes atuais mostram um agrupamento por recurso, mas ainda sem uma arvore de dominio tao clara quanto `TestTools`.
- Espelhamento com `TestTools` confirmado:
  - `BenchmarkScenarioFactory` ja expõe cenarios por `DDL`, `DML`, `Query`, `TemporaryTable`, `Sequence`, `Performance` e `Noop`;
  - a arvore de `FidelityBaseTests` e `FidelityServicesTests` ja separa `DDL`, `DML`, `Performance`, `Query`, `Schema` e `TemporaryTable`;
  - isso sugere que a reorganizacao mais util nao e apenas mover arquivos, mas alinhar os estados preparados com as mesmas familias de dominio que os testes ja usam.
- Direcao mais provavel para a proxima fase:
  - criar bases intermediarias por capacidade, nao por provider isolado;
  - manter `BenchmarkSuiteBase` no minimo comum;
  - tratar `Core/Prepared` como apoio interno ate decidir quais grupos realmente merecem virar `Benchmarks/Features/<Dominio>`.
- Ponto de parada:
  - a proxima iteracao deve transformar essa leitura em uma proposta objetiva de extracao: quais bases novas valem existir primeiro e quais grupos devem continuar na base comum.
  - depois disso, o passo seguinte deve ser listar a ordem recomendada de implementacao estrutural sem alterar comportamento ainda.

### Iteracao 5 - proposta objetiva de extracao de bases

- Data: 2026-05-01.
- Estado: concluido.
- Prioridade de extracao proposta, em ordem:
  1. `Diagnostics`:
     - concentrar `ExecutionPlan`, `DebugTrace` e `LastExecutionPlansHistory`;
     - separar o que e claramente app-specific/mock-only do fluxo comparativo principal;
     - reduzir o peso diagnostico dentro de `BenchmarkSuiteBase`.
  2. `DDL` e lifecycle de setup:
     - concentrar `CreateSchema`, `CreateTable`, `CreateTableWithFK`, `DropTable` e fluxos proximos de inicializacao/limpeza;
     - alinhar melhor com a arvore `TestTools/DDL`.
  3. `TemporaryTable` e snapshots de ambiente:
     - separar os estados de tabela temporaria e exportacao/recuperacao de schema;
     - manter esses fluxos visivelmente ligados ao apoio de ambiente, nao ao caminho principal de query.
  4. `DML` operacional:
     - concentrar `Insert`, `Batch`, `Check`, `Transaction`, `Merge`, `Upsert`, `Parameter` e `Sequence` onde ainda fizer sentido;
     - manter `SequenceBenchmarkSuiteBase` como prova de que a extraçao por capacidade ja funciona.
  5. `Query` e `AdvancedQuery`:
     - mover o maior bloco restante para uma arvore de dominio mais clara;
     - separar selecao simples, joins, parametros, typed fields, window/query combinada e consultas compostas;
     - deixar `BenchmarkSuiteBase` apenas com o que realmente for comum a todas as suites.
  6. `Dialect`, `JSON` e `Temporal`:
     - tratar como ultima onda estrutural, porque ja sao familias grandes e mais especializadas;
     - reavaliar se alguma delas merece base propria ou apenas uma subclasse por capacidade.
- Critério de corte para a proxima fase:
  - uma nova base so deve nascer se ela reduzir acoplamento e tornar a ausencia de suporte mais visivel;
  - mover arquivo sem mudar leitura de dominio nao e ganho suficiente;
  - qualquer extracao precisa corresponder a uma familia ja reconhecivel em `TestTools` ou no catalogo de features.
- Ponto de parada:
  - a proxima iteracao deve virar este mapa em uma ordem de implementacao mais pratica, incluindo o que fica na base comum e o que pode ser extraido primeiro sem risco de quebrar a leitura atual.
  - depois disso, o backlog deve entrar na fase de estrutura com escopo pequeno e sequencial.

### Iteracao 6 - ordem pratica de implementacao estrutural

- Data: 2026-05-01.
- Estado: concluido.
- Base comum que deve permanecer em `BenchmarkSuiteBase` no curto prazo:
  - ciclo de vida do benchmark (`GlobalSetup`, `GlobalCleanup`, `IterationSetup`, `IterationCleanup`);
  - roteamento minimo `Run(BenchmarkFeatureId feature)`;
  - `ConnectionOpen`;
  - suporte comum de falha de setup e log de issue;
  - qualquer helper que seja realmente transversal a todas as suites.
- Primeiro bloco de extracao recomendado:
  1. `BenchmarkSuiteSetupBase` ou equivalente:
     - mover o ciclo de criacao/preparacao de estados e helpers de setup que nao dependem de query/dealito especifico;
     - isolar `CreateSchema`, `CreateTable`, `CreateTableWithFK`, `DropTable` e preparacao correlata.
  2. `BenchmarkDiagnosticsSuiteBase`:
     - mover `ExecutionPlan`, `DebugTrace` e `LastExecutionPlansHistory`;
     - separar claramente o caminho app-specific/mock-only do comparativo principal.
  3. `BenchmarkTemporaryTableSuiteBase`:
     - mover `TempTableCreateAndUse`, `TempTableRollback` e `TempTableCrossConnectionIsolation`;
     - manter esse grupo alinhado com a arvore `TestTools/TemporaryTable`.
  4. `BenchmarkDmlSuiteBase`:
     - mover insert, batch, check, transaction, merge, upsert e parameter flows que ainda estejam na base comum;
     - deixar sequence fora daqui, porque ja tem base propria.
  5. `BenchmarkQuerySuiteBase`:
     - mover consulta simples, joins, parametros, typed fields, `AllRows`, `OrderBy`, `GroupBy`, `Window`, `Apply` e demais flows de leitura;
     - deixar `AdvancedQuery` como subgrupo, se a superficie continuar grande.
  6. `BenchmarkDialectJsonTemporalSuiteBase` ou separacao por familias:
     - dividir os grupos especializados que ainda ficam misturados na base comum;
     - so criar base nova quando houver massa critica suficiente para justificar a nova fronteira.
- Regra pratica para decidir a primeira extracao:
  - escolher o grupo com maior repeticao de helpers e estados preparados, mas com menor risco de alterar a semantica das suites;
  - priorizar `Diagnostics` ou `Setup`, porque eles sao mais nitidos no espelhamento com `TestTools` e menos misturados com fluxo de query principal;
  - manter `SequenceBenchmarkSuiteBase` como modelo de referencia para a proxima extracao.
- Ordem operacional sugerida para a fase seguinte:
  1. extrair `Diagnostics`;
  2. extrair `Setup/DDL`;
  3. extrair `TemporaryTable`;
  4. extrair `DML`;
  5. extrair `Query/AdvancedQuery`;
  6. reavaliar `Dialect/JSON/Temporal`;
  7. ajustar o README e os catálogos se a nova arvore exigir nomes novos de apoio.
- Ponto de parada:
  - a proxima iteracao deve marcar o que seria o recorte minimo da primeira extracao real e o que precisa esperar para nao dispersar o refactor.
  - depois disso, a execucao pode sair do inventario e entrar em alteracao controlada.

### Iteracao 7 - primeira extracao real concluida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o bloco de `Diagnostics` saiu de `BenchmarkSessionBase.cs` e passou para o parcial `BenchmarkSessionBase.Diagnostics.cs`;
  - permaneceram na base comum apenas o ciclo de vida geral, o roteamento minimo e o suporte transversal;
  - a fronteira de `ExecutionPlan`, `DebugTrace` e `LastExecutionPlansHistory` ficou visivel como grupo proprio sem alterar a superficie publica da classe.
- Valor obtido:
  - a base comum ficou menor sem mudar comportamento;
  - a extracao confirmou que o refactor pode ser feito por grupos pequenos e contiguos;
  - o primeiro passo pratico nao exigiu mexer em suites, providers ou catalogo.
- Proximo corte sugerido:
  - `Setup/DDL`, porque e o segundo grupo mais natural depois de `Diagnostics` e ainda conversa bem com a arvore de `TestTools`;
  - depois disso, seguir para `TemporaryTable` e `DML` antes de tocar em `Query/AdvancedQuery`.
- Ponto de parada:
  - a proxima iteracao deve capturar o recorte minimo do grupo `Setup/DDL` para uma segunda extracao real.
  - se esse corte continuar limpo, a ordem de refactor pode seguir sem reabrir a estrutura inteira.

### Iteracao 8 - segunda extracao real concluida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o bloco de `Setup/DDL` saiu de `BenchmarkSessionBase.cs` e passou para o parcial `BenchmarkSessionBase.Ddl.cs`;
  - ficaram separados os fluxos de `RunCreateSchema`, `RunCreateTable`, `RunCreateTableWithFK`, `RunCreateTableWithFKInsert`, `RunInsertInTableWithFK` e `RunDropTable`;
  - a leitura estrutural da classe principal ficou mais curta sem mudar o contrato publico.
- Valor obtido:
  - a extração confirmou que grupos pequenos e contiguos podem sair da base comum sem espalhar dependencias;
  - o desenho da arvore agora espelha melhor a separacao de dominio que ja aparece em `TestTools/DDL`;
  - o refactor segue sem necessidade de alterar suites ou catalogo.
- Proximo corte sugerido:
  - `TemporaryTable`, porque e o terceiro grupo mais isolado e conversa bem com o apoio de ambiente;
  - depois disso, seguir para `DML` antes de voltar a `Query/AdvancedQuery`.
- Ponto de parada:
  - a proxima iteracao deve capturar o recorte minimo do grupo `TemporaryTable` para a terceira extracao real.
  - se esse grupo tambem sair limpo, o ritmo da reorganizacao continua seguro e previsivel.

### Iteracao 9 - terceira extracao real concluida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o bloco de `TemporaryTable` saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.TemporaryTable.cs`;
  - os overrides do `DbSqlLikeMemBenchmarkSessionBase` para `TemporaryTable` passaram para `DbSqlLikeMemBenchmarkSessionBase.TemporaryTable.cs`;
  - a base principal ficou ainda mais enxuta sem alterar a regra de execucao dos providers.
- Valor obtido:
  - a familia de tabela temporaria ficou visivelmente isolada como dominio proprio;
  - o corte confirmou que a reorganizacao pode continuar por grupos de capacidade;
  - o fluxo de `Diagnostics`, `Setup/DDL` e `TemporaryTable` agora aparece separado em arquivos diferentes, o que reduz o atrito de manutencao.
- Proximo corte sugerido:
  - `DML`, porque e o proximo bloco grande e ainda bem alinhado com o espelhamento de `TestTools`;
  - manter `Sequence` fora do proximo corte, porque ele ja esta segregado e serve como referencia de base intermediaria.
- Ponto de parada:
  - a proxima iteracao deve capturar o recorte minimo do bloco `DML` para a quarta extracao real.
  - se esse bloco sair limpo, a base comum pode continuar a encolher sem precisar reabrir o desenho da suite.

### Iteracao 10 - primeiro recorte de DML concluido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o slice inicial de `DML` saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.DmlInsert.cs`;
  - foram movidos os fluxos de `Insert`, `Check` e `Batch` que ainda estavam na base comum;
  - a classe principal perdeu um bloco grande de rotina de escrita sem alterar o roteamento das features.
- Valor obtido:
  - o refactor confirmou que `DML` tambem pode ser fatiado sem quebrar a leitura da classe principal;
  - o bloco extraido ja representa um subconjunto coerente e reconhecivel de `TestTools/DML`;
  - o restante de `DML` ficou ainda mais claro como alvo de cortes futuros em transacao, parametros e tipos.
- Proximo corte sugerido:
  - o restante de `DML`, priorizando `Update/Delete`, `Transaction`, `Savepoint`, `Upsert/Merge`, `Parameter` e `TypedField`;
  - depois disso, voltar para `Query/AdvancedQuery`, que ainda e o maior bloco em volume.
- Ponto de parada:
  - a proxima iteracao deve capturar o bloco restante de `DML` ou, se o corte ficar grande demais, dividir em `Update/Transaction` e `Parameter/TypedField`.
  - o objetivo agora e reduzir o custo de leitura da classe base sem criar fragmentacao excessiva de arquivos.

### Iteracao 11 - segundo recorte de DML concluido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o bloco de mutacao/transacao saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.DmlMutation.cs`;
  - foram movidos `Update/Delete`, `Transaction`, `Savepoint`, `Upsert`, `Merge` e o alias de `ParameterUpdateDeleteRoundTrip`;
  - a classe base principal ficou com menos rotinas de escrita e transacao, mantendo apenas os blocos que ainda nao foram fatiados.
- Valor obtido:
  - o dominio de mutacao ficou separado do slice de `Insert/Check/Batch`;
  - a arvore de benchmark agora espelha melhor a separacao de responsabilidade que ja aparece no catalogo e nos testes de fidelidade;
  - o restante de `DML` ficou concentrado em `Parameter` e `TypedField`, o que torna o proximo corte mais evidente.
- Proximo corte sugerido:
  - `Parameter` e `TypedField`, preferencialmente em um corte unico se a fronteira continuar limpa;
  - se o corte ficar grande demais, separar primeiro `Parameter` e depois `TypedField`.
- Ponto de parada:
  - a proxima iteracao deve capturar o bloco restante de `DML` focado em `Parameter` e `TypedField`.
  - depois disso, `DML` deve ficar praticamente encerrado como serie de parciais, restando apenas os helpers realmente transversais.

### Iteracao 12 - terceiro recorte de DML concluido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o bloco de `Parameter` e `TypedField` saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.ParameterTypedField.cs`;
  - foram movidos os fluxos de parameter binding, typed parameter round-trip, typed-field storage/function/calculation, matrix compound/predicate e parameter transaction;
  - `StoredProcedureCall` foi mantido fora deste corte por ser um bloco isolado de outro eixo de capacidade.
- Valor obtido:
  - o restante de `DML` ficou separado em uma fronteira coerente e mais facil de ler;
  - a base comum agora guarda muito menos lógica de mutacao parametrizada e tipada;
  - a reorganizacao continua seguindo familias reconheciveis no `TestTools`.
- Estado atual de `DML`:
  - `Insert/Check/Batch` ja estao em `BenchmarkSessionBase.DmlInsert.cs`;
  - `Update/Transaction/Savepoint/Upsert/Merge` ja estao em `BenchmarkSessionBase.DmlMutation.cs`;
  - `Parameter/TypedField` ja estao em `BenchmarkSessionBase.ParameterTypedField.cs`;
  - `StoredProcedureCall` segue na base comum como bloco isolado para outro corte.
- Proximo corte sugerido:
  - `StoredProcedureCall`, se for considerado parte do fluxo de performance a isolar agora;
  - depois disso, a rota natural volta para `Query/AdvancedQuery`, que ainda concentra o maior volume restante.
- Ponto de parada:
  - a proxima iteracao deve decidir se `StoredProcedureCall` entra como corte isolado ou se o refactor passa direto para `Query/AdvancedQuery`.
  - a base comum ja esta em um ponto bom para trocar de familia sem perder a trilha.

### Iteracao 13 - `StoredProcedureCall` isolado em partial proprio

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - o metodo-base de `StoredProcedureCall` saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.StoredProcedure.cs`;
  - o contrato continuou o mesmo, apenas ficou separado do restante da base comum;
  - as overrides em `DbSqlLikeMemBenchmarkSessionBase` e `ExternalBenchmarkSessionBase` continuam apontando para o mesmo ponto de extensao.
- Valor obtido:
  - o eixo de procedimento armazenado ficou visivelmente isolado como dominio proprio;
  - a classe principal perdeu mais um bloco especifico e ficou mais facil de ler por familias;
  - o refactor confirma que a separacao por capacidade continua segura antes de entrar no bloco grande de query.
- Proximo corte sugerido:
  - `Query/AdvancedQuery`, que ainda concentra o maior volume restante na base comum;
  - antes disso, vale apenas revisar se algum helper de consulta ja merece ser puxado junto para reduzir retrabalho.
- Ponto de parada:
  - a proxima iteracao deve entrar em `Query/AdvancedQuery` com um corte pequeno e claramente delimitado.
  - se esse bloco ficar grande demais, a divisao deve ocorrer por subfamilia de consulta, nao por provider.

### Iteracao 14 - primeira subfamilia de `Query/AdvancedQuery` concluida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - a familia de agregacao de strings saiu de `BenchmarkSessionBase.cs` e passou para `BenchmarkSessionBase.StringAggregate.cs`;
  - o corte levou junto `StringAggregate`, `StringAggregateOrdered`, `StringAggregateDistinct`, `StringAggregateCustomSeparator`, `StringAggregateLargeGroup`, `StringAggregateSummaryMatrix`, `StringAggregateGroupCaseMatrix` e os aliases `StringAggregationSummaryMatrix` e `StringAggregationGroupCaseMatrix`;
  - `StringAggregationVariants` ficou no mesmo partial como wrapper de composicao da familia.
- Valor obtido:
  - a primeira subfamilia grande de query ficou isolada sem misturar com temporal, math ou json;
  - a base comum perdeu um bloco coerente de leitura por strings agregadas, o que reduz o ruído ao navegar por `BenchmarkSessionBase`;
  - a fronteira mostra que `Query/AdvancedQuery` pode ser fatiado por dominio, nao por ordem dos metodos no arquivo.
- Proximo corte sugerido:
  - `Temporal`, por ser a proxima familia de leitura relativamente fechada e com dependencias locais claras;
  - depois disso, reavaliar se `Math` ou `JSON` merecem corte proprio antes de voltar para o restante de query.
- Ponto de parada:
  - a proxima iteracao deve entrar em `Temporal` ou, se o bloco parecer mais limpo, em `Math`;
  - o criterio e sempre extrair a proxima familia mais coesa sem criar um partial excessivamente largo.

### Iteracao 15 - familia temporal extraida do bloco de query

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - os metodos temporais sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.Temporal.cs`;
  - o corte levou junto `TemporalCurrentTimestamp`, `TemporalDateAdd`, `TemporalNowWhere`, `TemporalNowOrderBy`, `ScalarTemporalMatrix`, `TemporalFieldMatrix`, `TemporalComparisonMatrix`, `TemporalArithmeticMatrix`, `TemporalDateTrunc`, `TemporalTimeZoneOffset`, `TemporalFromParts`, `TemporalEndOfMonth` e `TemporalDateDiffBig`;
  - `JoinTemporal` e `ApplyTemporal` ficaram na base comum para um corte posterior, mantendo a primeira fatia temporal pequena.
- Valor obtido:
  - a familia temporal ficou visivelmente separada do restante de query;
  - a base comum perdeu mais um grupo de leitura com fronteira reconhecivel;
  - o restante de `Query/AdvancedQuery` continua claramente fatiavel por dominio.
- Proximo corte sugerido:
  - `Math`, porque ainda aparece como familia coesa e isolada;
  - se `Math` ficar grande demais, o proximo corte pode ser `JSON` antes de voltar aos joins e applies temporais.
- Ponto de parada:
  - a proxima iteracao deve entrar em `Math` ou, se a fronteira estiver mais limpa, em `JSON`;
  - ainda ha massa suficiente em query para manter cortes controlados.

### Iteracao 16 - familia temporal fechada com partial proprio

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - os metodos temporais foram consolidados em `BenchmarkSessionBase.Temporal.cs`;
  - o partial levou junto `TemporalCurrentTimestamp`, `TemporalDateAdd`, `TemporalNowWhere`, `TemporalNowOrderBy`, `ScalarTemporalMatrix`, `TemporalFieldMatrix`, `TemporalComparisonMatrix`, `TemporalArithmeticMatrix`, `TemporalDateTrunc`, `TemporalTimeZoneOffset`, `TemporalFromParts`, `TemporalEndOfMonth` e `TemporalDateDiffBig`;
  - `JoinTemporal` e `ApplyTemporal` continuam na base comum para o proximo corte, porque ainda podem valer um bloco separado.
- Valor obtido:
  - a familia temporal ficou coerente e isolada;
  - a base comum perdeu um conjunto grande de metodos de leitura e data/hora;
  - o caminho para o restante de `Query/AdvancedQuery` ficou mais enxuto.
- Proximo corte sugerido:
  - `Math`, que segue como a proxima familia mais coesa e independente;
  - se `Math` ficar muito grande, o plano pode alternar para `JSON` antes de voltar aos joins e applies.
- Ponto de parada:
  - a proxima iteracao deve entrar em `Math` com um bloco pequeno e fechado.
  - se o bloco nao fechar bem, a extracao deve parar em uma subfamilia interna e nao forcar um partial largo.

### Iteracao 17 - familia `Math` extraida do bloco de query

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - os metodos de math sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.Math.cs`;
  - o corte levou junto `MathFunctions`, `MathLogBaseFunction`, `MathLog2Function`, `MathPiFunction`, `MathRandFunction`, `MathRemainderFunction`, `MathTruncFunction`, `MathCotFunction`, `MySqlUtilityMathFunctions`, `GreatestLeastModFunctions`, `Db2AliasMathFunctions`, `FirebirdAliasMathFunctions` e `MathTranscendentalFunctions`;
  - `JsonScalarRead` ficou na base comum, preservando a fronteira entre math e o restante da query especializada.
- Valor obtido:
  - a familia matematica ficou isolada como bloco proprio;
  - a base comum perdeu outro grupo de leitura claramente reconhecivel;
  - o restante de `Query/AdvancedQuery` agora fica mais concentrado em JSON, SQL Server specific e joins/applies temporais.
- Proximo corte sugerido:
  - `JSON`, porque ainda guarda uma fronteira bem fechada e normalmente independente dos blocos de math e temporal;
  - depois disso, os joins e applies temporais voltam a ser o candidato mais natural.
- Ponto de parada:
  - a proxima iteracao deve entrar em `JSON` ou, se o bloco estiver maior do que parece, em algum subgrupo JSON menor;
  - o restante de query segue com cortes possiveis sem romper a coesao por dominio.

### Iteracao 18 - subfamilia JSON de leitura extraida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - os metodos centrais de JSON sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.JsonCore.cs`;
  - o corte levou junto `JsonScalarRead`, `JsonPathRead`, `JsonMissingPathRead`, `JsonMissingPathReturnsNull`, `JsonQueryRootFragment`, `JsonModifyReplace`, `JsonTypedFieldMatrix`, `JsonEachFromArray`, `JsonEachFromObject`, `JsonTreeStructure` e `OpenJsonArray`;
  - `ForJsonPathProjection`, `JsonInsertCast`, `JsonInsertCastReturnsNull` e `IsJson` ficaram na base comum como um segundo bloco JSON ainda separado.
- Valor obtido:
  - a parte central de JSON ficou isolada como subfamilia propria;
  - a base comum perdeu um bloco grande de leitura e parsing JSON;
  - o restante de `JSON` agora aparece como um segundo corte menor e bem definido.
- Proximo corte sugerido:
  - `ForJsonPathProjection`, `JsonInsertCast` e `IsJson`, se a ideia for fechar JSON em um segundo passo pequeno;
  - depois disso, o retorno natural segue para `JoinTemporal` e `ApplyTemporal`.
- Ponto de parada:
  - a proxima iteracao deve decidir se fecha o restante de JSON em um unico corte ou se separa `ForJsonPathProjection` do grupo `Insert/IsJson`.
  - a familia JSON ja esta dividida em duas partes reconheciveis.

### Iteracao 19 - restante de JSON extraido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - `ForJsonPathProjection`, `JsonInsertCast`, `JsonInsertCastReturnsNull` e `IsJson` sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.JsonExtras.cs`;
  - com isso, toda a familia JSON ficou fora da classe base comum;
  - a base comum ganhou mais uma fronteira limpa antes de voltar para os blocos temporais de join/apply.
- Valor obtido:
  - JSON agora está dividido em parciais pequenos e legiveis;
  - a classe principal ficou menos carregada com variacoes de JSON e SQL Server specific;
  - o restante de query agora se concentra mais claramente em joins e applies temporais.
- Proximo corte sugerido:
  - `JoinTemporal` e `ApplyTemporal`, se a ideia for fechar o ultimo bloco grande ainda pendente em query;
  - depois disso, vale reavaliar se sobrou alguma familia menor que ainda mereca isolamento.
- Ponto de parada:
  - a proxima iteracao deve entrar em `JoinTemporal`/`ApplyTemporal` ou numa subfamilia vizinha se a fronteira ficar mais limpa.
  - o objetivo agora e encerrar os grandes blocos de query ja reconhecidos.

### Iteracao 20 - bloco temporal de join/apply extraido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - `JoinWindowTemporalMatrix`, `JoinTemporalMatrix`, `JoinWindowAggregateTemporalMatrix`, `ApplyTemporalComposite` e `ApplyWindowTemporalComposite` sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.JoinTemporal.cs`;
  - o corte manteve `JoinWindowMatrix` na base comum, porque ele continua sendo um bloco de janela mais geral;
  - a ultima fronteira grande de query ficou reduzida a um conjunto bem menor de blocos especializados.
- Valor obtido:
  - o subdominio temporal de join/apply ficou separado do restante de query;
  - a classe base ficou menos concentrada em composicoes temporais;
  - o restante do arquivo agora esta mais perto de um estado de leitura modular por dominio.
- Proximo corte sugerido:
  - revisar se sobrou alguma subfamilia menor de query que ainda mereca isolamento;
  - se nao houver mais blocos grandes, o proximo passo pode ser apenas consolidacao e revisao de estrutura.
- Ponto de parada:
  - a proxima iteracao deve fazer a triagem final do que ainda sobra em `BenchmarkSessionBase`;
  - se nada grande restar, o trabalho pode sair de extracao e entrar em fechamento/documentacao.

### Iteracao 21 - familia `Window` extraida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - os metodos de janela sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.Window.cs`;
  - o corte levou junto `WindowRowNumber`, `WindowLag`, `WindowLead`, `WindowRankDenseRank`, `WindowFirstLastValue`, `WindowNtile`, `WindowPercentRankCumeDist` e `WindowNthValue`;
  - a base comum ficou com menos um bloco de consulta especializado e mais proximidade da parte administrativa do arquivo.
- Valor obtido:
  - a familia de janela ficou isolada como bloco proprio;
  - o arquivo principal ficou mais simples de navegar porque o conjunto de metodos analiticos saiu de cena;
  - o restante pendente agora tende mais a blocos de suporte, parse e relacional do que a familias grandes de query.
- Proximo corte sugerido:
  - reavaliar se `Parse` ainda justifica um parcial proprio ou se a triagem final ja pode encerrar a extracao;
  - se houver mais uma familia grande, ela deve ser a ultima antes do fechamento.
- Ponto de parada:
  - a proxima iteracao deve decidir entre extrair `Parse` ou fechar a fase com o que resta;
  - o objetivo agora e evitar criar parciais pequenos demais sem ganho de legibilidade.

### Iteracao 22 - familia `String`/`Parse` basica extraida

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - `StringBasicFunctions`, `ParseFamily`, `Soundex`, `Compression` e `ApproxCountDistinct` sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.StringBasics.cs`;
  - o corte consolidou um bloco pequeno de utilitarios de string e parsing basico;
  - a base comum ficou mais perto do fim da triagem estrutural.
- Valor obtido:
  - a familia basica de string/parsing ficou isolada como bloco proprio;
  - a classe principal perdeu um grupo coerente de utilitarios gerais;
  - o restante pendente ja tende a parecer mais suporte do que dominio grande.
- Proximo corte sugerido:
  - revisar se ainda existe alguma familia grande o bastante para justificar outro partial;
  - se nao houver, a fase pode ir para fechamento e apenas revisao final.
- Ponto de parada:
  - a proxima iteracao deve decidir se sobra algo grande em `BenchmarkSessionBase` ou se o trabalho entra em consolidacao final;
  - o refactor ja retirou os blocos mais pesados de query e utilitarios.

### Iteracao 23 - utilitarios SQL Server de string extraidos

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - `StringUtilityFunctions`, `StringMetadataFunctions`, `StringEscape`, `Translate`, `FormatMessage` e `Format` sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.StringSqlServer.cs`;
  - a base comum ficou mais curta em helpers de string SQL Server;
  - o arquivo principal agora tende ainda mais a concentrar as familias de query restantes e o suporte geral.
- Valor obtido:
  - o grupo SQL Server de string ficou isolado em um partial proprio;
  - a leitura da base comum ficou menos poluida por helpers de string específicos;
  - o restante do arquivo vai ficando mais perto de consolidacao do que de nova extracao grande.
- Proximo corte sugerido:
  - fazer nova triagem para ver se ainda sobra alguma familia grande o suficiente para justificar outro partial;
  - se nao houver, a fase pode ser encerrada com revisao final.
- Ponto de parada:
  - a proxima iteracao deve confirmar se resta algum bloco grande em `BenchmarkSessionBase` ou se ja e hora de fechar.
  - evitar criar extracoes pequenas demais sem ganho claro de manutencao.

### Iteracao 24 - bloco relacional extraido

- Data: 2026-05-01.
- Estado: concluido.
- Mudanca executada:
  - `RunSelectByPk`, `RunSelectJoin`, `RunSelectJoinCount`, `RunSelectApplyProjection`, `RunSelectWindowFunctions`, `RunSelectScalarSubqueryCaseMatrix`, `RunSelectRangeAndPivot`, `RunInListPredicate`, `RunBetweenPredicate`, `RunLikePredicate`, `RunNotLikePredicate`, `RunNotEqualPredicate`, `RunEqualPredicate`, `RunGreaterThanPredicate`, `RunLessThanPredicate`, `RunGreaterThanOrEqualPredicate`, `RunLessThanOrEqualPredicate`, `RunNotInSubqueryNull`, `RunRelationalComposite`, `RunAllRowsCount`, `RunAllRowsSnapshot`, `RunCteMaterializedHint`, `RunDistinctOnProjection`, `RunOrderByNameMatrix`, `RunOrderByOrdinalMatrix`, `RunOrderByNameDescendingMatrix`, `RunNamePaginationMatrix`, `RunGroupByNameInitialMatrix`, `RunGroupByNameHavingMatrix`, `RunGroupByOrdinalMatrix`, `RunDistinctOrderByOrdinalMatrix`, `RunDistinctLikeOrderByOrdinalMatrix`, `RunJoinTypedExpressionMatrix`, `RunJoinNullAggregateMatrix`, `RunJoinCastNullMatrix`, `RunJoinCastTextComparisonMatrix`, `RunJoinHavingCastMatrix`, `RunJoinLengthNumericMatrix`, `RunJoinTextCaseLengthMatrix`, `RunJoinDistinctCaseMatrix`, `RunJoinDistinctHavingMatrix` e `RunStringSplitProjection` sairam de `BenchmarkSessionBase.cs` e passaram para `BenchmarkSessionBase.Relational.cs`;
  - a classe base ficou mais enxuta e o bloco relacional passou a morar em um partial proprio;
  - as estruturas de apoio de consulta ficaram menos misturadas com o fim administrativo do arquivo.
- Valor obtido:
  - o conjunto relacional ficou isolado como familia coerente;
  - a base comum agora concentra menos fluxos de consulta de alto nivel;
  - o arquivo principal ficou mais perto de um fechamento estrutural do que de nova expansao.
- Proximo corte sugerido:
  - se ainda houver alguma familia grande, ela provavelmente ja e menor e pode nao justificar nova extracao;
  - caso contrario, o trabalho deve seguir para consolidacao final e revisao de leftovers.
- Ponto de parada:
  - a proxima iteracao deve confirmar se ainda sobra algo grande em `BenchmarkSessionBase` ou se ja e hora de encerrar a fase;
  - o objetivo agora e nao abrir novos blocos desnecessarios.

## Regras de dominio

- Funcionalidade contra banco real:
  - so entra na matriz comparativa se o provider real suportar a feature;
  - nao deve gerar erro indevido por executar benchmark fora do suporte do provider.
- Funcionalidade somente do mock:
  - deve ser executada para todos os providers;
  - deve aparecer na matriz do mock-only sem depender de suporte externo.
- Fidelidade funcional:
  - continua sendo validada em outra camada da aplicacao;
  - benchmark nao deve reproduzir toda a bateria de comportamento, apenas medir o caminho suportado.

## Melhorias propostas

### 1. Formalizar os dois grupos de benchmark

- Manter o catalogo em dois blocos claramente separados:
  - features comparaveis contra provider real;
  - features app-specific/mock-only.
- Evitar que uma feature mude de categoria sem registro explicito no backlog.
- Documentar o criterio de classificacao na mesma pasta do backlog de benchmarks.
- Diferenciar de forma explicita, em codigo e documentacao:
  - benchmark real suportado por provider;
  - benchmark mock-only executado em todos os providers.

### 2. Espelhar dominio entre TestTools e Benchmarks

- Avaliar mover `Core/Prepared` para uma arvore orientada a dominio em `Benchmarks/Features`.
- Espelhar a estrutura do `TestTools` quando isso reduzir atrito de manutencao:
  - `DDL`;
  - `DML`;
  - `Query`;
  - grupos de diagnostico, lifecycle e setup quando fizer sentido.
- O objetivo e deixar obvio qual benchmark corresponde a qual teste de fidelidade, sem precisar abrir varios arquivos.

### 3. Extrair bases por capacidade

- Manter `BenchmarkSuiteBase` apenas com o conjunto comum minimo.
- Criar bases intermediarias quando um grupo exigir suporte especifico:
  - sequence;
  - diagnostics/mock-only;
  - upsert/merge/returning, se a superficie continuar crescendo;
  - outros grupos que dependam de um subconjunto real de providers.
- O objetivo e reduzir heranca com heranca acidental e deixar a ausencia de suporte visivel na estrutura.

### 4. Centralizar o mapa de suporte por provider

- Tratar `benchmark-feature-map.json` como fonte de verdade para features reais.
- Tratar `benchmark-feature-map.app-specific.json` como fonte de verdade para features mock-only.
- Extrair uma politica central de execucao para responder se um benchmark pode rodar em um provider dado.
- Fazer essa politica ser consumida por:
  - suites;
  - validacao de catalogo;
  - geracao da matriz;
  - futuras rotinas de skip/abortamento gracioso.

### 5. Simplificar o comportamento das Sessions

- Manter a separacao entre:
  - `DbSqlLikeMem` mock session;
  - `External` testcontainers session;
  - `Native` quando houver engine local dedicada.
- A politica central deve ser avaliada antes da execucao do payload para evitar erro indevido em provider sem suporte.
- Quando um benchmark nao for suportado, a session deve registrar skip ou retorno controlado, nao falha inesperada.

### 6. Reorganizar a geracao das matrizes

- Criar um script orquestrador unico para gerar os artefatos:
  - um arquivo de comparativo por banco;
  - matriz do mock-only.
- O script orquestrador deve apenas chamar os geradores existentes ou futuras variantes.
- A saida comparativa deve ser quebrada em um arquivo por banco/provider, para isolar leitura e manutencao.
- A saida atual de comparativo deve virar um indice/landing page com links para cada arquivo por banco.
- O resultado do mock-only deve continuar em um arquivo unico separado em `docs/Wiki`.
- O input da geracao deve ser estruturado, preferencialmente em JSON, para evitar reprocessamento de texto livre do BenchmarkDotNet.
- O formato exato do JSON de resultados atual deve ser documentado antes de qualquer refactor maior de exportacao.

### 7. Melhorar a rastreabilidade

- Manter um backlog por pasta com:
  - item;
  - status;
  - origem do problema;
  - criterio de conclusao.
- Registrar quando uma feature muda de categoria, de base de suite ou de script gerador.

### 8. Ajustar a documentacao de entrada

- Atualizar `README.Benchmarks.md` para refletir a realidade atual:
  - wrappers podem usar alias;
  - nem toda classe publica precisa repetir a regra de mapeamento;
  - o catalogo e a base determinam a superficie real.
- Explicar em linguagem direta a diferenca entre:
  - benchmark comparativo;
  - benchmark mock-only;
  - teste de fidelidade funcional.

## Plano de execucao sugerido

### Fase 1 - Inventario

- Revisar todas as suites publicas.
- Separar o que e comparavel, o que e mock-only e o que e legado/fallback.
- Listar as classes-base que ainda concentram muito comportamento.
- Mapear os dominios que podem ser espelhados entre `TestTools` e `Benchmarks`.
- Levantar o formato exato do JSON de resultados usado hoje pelo BenchmarkDotNet e pelos scripts de exportacao.

### Fase 2 - Estrutura

- Extrair bases intermediarias onde houver grupo claro de capacidades.
- Reduzir o tamanho de `BenchmarkSuiteBase`.
- Revisar o contrato dos wrappers para manter a superficie previsivel.
- Avaliar a substituicao de `Core/Prepared` por uma arvore `Benchmarks/Features/<Dominio>`.

### Fase 3 - Catalogo e scripts

- Consolidar a regra de mapeamento dos benchmarks no catalogo.
- Criar um orquestrador unico para gerar as duas matrizes.
- Garantir que a wiki publicada continue consumindo apenas artefatos gerados.
- Definir o contrato de dados de exportacao para a geracao das matrizes.

### Fase 4 - Documentacao

- Atualizar o README do projeto de benchmarks.
- Atualizar o backlog com as decisoes estruturais finais.
- Registrar o criterio de suporte por provider para evitar regressao futura.
- Documentar a regra de execucao separada entre provider real e mock-only.

### Fase 5 - Governanca, confiabilidade e operacao

Objetivo: transformar o benchmark em um fluxo confiavel, repetivel e facil de manter depois da reorganizacao estrutural. Esta fase deve ser executada depois das fases de inventario, estrutura, catalogo/scripts e documentacao, porque depende das decisoes finais de organizacao.

#### 5.1 Governanca de evolucao das features

##### Problema que esta fase resolve

Sem uma regra formal de evolucao, uma feature pode mudar de categoria sem deixar rastro. Isso afeta tres coisas:
- a matriz comparativa pode passar a incluir algo que ainda nao e suportado por provider real;
- a matriz mock-only pode perder uma feature que ainda deveria ser medida em todos os providers;
- comparacoes antigas podem ficar dificeis de interpretar, porque o mesmo nome pode representar uma regra diferente ao longo do tempo.

##### Implementacao sugerida

- Criar um arquivo de governanca na pasta de benchmarks, por exemplo:
  - `Benchmarks/Governance/feature-change-log.md`;
  - ou `docs/Benchmarks/feature-change-log.md`, se a documentacao tecnica ficar fora do projeto.
- Para cada mudanca de feature, registrar:
  - `FeatureId`;
  - nome legivel;
  - categoria anterior;
  - categoria nova;
  - motivo da mudanca;
  - impacto esperado na matriz comparativa;
  - impacto esperado na matriz mock-only;
  - data;
  - PR/commit relacionado quando existir.
- Criar uma convencao simples de status para cada feature:
  - `Proposed`: feature ainda nao implementada no benchmark;
  - `Comparable`: pode entrar na matriz por provider real;
  - `MockOnly`: deve rodar somente na matriz app-specific/mock-only;
  - `Deprecated`: ainda existe para historico, mas nao deve receber novos investimentos;
  - `Removed`: foi removida do catalogo ativo.
- Se a feature sair de `MockOnly` para `Comparable`, exigir explicitamente:
  - suporte real em pelo menos um provider;
  - entrada no `benchmark-feature-map.json`;
  - remocao ou justificativa no `benchmark-feature-map.app-specific.json`;
  - atualizacao da documentacao da matriz.
- Se a feature sair de `Comparable` para `MockOnly`, exigir explicitamente:
  - motivo tecnico para deixar de comparar contra provider real;
  - ajuste nos scripts de exportacao;
  - indicacao clara na wiki para evitar interpretacao errada.

##### Checklist de implementacao

- [x] Criar arquivo de historico de mudancas de features.
- [x] Definir os status oficiais de feature.
- [x] Atualizar `FeatureDefinition` ou o catalogo JSON para carregar o status, se fizer sentido.
- [x] Atualizar o validador para impedir feature ativa sem status.
- [x] Atualizar o README para explicar o ciclo de vida da feature.
- [x] Registrar no historico a separacao atual entre comparavel e mock-only como primeira entrada.

##### Criterio de conclusao

- Toda feature ativa tem categoria explicita.
- Toda mudanca de categoria tem registro.
- O validador falha quando uma feature nova entra sem categoria ou status.

#### 5.2 Identidade estavel dos benchmarks

##### Problema que esta fase resolve

Hoje o nome da classe, o nome do metodo e o `BenchmarkFeatureId` ajudam na identificacao, mas futuras reorganizacoes de pastas, bases e wrappers podem mudar nomes publicos sem que a feature tenha mudado de significado. Isso dificulta comparar resultados antigos e novos.

##### Implementacao sugerida

- Definir um identificador estavel para cada benchmark.
- O identificador deve sobreviver a:
  - mudanca de nome de metodo;
  - mudanca de classe/suite;
  - mudanca de pasta;
  - extracao de base intermediaria.
- Formato sugerido:
  - `category.feature.variant`
  - exemplos:
    - `core.insert.single`;
    - `setup.create-table.with-fk`;
    - `advanced-query.join.cast-null`;
    - `diagnostics.execution-plan.history`;
    - `mock-only.debug-trace.select`.
- Guardar esse identificador em uma das opcoes abaixo:
  - no `FeatureDefinition`;
  - no JSON de catalogo;
  - em um atributo customizado no metodo de benchmark, caso a superficie publica precise ficar autoexplicativa.
- A declaracao canonica atualmente fica em `FeatureDefinition.StableId`, com fallback para o nome do enum quando o catalogo nao define um alias explicito.
- A saida JSON usada pelos scripts deve carregar:
  - `BenchmarkStableId`;
  - `BenchmarkFeatureId`;
  - `ProviderId`;
  - `Engine`;
  - `SuiteName`;
  - `MethodName`;
  - `Category`;
  - `Comparable` ou `MockOnly`;
  - versao do schema de resultado.

##### Checklist de implementacao

- [x] Escolher onde o identificador estavel sera declarado.
- [x] Adicionar identificador estavel a todas as features atuais.
- [x] Atualizar exportadores para usar o identificador estavel como chave primaria logica.
- [x] Garantir que a wiki continue exibindo nomes legiveis, mas use o ID estavel para agrupamento.
- [x] Criar validacao contra IDs duplicados.
- [x] Criar validacao contra alteracao acidental de ID estavel.

##### Criterio de conclusao

- Renomear uma suite ou metodo nao muda a identidade historica do benchmark.
- A matriz gerada consegue agrupar resultados pelo ID estavel.
- O validador detecta IDs duplicados.

#### 5.3 Reprodutibilidade do ambiente

##### Problema que esta fase resolve

Benchmarks comparativos perdem valor se forem executados em ambientes diferentes sem registro. Testcontainers, bancos locais, configuracoes de container, hardware e versoes de runtime podem afetar o resultado.

##### Implementacao sugerida

- Criar um arquivo de especificacao do ambiente, por exemplo:
  - `Benchmarks/benchmark-environment.md`;
  - ou `docs/Benchmarks/benchmark-environment.md`.
- Documentar obrigatoriamente:
  - versao do .NET SDK/runtime;
  - sistema operacional;
  - CPU;
  - memoria;
  - Docker/Testcontainers;
  - imagem e versao de cada banco;
  - configuracoes especiais por provider;
  - se o benchmark foi local ou manual.
- Adicionar no JSON de resultado um bloco `Environment`, contendo no minimo:
  - `OS`;
  - `Runtime`;
  - `Processor`;
  - `DockerVersion`;
  - `ProviderImage`;
  - `ProviderVersion`;
  - `RunProfile`;
  - `TimestampUtc`.
- Criar perfis de execucao:
  - `smoke`: rapido, usado para validar que tudo roda;
  - `core`: matriz essencial, usada em validacao manual;
  - `full`: execucao completa, recomendada para validacao manual;
  - `diagnostic`: execucao com logs adicionais.

##### Checklist de implementacao

- [x] Criar documento de ambiente de benchmark.
- [x] Definir os perfis oficiais de execucao.
- [x] Atualizar scripts de run para receber `-Profile smoke|core|full|diagnostic`.
- [x] Registrar perfil usado no resultado exportado.
- [x] Registrar versoes dos providers/testcontainers quando possivel.
- [x] Documentar quais resultados podem ser comparados entre si.

##### Criterio de conclusao

- Todo resultado publicado informa o ambiente usado.
- Fica claro se duas execucoes sao comparaveis.
- Existe um fluxo rapido para validar manualmente sem rodar a matriz inteira.

#### 5.4 Qualidade estatistica e confiabilidade dos dados

##### Problema que esta fase resolve

A matriz nao deve apenas mostrar numeros. Ela precisa indicar quando um resultado e confiavel, instavel ou incompleto. Sem isso, uma diferenca pequena pode ser interpretada como ganho ou perda real.

##### Implementacao sugerida

- Definir campos minimos que os exportadores devem preservar do BenchmarkDotNet:
  - media;
  - mediana;
  - desvio padrao;
  - erro;
  - intervalo de confianca, se disponivel;
  - numero de iteracoes;
  - quantidade de warmups;
  - outliers removidos;
  - status da execucao.
- Criar classificacao simples de confiabilidade:
  - `Ok`: resultado completo e dentro da variacao esperada;
  - `Noisy`: variacao alta;
  - `Incomplete`: faltou dado;
  - `Skipped`: nao executado por regra de suporte ou perfil;
  - `Failed`: falhou inesperadamente;
  - `NotSupported`: provider real nao suporta a feature.
- Na wiki, exibir legenda para cada status.
- Evitar misturar `Skipped`, `NotSupported` e `Failed`, pois eles significam coisas diferentes.
- Definir limite inicial para marcar resultado como instavel, por exemplo:
  - alto desvio relativo;
  - numero insuficiente de iteracoes;
  - erro acima de um percentual definido.

##### Checklist de implementacao

- [x] Definir quais campos estatisticos entram no JSON intermediario.
- [x] Definir status oficiais de execucao.
- [x] Atualizar scripts de exportacao para manter status sem converter tudo para texto livre.
- [x] Atualizar wiki para mostrar legenda de status.
- [x] Adicionar validacao para diferenciar falha inesperada de feature nao suportada.
- [x] Documentar que benchmark mede performance, nao fidelidade funcional completa.

##### Criterio de conclusao

- Resultado publicado mostra numero e status.
- Feature nao suportada nao aparece como falha.
- Falha inesperada nao aparece como skip esperado.

#### 5.5 Historico, baseline e regressao

##### Problema que esta fase resolve

Sem baseline, a matriz mostra apenas o estado atual. Para evoluir o projeto, tambem e importante saber se uma mudanca melhorou, piorou ou apenas reorganizou os benchmarks.

##### Implementacao sugerida

- Criar uma pasta de resultados historicos, por exemplo:
  - `docs/Wiki/BenchmarkResults/history`;
  - ou `BenchmarkDotNet.Artifacts/history`, se nao quiser versionar todos os resultados publicados.
- Definir um arquivo baseline por perfil:
  - `baseline-smoke.json`;
  - `baseline-core.json`;
  - `baseline-full.json`.
- Criar um script de comparacao:
  - entrada: resultado atual + baseline;
  - saida: resumo de regressao/melhoria/sem mudanca relevante.
- Definir limites iniciais:
  - melhoria relevante: acima de X%;
  - regressao relevante: acima de Y%;
  - ignorar diferencas pequenas dentro do ruido estatistico.
- O comparador deve usar `BenchmarkStableId + ProviderId + Engine + Profile` como chave.

##### Checklist de implementacao

- [x] Definir onde os baselines serao armazenados.
- [x] Criar formato JSON de baseline.
- [x] Criar script de comparacao baseline vs atual.
- [x] Definir thresholds iniciais de regressao.
- [x] Publicar resumo de regressao junto com a wiki.
- [x] Documentar quando atualizar o baseline.

##### Criterio de conclusao

- Uma execucao nova pode ser comparada com uma execucao anterior.
- O resultado informa regressao, melhoria ou variacao irrelevante.
- Mudancas estruturais nao quebram comparacao historica se o ID estavel continuar igual.

#### 5.6 Execucao manual dos benchmarks e validacao local

##### Problema que esta fase resolve

Rodar os benchmarks manualmente exige uma rotina clara para evitar esquecer etapas e manter a wiki sempre coerente.

##### Implementacao sugerida

- Criar tres niveis de execucao manual:
  - smoke: valida catalogo, scripts e um subconjunto rapido;
  - core: roda matriz essencial;
  - full: roda matriz completa e gera artefatos.
- O fluxo de smoke deve validar:
  - build do projeto de benchmarks;
  - validador de catalogo;
  - parse dos dois JSONs de feature map;
  - geracao seca da wiki sem publicar;
  - perfil `smoke`.
- O fluxo full deve validar:
  - start dos bancos;
  - execucao por provider;
  - exportacao dos resultados;
  - comparacao com baseline;
  - publicacao dos arquivos em `docs/Wiki`.
- Separar falha de infraestrutura de falha de benchmark:
  - banco nao subiu;
  - container indisponivel;
  - benchmark falhou;
  - feature nao suportada.

##### Checklist de implementacao

- [x] Criar script unico para validar catalogo e mapas.
- [x] Criar profile `smoke`.
- [x] Criar rotina manual de validacao rapida.
- [x] Criar rotina manual full.
- [x] Publicar artefatos gerados apenas quando a execucao for completa e valida.
- [x] Documentar como reproduzir localmente o mesmo fluxo manual.

##### Criterio de conclusao

- Uma alteracao de benchmark quebra a validacao se violar catalogo ou documentacao gerada.
- A execucao full gera os mesmos artefatos que a rotina local.
- O processo de publicacao nao depende de passos escondidos em automacao.

#### 5.7 Observabilidade e diagnostico do fluxo

##### Problema que esta fase resolve

Quando um benchmark falha, e preciso saber rapidamente se o problema esta no provider, no container, na sessao, no setup, no payload ou no exportador.

##### Implementacao sugerida

- Padronizar logs por etapa:
  - inicializacao do provider;
  - criacao de banco/schema/tabelas;
  - execucao do payload;
  - cleanup;
  - exportacao;
  - publicacao.
- Adicionar correlation id por execucao:
  - `RunId`;
  - `ProviderId`;
  - `Engine`;
  - `BenchmarkStableId`.
- Criar pasta de logs estruturada:
  - `BenchmarkDotNet.Artifacts/logs/<RunId>/<ProviderId>/...`
- Separar logs humanos de dados estruturados:
  - `.log` para leitura direta;
  - `.json` para scripts.
- Registrar skips controlados com motivo:
  - `ProviderDoesNotSupportFeature`;
  - `MockOnlyFeature`;
  - `ProfileExcluded`;
  - `InfrastructureUnavailable`;
  - `DeprecatedFeature`.

##### Checklist de implementacao

- [x] Definir campos obrigatorios de log.
- [x] Gerar `RunId` unico por execucao.
- [x] Incluir `RunId` nos resultados exportados.
- [x] Registrar motivo de skip de forma estruturada.
- [x] Separar erro de infraestrutura de erro do benchmark.
- [x] Documentar onde olhar quando uma execucao falhar.

##### Criterio de conclusao

- Toda falha tem etapa, provider, engine e feature associados.
- Todo skip esperado tem motivo estruturado.
- O diagnostico nao depende apenas de ler output livre do console.

#### 5.8 UX da wiki gerada

##### Problema que esta fase resolve

A wiki nao deve ser apenas um dump de dados. Ela precisa ajudar o leitor a entender rapidamente o que foi medido, o que foi pulado e o que nao e comparavel.

##### Implementacao sugerida

- Criar uma landing page de benchmarks com:
  - objetivo da matriz;
  - data da ultima execucao;
  - perfil usado;
  - ambiente usado;
  - links para matriz por provider;
  - link para matriz mock-only;
  - link para resumo de regressao;
  - legenda dos status.
- Em cada pagina por provider, exibir:
  - provider;
  - engine;
  - versao do banco;
  - features executadas;
  - features nao suportadas;
  - falhas inesperadas;
  - observacoes do ambiente.
- Na matriz mock-only, deixar explicito que:
  - a feature nao depende do provider real;
  - a execucao usa a camada mock;
  - o objetivo e medir comportamento interno/app-specific.
- Usar simbolos padronizados, por exemplo:
  - `OK`: executado com sucesso;
  - `NS`: nao suportado pelo provider real;
  - `SKIP`: skip esperado por perfil/regra;
  - `FAIL`: falha inesperada;
  - `NOISY`: resultado instavel.

##### Checklist de implementacao

- [x] Criar landing page da wiki.
- [x] Criar legenda global de status.
- [x] Separar visualmente comparativo real e mock-only.
- [x] Mostrar data, ambiente e perfil da execucao.
- [x] Adicionar link para baseline/regressao quando existir.
- [x] Garantir que ausencia de suporte nao seja confundida com erro.

##### Criterio de conclusao

- Um leitor consegue entender a matriz sem abrir o codigo.
- O motivo de cada ausencia de resultado fica claro.
- A wiki mostra dados suficientes para reproduzir ou questionar a execucao.

#### 5.9 Testes do proprio sistema de benchmark

##### Problema que esta fase resolve

O projeto de benchmark tambem precisa ser testado. Caso contrario, os scripts e validadores podem gerar uma matriz errada sem que o build perceba.

##### Implementacao sugerida

- Criar testes unitarios ou de snapshot para:
  - parse de `benchmark-feature-map.json`;
  - parse de `benchmark-feature-map.app-specific.json`;
  - validacao de IDs duplicados;
  - validacao de feature sem status;
  - validacao de wrapper publico sem catalogo;
  - geracao da matriz comparativa;
  - geracao da matriz mock-only;
  - conversao de status `NotSupported`, `Skipped` e `Failed`.
- Criar fixtures pequenas com JSON de exemplo:
  - resultado completo;
  - resultado com feature nao suportada;
  - resultado com falha;
  - resultado mock-only;
  - resultado com campo ausente para testar erro.
- Evitar que os testes dependam de rodar banco real. A maior parte deve validar catalogo, politica e exportacao.

##### Checklist de implementacao

- [x] Criar projeto ou pasta de testes para o sistema de benchmark.
- [x] Criar fixtures de JSON minimo.
- [x] Testar geracao da wiki com snapshot.
- [x] Testar politica de execucao por provider.
- [x] Testar validacao de catalogo.
- [x] Testar diferenciacao entre skip esperado e falha.

##### Criterio de conclusao

- Alteracao errada no catalogo quebra teste.
- Alteracao errada no exportador quebra teste.
- O sistema de benchmark consegue ser validado sem rodar a matriz completa.

#### 5.10 Politica de depreciacao e limpeza

##### Problema que esta fase resolve

Com o crescimento do projeto, algumas features, scripts, wrappers ou bases antigas vao ficar obsoletos. Sem regra de depreciacao, o projeto acumula legado e fica mais dificil manter a matriz coerente.

##### Implementacao sugerida

- Criar uma regra simples:
  - nada e removido diretamente se ja foi publicado na matriz;
  - primeiro vira `Deprecated`;
  - depois pode virar `Removed` em uma versao futura;
  - a wiki deve indicar quando uma feature esta depreciada.
- Para depreciar uma feature, registrar:
  - motivo;
  - substituto, se houver;
  - ultima versao/execucao em que foi considerada ativa;
  - impacto nos baselines.
- Para remover uma feature, garantir:
  - retirada do catalogo ativo;
  - preservacao do historico, se aplicavel;
  - atualizacao dos snapshots/testes;
  - atualizacao da wiki.

##### Checklist de implementacao

- [x] Definir estados `Deprecated` e `Removed`.
- [x] Documentar regra de depreciacao.
- [x] Atualizar validador para aceitar depreciado de forma controlada.
- [x] Garantir que feature depreciada nao entre como feature nova em comparativos futuros.
- [x] Registrar primeira revisao de legado/fallback.

##### Criterio de conclusao

- Existe caminho claro para remover benchmark antigo.
- O historico continua interpretavel.
- A matriz nao mistura feature ativa com feature depreciada sem sinalizacao.

#### 5.11 Ordem recomendada dentro da Fase 5

Para nao se perder na implementacao, executar nesta ordem:

1. Definir status oficiais de feature e status oficiais de execucao.
2. Criar `BenchmarkStableId`.
3. Atualizar catalogo e validador para exigir categoria, status e ID estavel.
4. Definir schema JSON intermediario de resultado.
5. Atualizar exportadores para consumir o schema estruturado.
6. Criar legenda e landing page da wiki.
7. Criar perfil `smoke`.
8. Criar testes do catalogo, politica e exportadores.
9. Registrar ambiente no resultado.
10. Criar baseline inicial.
11. Criar comparador baseline vs execucao atual.
12. Ligar fluxo manual de execucao e validacao.
13. Adicionar logs estruturados e `RunId`.
14. Formalizar depreciacao/remocao.

#### 5.12 Entregaveis finais da Fase 5

Ao final desta fase, devem existir:

- Documento de governanca de features.
- Status oficial de feature.
- Status oficial de execucao.
- ID estavel para cada benchmark.
- Schema JSON de resultado documentado.
- Registro de ambiente por execucao.
- Perfis `smoke`, `core`, `full` e `diagnostic`.
- Testes de catalogo, politica e exportacao.
- Baseline inicial.
- Comparador de regressao.
- Landing page da wiki.
- Legenda global de status.
- Logs estruturados com `RunId`.
- Politica de depreciacao.

#### 5.13 Riscos e cuidados

- Nao usar nome de metodo como identidade historica do benchmark.
- Nao publicar resultado sem ambiente e perfil.
- Nao tratar feature nao suportada como falha.
- Nao tratar falha inesperada como skip.
- Nao comparar resultados de perfis diferentes como se fossem equivalentes.
- Nao mudar categoria de feature sem registrar no historico.
- Nao deixar script de wiki depender de parse fragil de texto livre.
- Nao rodar benchmark real em provider que nao suporta a feature.
- Nao remover benchmark publicado sem etapa de depreciacao.

### Fase 6 - Execucao operacional da governanca

Objetivo: transformar as decisoes da Fase 5 em artefatos reais e manter a operacao do benchmark previsivel apos o fechamento estrutural. Esta fase nao deve reabrir a arquitetura de suites; ela deve apenas materializar governanca, validacao, publicacao e manutencao.

#### 6.1 Identidade e catalogo

- Declarar `BenchmarkStableId` para todas as features ativas.
- Registrar status oficial de feature no catalogo.
- Garantir validacao contra IDs duplicados e categorias ausentes.
- Manter a separacao explicita entre `Comparable`, `MockOnly`, `Deprecated` e `Removed`.

#### 6.2 Resultado estruturado

- Formalizar o schema JSON de resultado.
- Registrar `Environment`, `BenchmarkStableId`, `BenchmarkFeatureId`, `ProviderId`, `Engine`, `SuiteName`, `MethodName` e `Category`.
- Garantir que os exportadores consumam o schema estruturado sem heuristicas fragilizadas por texto livre.

#### 6.3 Publicacao e wiki

- Atualizar os geradores para produzir o comparativo por provider e a matriz mock-only.
- Garantir landing page, legenda global e links de navegacao consistentes.
- Preservar a regra de publicacao em arquivos previsiveis dentro de `docs/Wiki`.

#### 6.4 Validacao e perfis

- Consolidar os perfis `smoke`, `core`, `full` e `diagnostic`.
- Registrar o ambiente de execucao no resultado publicado.
- Separar skip esperado, feature nao suportada e falha inesperada.

#### 6.5 Historico e depreciacao

- Manter o historico de mudancas de feature.
- Registrar toda mudanca de categoria e toda remocao formal.
- Usar a politica de depreciacao para evitar que o catalogo volte a misturar feature ativa com legado.
- Manter `Deprecated` como estado rastreavel no catalogo e nos exportadores.
- Reservar `Removed` para remocao formal depois de registrar o historico correspondente.
- Nao reintroduzir recurso removido como comparavel sem uma nova definicao de feature.

#### 6.6 Ordem pratica da Fase 6

Para manter a fase operacional sem dispersar o escopo, executar nesta ordem:

1. Definir o schema JSON final de resultado.
2. Ligar `BenchmarkStableId` ao catalogo e aos exportadores.
3. Consolidar o bloco `Environment` e os perfis `smoke`, `core`, `full` e `diagnostic`.
4. Atualizar os geradores da wiki para consumir o schema estruturado.
5. Registrar a politica de status de feature no catalogo.
6. Criar testes para catalogo, exportacao e politica de execucao.
7. Formalizar o historico de mudancas e a politica de depreciacao.
8. Fechar a fase somente depois de validar que o resultado publicado e rastreavel por ID estavel, ambiente e perfil.

##### Checklist de implementacao da Fase 6

- [x] Implementar o schema JSON final de resultado.
- [x] Ligar `BenchmarkStableId` ao catalogo e aos exportadores.
- [x] Consolidar a publicacao da wiki a partir do schema estruturado.
- [x] Formalizar os perfis de execucao e o registro de ambiente.
- [x] Criar os testes de catalogo, exportacao e politica de execucao.
- [x] Registrar o ciclo de depreciacao e remocao.

##### Fechamento da Fase 6

- A governanca operacional agora esta materializada no catalogo, no schema, nos exportadores e nos testes.
- O status de ciclo de vida ficou visivel no catalogo publicado e na wiki gerada.
- A fase pode ser considerada concluida sem reabrir a arquitetura de suites.

##### Ponto de parada da Fase 6

- A primeira iteracao desta fase deve começar por `schema JSON` e `BenchmarkStableId`.
- Se o catalogo ainda nao estiver pronto para esses campos, a proxima parada deve ser ajustar o catalogo antes de mexer nos exportadores.

##### Criterio de conclusao da Fase 6

- O resultado publicado e rastreavel por ID estavel, ambiente e perfil.
- A wiki e gerada sem ambiguidade de categoria ou status.
- O catalogo e os exportadores falham de forma previsivel quando a governanca e violada.

### Fase 7 - Orquestracao unica da publicacao

Objetivo: reduzir a friccao operacional da publicacao da wiki sem reabrir a arquitetura de suites. Esta fase deve apenas agrupar os comandos ja existentes em um fluxo unico e previsivel.

#### 7.1 Fluxo unico de exportacao

- Criar um orquestrador que gere as duas matrizes principais em uma unica chamada.
- Preservar os scripts especializados para execucoes manuais, validacao pontual e suporte legado.
- Evitar que a publicacao dependa de sequencia manual de comandos espalhados no README.

#### 7.2 Consistencia de saida

- Manter os mesmos caminhos previsiveis em `docs/Wiki`.
- Preservar o fluxo opcional da variante `single-table` como legado explicito, sem inclui-la no fluxo padrao.
- Garantir que o orquestrador nao altere o contrato do catalogo ou do schema.

#### 7.3 Checklist inicial da Fase 7

- [x] Criar o orquestrador unico de publicacao para as duas matrizes principais.
- [x] Documentar o novo atalho de exportacao no README do benchmark.
- [x] Avaliar se a variante `single-table` deve permanecer apenas como atalho legado opcional ou ser removida em uma fase futura.

- Decisao: manter `single-table` apenas como atalho legado opcional por enquanto.
- A remocao, se vier, deve acontecer em fase futura independente.

#### 7.4 Ponto de parada da Fase 7

- A primeira iteracao deve consolidar a publicacao unica sem mexer no catalogo nem no schema.
- Se surgir necessidade de mudar a forma de saida, a proxima parada deve ser apenas no orquestrador, nao na estrutura dos benchmarks.

##### Fechamento da Fase 7

- O orquestrador unico de publicacao ficou consolidado como fluxo normal.
- A variante `single-table` permaneceu apenas como atalho legado opcional.
- O README e o roadmap ficaram alinhados com o fluxo real de exportacao.
- A fase pode ser considerada concluida sem reabrir a arquitetura de suites.

### Fase 8 - Higiene final de XML docs e contratos publicos

Objetivo: fechar a ultima passada de documentacao publica dos tipos novos do benchmark, manter a consistencia EN/PT e reduzir o risco de `CS1591` em superficies que passaram a ser expostas pela governanca operacional.

#### 8.1 Tipos e registros publicos do runner

- Garantir `param` docs nos `record` publicos novos do runner.
- Revisar se algum contrato publico recente ainda carece de `summary` ou `param`.
- Manter o padrao EN primeiro e PT em seguida.

#### 8.2 Catálogo e validacao publica

- Verificar a documentacao publica dos tipos de catalogo e validacao que passaram a sustentar a governanca.
- Preservar a distincao entre tipos internos de infraestrutura e contratos publicos exportados.

#### 8.3 Checklist inicial da Fase 8

- [x] Revisar `BenchmarkRunOptions` e demais registros publicos novos do runner.
- [x] Identificar e corrigir eventuais lacunas de XML docs nas superfices publicas tocadas pela Fase 6 e Fase 7.
- [x] Manter o roadmap alinhado com o criterio de documentacao publicavel antes de qualquer nova consolidacao.

#### 8.4 Ponto de parada da Fase 8

- A primeira iteracao deve começar pelos `record` publicos do runner e pelos contratos que foram introduzidos na Fase 6.
- Se nao houver mais lacunas publicas, a proxima parada deve ser fechar a fase em consolidacao documental, sem reabrir a arquitetura de suites.

##### Criterio de conclusao da Fase 8

- Os tipos publicos novos do benchmark estao documentados em EN/PT.
- As superfices publicas recentes nao geram `CS1591` por falta de resumo ou parametros.
- A documentacao do runner, do catalogo e da exportacao permanece consistente com a estrutura real do benchmark.

##### Fechamento da Fase 8

- A higiene final de XML docs foi concluida nas superficies publicas novas do benchmark.
- O runner, o catalogo, os contratos de suporte e os contratos de suite ficaram alinhados ao padrao EN/PT-br.
- A fase pode ser considerada concluida sem reabrir a arquitetura de suites ou os contratos estruturais do benchmark.

### Fase 9 - Higiene final dos contratos compartilhados do runtime

Objetivo: continuar a padronizacao de XML docs nos contratos compartilhados do runtime e dos geradores sem reabrir a arquitetura de suites do benchmark. Esta fase trata dos atributos e helpers publicos que sustentam a geracao de SQL e a integracao com as camadas compartilhadas.

#### 9.1 Atributos e contratos compartilhados

- Normalizar a documentacao dos atributos compartilhados usados por geradores e mapeadores.
- Manter o padrao EN primeiro e PT-br depois.
- Preservar a leitura factual do contrato, sem texto genérico.

#### 9.2 Helpers e contratos de suporte

- Revisar os contratos publicos dos helpers centrais que ainda sustentam as camadas compartilhadas.
- Fechar as lacunas de `summary` e `param` que ainda aparecam fora do benchmark puro.

#### 9.3 Checklist inicial da Fase 9

- [x] Documentar o contrato compartilhado `ScalarFunctionAttribute`.
- [x] Revisar os demais atributos publicos compartilhados usados pelos geradores e pelos helpers centrais.
- [x] Normalizar os contratos publicos de suporte que ainda mantenham `PT:` residual.

#### 9.4 Ponto de parada da Fase 9

- A primeira iteracao deve começar pelos atributos compartilhados com uso direto pelos geradores.
- Se nao houver mais lacunas relevantes, a proxima parada deve ser fechar a fase em consolidacao documental sem reabrir a arquitetura de suites.

##### Criterio de conclusao da Fase 9

- Os contratos compartilhados novos e relevantes estao documentados em EN/PT-br.
- Os atributos e helpers publicos centrais nao mantêm texto inconsistente de documentacao.
- O runtime compartilhado permanece coerente com a superficie publica do benchmark e dos geradores.

##### Fechamento da Fase 9

- A higiene documental das superficies compartilhadas do runtime foi concluida.
- Os atributos, helpers e contratos de suporte publicos do `src` ficaram alinhados ao padrao EN/PT-br.
- A fase pode ser considerada concluida sem reabrir a arquitetura de suites ou a geracao de SQL.

#### 9.5 Anotacao explicita dos handlers do benchmark

- Todos os `Run*` de `BenchmarkSessionBase` passaram a carregar `BenchmarkFeatureAttribute`.
- O registry continua podendo validar a cobertura por atributo sem depender de um switch manual.
- A cobertura ficou explicita para os 258 handlers de benchmark localizados em `BenchmarkSessionBase*.cs`.
- O metodo dispatcher `RunFeature` permanece sem atributo por ser o ponto de roteamento, nao um handler de benchmark.
- As cinco implementacoes override de `Run*` fora da base principal tambem passaram a carregar `BenchmarkFeatureAttribute`, cobrindo `RunStoredProcedureCall` e os tres overloads de `TemporaryTable`.

## Pontos de verificacao

- Nao pode existir benchmark real de provider executando em provider sem suporte.
- Todo benchmark mock-only deve permanecer disponivel em todos os providers.
- Todo wrapper publico deve continuar mapeado no catalogo.
- A matriz comparativa deve ser publicada em um arquivo indice mais um arquivo por banco/provider.
- A matriz mock-only deve continuar sendo gerada em um arquivo unico.
- A documentacao de entrada deve refletir a estrutura real, sem afirmar que nomes de metodo e enum sao sempre iguais.
- O caminho de exportacao deve ser previsivel e consumir dados estruturados, nao heuristicas fragilizadas por texto solto.
- A estrutura de pastas deve tornar obvio o dominio do benchmark sem abrir varios arquivos.

- Toda feature deve ter categoria, status e identificador estavel.
- Toda mudanca de categoria deve estar registrada no historico de governanca.
- Toda execucao publicada deve informar ambiente, perfil, data e schema de resultado.
- `Skipped`, `NotSupported` e `Failed` devem ser estados diferentes no resultado estruturado.
- A wiki deve possuir legenda global para status e ausencia de dados.
- A comparacao historica deve usar ID estavel, provider, engine e perfil.
- A rotina manual deve ter pelo menos um fluxo `smoke` para validar catalogo, scripts e geracao da wiki.
- O sistema de benchmark deve ter testes proprios para catalogo, politica de execucao e exportadores.
- Feature depreciada deve continuar rastreavel ate ser removida formalmente.

## Historico

- 2026-05-01: planilha criada para organizar a revisao estrutural dos benchmarks apos a separacao de sequence, o ajuste do catalogo e a definicao das duas matrizes de saida.
- 2026-05-01: adicionada Fase 5 para cobrir governanca, confiabilidade estatistica, reprodutibilidade, execucao manual, observabilidade, UX da wiki, testes do sistema de benchmark e depreciacao.
- 2026-05-01: adicionada Fase 7 para consolidar a orquestracao unica da publicacao da wiki sem reabrir a arquitetura de suites.
- 2026-05-01: iteracao 25 registrada; o restante do bloco relacional duplicado foi removido de `BenchmarkSessionBase.cs` e a proxima parada fica em `RunResetVolatileData`, inicio do bloco de lifecycle/schema.
- 2026-05-01: iteracao 26 registrada; o lifecycle de conexao foi extraido para `BenchmarkSessionBase.Lifecycle.cs` e a proxima parada fica em `RunSchemaSnapshotExport`.
- 2026-05-01: iteracao 27 registrada; o bloco `SchemaSnapshot` foi extraido para `BenchmarkSessionBase.SchemaSnapshot.cs` e a proxima parada fica em `RunFluentSchemaBuild`.
- 2026-05-01: iteracao 28 registrada; o bloco `Fluent*` foi extraido para `BenchmarkSessionBase.Fluent.cs` e `BenchmarkSessionBase.cs` agora termina em `RunPartitionPruningSelect`.
- 2026-05-01: iteracao 29 registrada; o ciclo de runtime externo foi extraido para `ExternalBenchmarkSessionBase.Runtime.cs` e a proxima parada fica em `LogBenchmarkIssue` em `BenchmarkSuiteBase.cs`.
- 2026-05-01: iteracao 30 registrada; o bloco `json` foi extraido para `BenchmarkSuiteBase.Json.cs`, o bloco `temporal` para `BenchmarkSuiteBase.Temporal.cs`, e a proxima parada fica em `SqlServerMetadataFunctions`.
- 2026-05-01: iteracao 31 registrada; o bloco `core` inicial foi extraido para `BenchmarkSuiteBase.Core.cs`, o bloco `window` para `BenchmarkSuiteBase.Window.cs`, e a proxima parada fica em `SelectExistsPredicate`.
- 2026-05-01: iteracao 32 registrada; o bloco `advancedquery` ate `SelectPagedNameProjection` foi extraido para `BenchmarkSuiteBase.AdvancedQuery.cs` e a proxima parada fica em `BatchReaderMultiResult`.
- 2026-05-01: iteracao 33 registrada; o bloco `batch`/`parser`/`returning` foi extraido para `BenchmarkSuiteBase.Batch.cs` e a proxima parada fica em `PartitionPruningSelect`.
- 2026-05-01: iteracao 34 registrada; a triagem final do restante de `BenchmarkSessionBase.cs` ainda aponta `Sequence`, `Batch`/`RowCount`, helpers `SqlServer`-specific, `Parse`, `Returning`/`Merge` e `PartitionPruningSelect` como os ultimos clusters concentrados, e a proxima parada fica em extrair primeiro o bloco com melhor relacao ganho/risco antes de fechar a fase com consolidacao e documentacao.
- 2026-05-01: iteracao 35 registrada; a regra de execucao manual foi consolidada no roadmap, a linguagem de pipeline/CI foi removida da governanca de benchmark, e a proxima parada fica em manter essa regra refletida em qualquer novo item de benchmark ou roteiro relacionado.
- 2026-05-01: iteracao 36 registrada; a regra de execucao manual segue como criterio de base para novos itens do roadmap, sem pipeline ou GitHub Actions para a rodagem dos benchmarks, e a proxima parada fica em manter essa linguagem consistente em futuras revisoes do catalogo e da documentacao.
- 2026-05-01: iteracao 37 registrada; a base manual continua valendo para a execucao dos benchmarks e a proxima parada fica em revisar qualquer novo texto de apoio para nao reintroduzir linguagem de pipeline, CI ou automacao de rodagem.
- 2026-05-01: iteracao 38 registrada; a revisao de apoio permanece alinhada com execucao manual, sem pipeline ou GitHub Actions para a rodagem, e a proxima parada fica em manter esse criterio nas proximas atualizacoes do roadmap e da documentacao adjacente.
- 2026-05-01: iteracao 39 registrada; a documentacao de benchmark continua presa ao fluxo manual de execucao e a proxima parada fica em revisar novos trechos para evitar qualquer retorno de linguagem ligada a pipeline, CI ou automacao de rodagem.
- 2026-05-01: iteracao 40 registrada; a manutencao da documentacao segue orientada por execucao manual e a proxima parada fica em manter a ausencia de referencias a pipeline, CI ou GitHub Actions nos proximos ajustes do roadmap.
- 2026-05-01: iteracao 41 registrada; a revisao atual segue ancorada na execucao manual dos benchmarks e a proxima parada fica em preservar a mesma regra ao expandir ou revisar qualquer nova secao do roadmap.
- 2026-05-01: iteracao 42 registrada; a extracao estrutural foi reforcada em parciais focados para `BenchmarkSessionBase`, `BenchmarkSuiteBase`, `ExternalBenchmarkSessionBase` e `DbSqlLikeMemBenchmarkSessionBase`, e a proxima parada fica em revisar os ultimos metodos remanescentes na base comum para decidir se ainda ha uma familia final que vale separar ou se o fechamento deve ser apenas consolidacao.
- 2026-05-01: iteracao 43 registrada; `RunNestedSavepointFlow` foi movido para `BenchmarkSessionBase.DmlMutation.cs`, a familia de transacao/savepoint ficou mais coerente, e a proxima parada fica em triagem dos ultimos metodos remanescentes na base comum, com foco especial em `RowCount` e nos blocos de query/diagnostico que ainda nao justificam um partial separado.
- 2026-05-01: iteracao 44 registrada; `RunRowCountAfterInsert`, `RunRowCountAfterUpdate` e `RunRowCountAfterSelect` foram movidos para `BenchmarkSessionBase.DmlInsert.cs`, `BenchmarkSessionBase.DmlMutation.cs` e `BenchmarkSessionBase.Relational.cs`, e a proxima parada fica em decidir se os metodos restantes de query e helpers SQL Server ainda justificam um ultimo recorte ou se a fase deve encerrar em consolidacao.
- 2026-05-01: iteracao 45 registrada; os helpers SQL Server e o agregado percentual sairam para `BenchmarkSessionBase.SqlServer.cs` e o bloco batch puro foi separado em `BenchmarkSessionBase.Batch.cs`, e a proxima parada fica em triagem da cauda relacional restante (`CteSimple`, `PivotCount`, `Returning*`, `MergeBasic` e `PartitionPruningSelect`) para decidir se ainda existe um corte pequeno ou se o fechamento agora e apenas consolidacao.
- 2026-05-01: iteracao 46 registrada; `Sequence`, `Batch`, `DateScalar` e `Parse` sairam da classe comum para `BenchmarkSessionBase.Sequence.cs`, `BenchmarkSessionBase.Batch.cs`, `BenchmarkSessionBase.Temporal.cs` e `BenchmarkSessionBase.Parse.cs`, e a proxima parada fica em consolidar a arvore final dos parciais, revisar a consistencia de contratos e encerrar a fase sem abrir novos blocos.
- 2026-05-01: iteracao 47 registrada; a classe comum ficou reduzida ao esqueleto compartilhado e todos os grupos concretos restantes ja estao em parciais proprios, e a proxima parada fica em revisar consistencia de contratos, nomes, XML docs e superficie publica antes de considerar a fase concluida.
- 2026-05-01: iteracao 48 registrada; a triagem de consistencia confirmou que `BenchmarkSessionBase.cs` ficou sem rotinas concretas de benchmark e que os novos parciais estao documentados, e a proxima parada fica em revisao fina de nomes, contratos e XML docs antes de encerrar a fase sem novas extracoes.
- 2026-05-01: iteracao 49 registrada; a revisao fina de consistencia nao encontrou rotinas concretas restantes na classe comum nem contratos soltos nos parciais novos, e a proxima parada fica em fechamento da fase e consolidacao final do backlog sem abrir novos blocos estruturais.
- 2026-05-01: iteracao 50 registrada; a limpeza de documentação corrigiu os resumos `PT` restantes para `PT-br` nos parciais novos, sem alterar a estrutura ja consolidada, e a proxima parada fica em fechamento formal da fase e eventual inicio de uma nova etapa apenas se houver novo escopo material.
- 2026-05-01: iteracao 51 registrada; a ultima revisao confirmou que o ajuste de `PT-br` em `BenchmarkSessionBase.Temporal.cs` e `BenchmarkSessionBase.Batch.cs` fechou a limpeza de documentacao dos parciais novos, e a proxima parada fica em encerrar formalmente a fase sem novos ajustes estruturais.
- 2026-05-01: iteracao 52 registrada; a ultima varredura corrigiu o `PT` remanescente em `BenchmarkSessionBase.Lifecycle.cs` e nao encontrou mais resumos fora do padrao `PT-br`, entao a proxima parada fica em marcar esta fase como encerrada e tratar qualquer novo trabalho como uma nova etapa independente.
- 2026-05-01: iteracao 53 registrada; a fase de coerencia estrutural ficou encerrada com a classe comum reduzida ao esqueleto compartilhado, os grupos concretos isolados em parciais proprios e a documentacao alinhada ao padrao `PT-br`, e qualquer proximo trabalho devera nascer como uma nova etapa independente.
- 2026-05-01: iteracao 54 registrada; a Fase 6 foi aberta para materializar a governanca operacional do benchmark sem reabrir a estrutura de suites, e a proxima parada fica em schema JSON, `BenchmarkStableId` e validacao de catalogo como primeira frente pratica dessa nova etapa.
- 2026-05-01: iteracao 55 registrada; a Fase 6 ganhou uma ordem pratica de execucao e um ponto de parada inicial, com prioridade para `schema JSON`, `BenchmarkStableId` e catalogo, e a proxima parada fica em materializar o primeiro desses artefatos sem reabrir a estrutura de suites.
- 2026-05-01: iteracao 56 registrada; o catalogo de benchmark agora aceita `StableId` como chave opcional e os exportadores da wiki passam a resolver essa chave quando presente, entao a proxima parada fica em materializar o schema JSON estrutural da Fase 6 e, em seguida, ligar esse schema ao catalogo publicado sem alterar a superficie dos benchmarks.
- 2026-05-01: iteracao 57 registrada; o schema JSON estrutural foi materializado em `benchmark-result.schema.json` e documentado no README do projeto de benchmark, entao a proxima parada fica em ligar esse contrato ao catalogo publicado e depois preencher o catalogo com `StableId` explicito quando houver identificador estavel diferente do enum.
- 2026-05-01: iteracao 58 registrada; o catálogo publicado agora referencia `benchmark-result.schema.json` no topo dos JSONs de feature map e o README aponta para esse contrato, entao a proxima parada fica em preencher `StableId` explicito apenas nas features em que o identificador estavel divergir do enum e validar se o exportador precisa de mais alguma regra antes de fechar a fase.
- 2026-05-01: iteracao 59 registrada; os exportadores da wiki agora validam e carregam o `resultSchema` publicado no topo do catalogo e a analise local nao encontrou ainda uma divergencia real que justificasse preencher `StableId` em massa, entao a proxima parada fica em decidir quais alias merecem `StableId` canônico explicito ou, se o catálogo permanecer igual ao enum, seguir para `Environment` e perfis de execucao como proxima frente operacional.
- 2026-05-01: iteracao 60 registrada; o runner de benchmark agora reconhece os perfis `smoke`, `core`, `full` e `diagnostic` e preserva o perfil no identificador do job, o README passou a documentar esse uso e a proxima parada fica em fechar os metadados de `Environment` do resultado estruturado sem reabrir a arquitetura de suites.
- 2026-05-01: iteracao 61 registrada; o runner agora grava `docs/Wiki/BenchmarkResults/benchmark-run.environment.json` com perfil e metadados basicos do ambiente e o README passou a citar esse manifest, entao a proxima parada fica em conectar esse contexto de ambiente ao exportador estruturado e, so depois, decidir se ainda vale expandir `StableId` explicito para aliases canônicos.
- 2026-05-01: iteracao 62 registrada; os exportadores da wiki agora reconhecem opcionalmente `benchmark-run.environment.json` e injetam o contexto de ambiente no cabecalho gerado, entao a proxima parada fica em decidir se ainda existe algum alias que mereca `StableId` canônico explicito ou se a Fase 6 pode avançar para a consolidacao final da politica de ambiente e perfis.
- 2026-05-01: iteracao 63 registrada; o manifest de ambiente foi alinhado ao shape estruturado com `environment` aninhado, os exportadores aceitam tanto o formato novo quanto o legado e a proxima parada fica em consolidar a fase final sem abrir novos aliases ou novas estruturas de execucao.
- 2026-05-01: iteracao 64 registrada; os `Run*` faltantes de `BenchmarkSessionBase` foram restaurados como wrappers de compatibilidade nos parciais de `Batch`, `Relational` e `JoinTemporal`, e a proxima parada fica em revisar se ainda resta algum nome quebrado ou apenas fechar a consolidacao final desta frente.
- 2026-05-01: iteracao 65 registrada; a varredura de consistencia nao encontrou outro nome órfão no bloco relacional/batch/join temporal e a proxima parada fica em fechar a consolidacao final desta frente, com foco apenas em estabilidade e eventual limpeza de comentario/documentacao.
- 2026-05-01: iteracao 66 registrada; o alias `RunSelectScalarCaseMatrix` foi restaurado para fechar o ultimo `CS0103` do bloco relacional, e a proxima parada fica em verificar se ainda sobra algum nome historico pendente ou apenas encerrar a consolidacao final desta frente.
- 2026-05-01: iteracao 67 registrada; o dispatcher de `BenchmarkSessionBase` passou a ser registrado por reflexao com `BenchmarkFeatureAttribute`, a aliasagem pontual da matriz escalar CASE foi movida para o atributo, e a proxima parada fica em revisar se ainda sobra algum ajuste de consistencia ou apenas consolidar o fechamento desta frente.
- 2026-05-01: iteracao 68 registrada; o dispatcher segue agora um registry por reflexao em vez de um switch gigante, com `BenchmarkFeatureAttribute` servindo de ponto de alias pontual, e a proxima parada fica em revisar se ainda existe algum ajuste de consistencia no mapeamento ou apenas fechar a consolidacao final.
- 2026-05-01: iteracao 69 registrada; o registry foi ajustado para invocar os handlers por bloco explicito e reduzir risco de incompatibilidade no dispatch, e a proxima parada fica em revisar a consistencia final do mapeamento e encerrar a consolidacao desta frente.
- 2026-05-01: iteracao 70 registrada; foi adicionada cobertura de smoke para o alias `SelectScalarCaseMatrix` atraves do registry por atributo, e a proxima parada fica em continuar a fase operacional do plano com a proxima frente ainda aberta no roadmap.
- 2026-05-01: iteracao 71 registrada; foi adicionada cobertura da politica de execucao para o parser de perfis `smoke`, `core`, `full` e `diagnostic`, e a proxima parada fica em seguir com a governanca do roadmap em vez de parar na consolidacao do dispatcher.
- 2026-05-01: iteracao 72 registrada; o catalogo passou a expor status de ciclo de vida e alguns aliases foram marcados como `Deprecated`, com cobertura de teste para a nova politica, e a proxima parada fica em formalizar a regia de remocao futura e fechar a fase operacional sem perder o historico.
- 2026-05-01: iteracao 73 registrada; o validador agora rejeita qualquer feature marcada como `Removed` que ainda seja comparavel e o teste de catalogo garante que o inventario atual nao exponha recursos removidos, entao a proxima parada fica em fechar a regra de depreciacao/remocao e consolidar a fase sem perder o historico.
- 2026-05-01: iteracao 74 registrada; o catalogo exportado e a documentacao principal agora sinalizam `Deprecated` de forma explicita e a regra de `Removed` ficou reservada para remocao formal, entao a proxima parada fica em consolidar o historico de mudancas e encerrar a Fase 6 sem reabrir a arquitetura de suites.
- 2026-05-01: iteracao 75 registrada; a politica de historico e depreciacao ganhou regra operacional explicita no roadmap para manter `Deprecated` rastreavel e reservar `Removed` para remocao formal, e a proxima parada fica em consolidar apenas o fechamento da Fase 6 sem abrir novos blocos estruturais.
- 2026-05-01: iteracao 76 registrada; mais aliases explicitos foram marcados como `Deprecated`, o teste de politica foi ampliado para cobrir os aliases de math tambem e os exportadores da wiki passaram a exibir o status do catalogo, entao a proxima parada fica em consolidar o fechamento da Fase 6 com a leitura publica do ciclo de vida ja exposta.
- 2026-05-01: iteracao 77 registrada; as tabelas da wiki tiveram as linhas de categoria realinhadas com a nova coluna `Status`, entao a proxima parada fica em fechar a Fase 6 com a leitura publica consistente em ambos os formatos de exportacao.
- 2026-05-01: iteracao 78 registrada; a matriz app-specific agora segue a mesma leitura publica de `Status` e a documentacao foi alinhada ao comportamento novo, entao a proxima parada fica apenas em encerrar formalmente a Fase 6 sem reabrir estruturas.
- 2026-05-01: iteracao 79 registrada; o checklist da Fase 6 foi fechado e a fase passou a estar formalmente concluida no roadmap, entao qualquer novo trabalho deve nascer como uma fase ou item independente.
- 2026-05-01: iteracao 80 registrada; a Fase 7 foi aberta com um orquestrador unico de publicacao para as duas matrizes principais e a proxima parada fica em decidir o destino da variante `single-table` sem tocar na estrutura dos benchmarks.
- 2026-05-01: iteracao 81 registrada; a variante `single-table` foi mantida apenas como atalho legado opcional e a documentacao foi alinhada para nao coloca-la no fluxo padrao de publicacao.
- 2026-05-01: iteracao 82 registrada; a documentacao da Fase 7 agora expõe os comandos especializados e o orquestrador unico sem esconder o uso manual dos scripts antigos, e a proxima parada fica em manter o fluxo de publicacao simples e previsivel sem abrir novos blocos.
- 2026-05-01: iteracao 83 registrada; o roadmap foi alinhado ao orquestrador unico e ao suporte legado opcional da variante `single-table`, e a proxima parada fica em consolidar apenas o fluxo normal de publicacao sem abrir novos formatos.
- 2026-05-01: iteracao 84 registrada; o README e o roadmap foram alinhados ao mapeamento real por alias e wrappers, e a proxima parada fica em manter a Fase 7 apenas como consolidacao do fluxo normal de publicacao.
- 2026-05-01: iteracao 85 registrada; a Fase 7 foi fechada no roadmap com o orquestrador unico consolidado e a variante `single-table` mantida apenas como legado opcional, entao qualquer novo trabalho deve nascer como uma fase independente.
- 2026-05-01: iteracao 86 registrada; o `BenchmarkRunOptions` recebeu documentacao completa de parametros e a proxima parada fica em revisar os demais registros publicos novos do runner para fechar a higiene final de XML docs da Fase 8.
- 2026-05-01: iteracao 87 registrada; os docs novos do runner foram normalizados para `PT-br` em `Program.cs` e a proxima parada fica em identificar e corrigir eventuais lacunas de XML docs nas superficies publicas tocadas pela Fase 6 e Fase 7.
- 2026-05-01: iteracao 88 registrada; `BenchmarkCatalogValidator` e `ProviderDefinition` foram normalizados para `PT-br` nos contratos publicos e a proxima parada fica em revisar o restante das superficies publicas recentes ate a fase poder ser fechada em consolidacao documental.
- 2026-05-01: iteracao 89 registrada; os contratos menores de `BenchmarkEngine`, `BenchmarkLogPath`, `FeatureCatalog` e `IBenchmarkSession` foram normalizados para `PT-br`, e a proxima parada fica em reduzir o restante do `PT` que ainda concentra em `BenchmarkFeatureId` e em alguns contratos de suite antes de poder fechar a fase.
- 2026-05-01: iteracao 90 registrada; o residual de `PT:` foi eliminado dos contratos publicos do benchmark e dos testes de suporte, a Fase 8 ficou concluida no roadmap e a proxima parada passa a ser uma nova etapa independente caso ainda haja escopo para continuar.
- 2026-05-01: iteracao 91 registrada; o contrato compartilhado `ScalarFunctionAttribute` foi documentado e a Fase 9 foi aberta para continuar a higiene documental nos atributos e helpers publicos do runtime compartilhado.
- 2026-05-01: iteracao 92 registrada; os atributos xUnit `MemberDataVersionAttribute` e `MemberDataByVersionAttribute` foram normalizados para `PT-br`, e a proxima parada fica em revisar os demais atributos e helpers publicos compartilhados com escopo real de uso pelos geradores e contratos de suporte.
- 2026-05-01: iteracao 93 registrada; a varredura em `src` nao encontrou mais `PT:` residual, a Fase 9 foi concluida no roadmap e qualquer novo trabalho devera nascer como uma nova etapa independente.
- 2026-05-01: iteracao 94 registrada; todos os 258 handlers `Run*` de `BenchmarkSessionBase` passaram a carregar `BenchmarkFeatureAttribute` explicitamente, fechando a cobertura completa pedida para o dispatcher por atributo.
- 2026-05-01: iteracao 95 registrada; o roadmap passou a explicitar que `RunFeature` permanece sem atributo por ser o dispatcher central, enquanto a cobertura segue completa nos 258 handlers reais de benchmark.
- 2026-05-01: iteracao 96 registrada; as duas implementacoes override de `RunStoredProcedureCall` em `DbSqlLikeMemBenchmarkSessionBase` e `ExternalBenchmarkSessionBase` tambem foram anotadas, fechando o restante dos `Run*` relevantes fora da familia principal.
- 2026-05-01: iteracao 97 registrada; os tres overrides de `TemporaryTable` em `DbSqlLikeMemBenchmarkSessionBase` tambem foram anotados, fechando os cinco `protected Run*` fora da base principal e deixando a cobertura total em 263 handlers protegidos no benchmark.
- 2026-05-01: iteracao 98 registrada; o arquivo de historico de mudancas de features foi criado em `benchmark-feature-history.md`, a separacao inicial entre `Comparable` e `MockOnly` entrou como primeira entrada e o checklist de governanca da Fase 5 foi consolidado como concluido.
- 2026-05-01: iteracao 99 registrada; a origem canonica do identificador estavel ficou fixada em `FeatureDefinition.StableId` com fallback para o nome do enum, e o checklist de identidade estavel foi consolidado como concluido no roadmap.
- 2026-05-01: iteracao 100 registrada; o documento de ambiente de benchmark foi criado em `benchmark-environment.md`, os perfis oficiais e as regras de comparacao ficaram documentados e o checklist de reprodutibilidade passou a refletir apenas os metadados ainda nao persistidos.
- 2026-05-01: iteracao 101 registrada; o guia de status foi criado em `benchmark-execution-status.md`, o schema de resultado ficou documentado em termos de campos e leitura publica, e o checklist de qualidade estatistica foi consolidado como concluido.
- 2026-05-01: iteracao 102 registrada; o guia de baseline e regressao foi criado em `benchmark-baseline.md`, a estrategia de armazenamento por perfil e a chave de comparacao ficaram documentadas, e o checklist de baseline passou a destacar apenas a automacao de comparacao e o resumo publicado ainda em aberto.
- 2026-05-01: iteracao 103 registrada; o validador unico de catalogo e mapas foi criado em `validate-catalog-and-maps.ps1`, o runbook manual foi documentado em `benchmark-manual-runbook.md` e o checklist de execucao manual passou a refletir o fluxo smoke/core/full ja apoiado pelos scripts existentes.
- 2026-05-01: iteracao 104 registrada; o comparador de baseline foi criado em `compare-benchmark-baseline.ps1`, o resumo de regressao passou a ser publicado em `docs/Wiki/BenchmarkResults/benchmark-regression-summary.md` e o checklist de baseline/regressao ficou concluido no roadmap.
- 2026-05-01: iteracao 105 registrada; o `RunId` passou a ser gerado por execucao, os logs foram segmentados por correlacao e o guia de observabilidade foi criado em `benchmark-observability.md`, fechando o checklist de diagnostico do fluxo.
- 2026-05-01: iteracao 106 registrada; a landing page da wiki foi criada em `docs/Wiki/BenchmarkResults/Home.md` e `Home.pt-BR.md`, a legenda global e os atalhos de regressao ficaram publicados, e o checklist de UX da wiki foi consolidado como concluido.
- 2026-05-01: iteracao 107 registrada; as versoes declaradas no catalogo foram documentadas em `benchmark-environment.md`, a politica de comparacao foi explicitada em `benchmark-baseline.md`, e os checklists de reproducibilidade, testes e depreciacao foram consolidados como concluido no roadmap.
- 2026-05-01: iteracao 108 registrada; a politica de execucao por provider foi coberta por teste no projeto de benchmark, consolidando mais um item da Fase 5.9 sem ainda fechar o snapshot da wiki.
- 2026-05-01: iteracao 109 registrada; o snapshot da landing page da wiki foi coberto com fixtures para `Home.md` e `Home.pt-BR.md`, fechando a ultima pendencia material da Fase 5.9.
- 2026-05-01: iteracao 110 registrada; o resumo de regressao publicado tambem passou a ter snapshot de teste, fechando a cobertura de publicacao dos artefatos de wiki sem depender de validacao manual.
- 2026-05-01: iteracao 111 registrada; o manifesto de ambiente publicado tambem passou a ter snapshot de teste, fechando a ultima borda de contrato serializado ainda nao travada na superficie publica do benchmark.
- 2026-05-01: iteracao 112 registrada; os guias publicados de ambiente e baseline tambem passaram a ter snapshot de teste, travando a borda documental que sustenta a comparacao historica e a reproducibilidade.
- 2026-05-01: iteracao 113 registrada; os guias publicos de status, historico de features e runbook manual tambem passaram a ter snapshot de teste, fechando mais uma borda documental da governanca do benchmark.
- 2026-05-01: iteracao 114 registrada; o guia de observabilidade e o README publico do benchmark tambem passaram a ter snapshot de teste, fechando a ultima borda de apoio documental que ainda estava aberta na superficie publica.
- 2026-05-01: iteracao 115 registrada; os testes de snapshot foram consolidados em um helper compartilhado no projeto de benchmark, reduzindo duplicacao sem mudar o contrato publicado.
- 2026-05-01: iteracao 116 registrada; os mapas publicados de features agora tambem passam por teste de parse estruturado, fechando a cobertura publica dos JSONs de catalogo sem depender de execucao real.
- 2026-05-01: iteracao 117 registrada; os testes de mapa passaram a comparar os IDs publicados com o catalogo em codigo, fechando a protecao contra drift entre JSON publicado e catalogo do runtime.
- 2026-05-01: iteracao 118 registrada; os mapas publicados tambem passaram a validar os IDs de features contra o catalogo em codigo, fechando a ultima protecao pratica contra drift de enum e categoria no JSON publicado.
- 2026-05-01: iteracao 119 registrada; a cobertura de mapa foi refinada para garantir que features comparaveis nao fiquem orfas no JSON publicado, sem amarrar indevidamente o ciclo de vida ao teste de contrato.
- 2026-05-01: iteracao 120 registrada; a cobertura de mapa passou a validar metadados publicados de provider contra o catalogo em codigo, fechando drift adicional de identidade e engine nos JSONs publicados.
- 2026-05-01: iteracao 121 registrada; o schema de resultado ganhou teste proprio de contrato top-level e de `environment`, fechando a protecao direta do JSON estruturado publicado.
- 2026-05-01: iteracao 122 registrada; a cobertura de mapa passou a travar as organizacoes publicadas por tipo de mapa, fechando drift de agrupamento estrutural sem mexer no contrato de features.
- 2026-05-01: iteracao 123 registrada; a cobertura de mapa foi refinada para tratar `SqlAzure` como o unico provider nao comparavel no mapa comparativo atual, removendo o assert invalido e preservando a politica de proxy explicita no contrato publicado.
