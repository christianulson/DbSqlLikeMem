# Plano de Melhorias e Evoluções — DbSqlLikeMem (Core)

> **Escopo:** Projeto `C:\Projects\DbSqlLikeMem\src\code\DbSqlLikeMem`
> **Data:** 2026-04-22
> **Versão analisada:** 1.18.1

---

## 📊 Diagnóstico Geral

O projeto é uma engine SQL in-memory com suporte a **8 dialetos** (SqlServer, SqlAzure, Npgsql, MySql, MariaDb, Sqlite, Oracle, Db2, Firebird), integração com **5 ORMs** (Dapper, EF Core, NHibernate, HNibernate, LinqToDb), pipeline de interceptação ADO.NET, e infra de schema snapshot. A base de código é madura, bem documentada (EN/PT), com multi-target (net462, netstandard2.0, net8.0) e já possui analyzers Roslyn habilitados.

### Métricas de complexidade dos maiores arquivos

| Arquivo | Linhas | Bytes | Risco |
|---|---:|---:|---|
| `Strategies/DbSelectIntoAndInsertSelectStrategies.cs` | 3.892 | 144 KB | 🔴 Crítico |
| `Query/Execution/AstQueryExecutorBase.cs` | 3.403 | 138 KB | 🔴 Crítico |
| `Base/DbConnectionMockBase.cs` | 3.306 | 122 KB | 🔴 Crítico |
| `Models/TableMock.cs` | 3.359 | 119 KB | 🔴 Crítico |
| `Parser/SqlExpressionParser.cs` | 2.182 | 79 KB | 🟡 Alto |
| `SchemaSnapshot.cs` | 1.412 | 59 KB | 🟡 Alto |
| `Query/Execution/Aggregates/AstQueryAggregateEvaluator.cs` | ~1.300 | 55 KB | 🟡 Alto |
| `Parser/SqlQueryParser.cs` | ~1.300 | 49 KB | 🟡 Alto |
| `CommandScalarExecutionPrelude.cs` | 1.360 | 49 KB | 🟡 Alto |
| `Dialect/Dialects.cs` | 1.053 | 43 KB | 🟡 Médio |

---

## 🔵 Onda 1 — Refatorações Estruturais (Alta Prioridade)

Reduzir complexidade ciclomática e melhorar manutenibilidade dos "god classes".

### 1.1 Decomposição do `DbConnectionMockBase` (~3.300 linhas)

**Problema:** Concentra responsabilidades de conexão, transação, journal de rollback, schema snapshot, debug traces, execution plans, sequences, triggers, e tabelas temporárias em uma única classe.

- **Plano:**
- Extrair `DbConnectionTransactionJournal` — encapsula journal de rollback, savepoints, e replay ✅ (manager criado em `Base/DbConnectionTransactionJournalManager.cs`)
- Extrair `DbConnectionTransactionStateManager` — concentra id de transação, savepoints, e estado transitório ✅ (implementado em `Base/DbConnectionTransactionStateManager.cs`)
- Extrair `DbConnectionSessionStateManager` — concentra sequences de sessão, context values, e context info ✅ (implementado em `Base/DbConnectionSessionStateManager.cs`)
- Extrair `DbConnectionDebugTraceManager` — gerencia traces, retenção, e formatação ✅ (implementado em `Base/DbConnectionDebugTraceManager.cs`)
- Extrair `DbConnectionExecutionPlanCapture` — captura, registro, e cache de planos ✅ (implementado em `Base/DbConnectionExecutionPlanManager.cs`)
- Extrair `DbConnectionSchemaSnapshotBridge` — import/export de schema snapshots ✅ (implementado em `Base/DbConnectionSchemaSnapshotBridge.cs`)
- Manter `DbConnectionMockBase` como fachada que delega para componentes

> [!IMPORTANT]
> Não alterar a API pública da classe. Os componentes devem ser `internal`.

### 1.2 Decomposição do `TableMock` (~3.360 linhas)

**Problema:** Mistura armazenamento de dados, gerenciamento de índices/PK, triggers, particionamento, colunas computadas, foreign keys, e bulk operations.

**Plano:**
- Extrair `TablePartitionRouter` — parsing e lookup de partições ✅ (implementado em `Models/TablePartitionRouter.cs`)
- Extrair `TableTriggerManager` — registro, remoção, execução de triggers ✅ (implementado em `Models/TableTriggerManager.cs` e usado diretamente pelas strategies)
- Extrair `TableIndexManager` — PK index, secondary indexes, unique constraints (parcial: índices secundários, PK lookup/mutation/rebuild, register/remove helpers, batch PK uniqueness, update-if-needed, build helper reuse pelo `DbInsertStrategy`, conflict/unique validation direto nas strategies, update de índices direto nas strategies e no insert path, replay de drop index direto no helper, rebuild/dirty direto pelo `TableStateManager`, remoção de wrappers de índice em `TableMock`, e unique checks em `Models/TableIndexManager.cs`)
- Extrair `TableForeignKeyManager` — foreign keys, validação referencial ✅ (implementado em `Models/TableForeignKeyManager.cs` e usado diretamente pelas strategies)
- Extrair `TableStateManager` — backup, restore e replay de estado ✅ (implementado em `Models/TableStateManager.cs`, com rebuild de PK/indexes delegado ao `TableIndexManager`)
- Manter `TableMock` como orquestrador de dados e colunas

### 1.3 Decomposição do `AstQueryExecutorBase` (~3.400 linhas)

**Problema:** Classe base monolítica com inicialização lazy de ~15 sub-evaluators internos, lógica de select/join/filter/group/project/order, e binding manual de delegates.

**Plano:**
- Já existe alguma decomposição (helpers, evaluators), mas a classe ainda concentra `ExecuteSelect`, `ExecuteUnion`, `BuildFrom`, `ApplyJoin`, `ProjectRows`, `ExecuteGroup`
- Extrair `AstQuerySourcePipeline` — `BuildFrom`, `ResolveSource`, `ApplyJoin`, e a preparação de origem/parsing de partições ✅ (implementado em `Query/Execution/AstQueryExecutorBase.SourcePipeline.cs`)
- Extrair `AstQueryProjectionPipeline` — `ProjectRows`, `ProjectGrouped`, e `HasSqlCalcFoundRows` ✅ (implementado em `Query/Execution/AstQueryExecutorBase.Projection.cs`)
- Extrair `AstQueryExecutionOrchestration` — `ExecuteUnion`, `ExecuteSelect`, e contadores de `UNION` simples ✅ (implementado em `Query/Execution/AstQueryExecutorBase.Execution.cs`)
- Extrair `AstQueryExpressionEvaluation` — `Eval`, `EvalIdentifier`, `EvalIsNull`, `EvalCall`, `ContainsParameter`, e helpers imediatos ✅ (implementado em `Query/Execution/AstQueryExecutorBase.ExpressionEval.cs`)
- Extrair `AstQueryFunctionEvaluation` — `EvalFunction`, `FunctionEvaluator`, families de função e hooks de dialeto ✅ (implementado em `Query/Execution/AstQueryExecutorBase.Functions.cs`)
- Extrair `AstQueryTemporalParsing` — `ParseIntervalValue`, `TryParseSplitIntervalArguments`, `TryParseIntervalLiteral`, `ApplyDateDelta`, `TruncateDateTime`, `GetTemporalPartValue`, `TryParseDateModifier`, `TryConvertIntervalToTimeSpan`, `TryCoerceDateTime`, `TryCoerceTimeSpan`, e caches de parse ✅ (implementado em `Query/Execution/AstQueryExecutorBase.Temporal.cs`)
- Extrair `AstQuerySelectPipeline` — `TryEvaluateSimpleStringAggregate`, `TryCountSimpleRows`, `TryCountRowsFromPrimaryKey`, `ExecuteGroup`, `ApplyRowPredicate`, `ApplyHavingPredicate`, e `BuildHavingEvaluationContext` ✅ (implementado em `Query/Execution/AstQueryExecutorBase.SelectPipeline.cs`)
- Extrair `AstQueryEvalContext` — `_localParameterScopes`, `AttachOuterRow`, e `AttachOuterRows` ✅ (implementado em `Query/Execution/AstQueryExecutorBase.EvalContext.cs`)
- Considerar tornar os lazy evaluators injetáveis via construtor ao invés de propriedades lazy

### 1.4 Decomposição do `DbSelectIntoAndInsertSelectStrategies` (~3.900 linhas)

**Problema:** Arquivo monolítico que concentra dispatch de NonQuery, DDL handlers (create/drop view, table, index, sequence, function, procedure, trigger), e execução de EXECUTE BLOCK.

**Plano:**
- Extrair `DbDdlViewStrategy` — create/drop view ✅ (implementado em `Strategies/DbDdlViewStrategy.cs`)
- Extrair `DbDdlIndexStrategy` — create/drop index ✅ (implementado em `Strategies/DbDdlIndexStrategy.cs`)
- Extrair `DbDdlSequenceStrategy` — create/drop/alter sequence ✅ (implementado em `Strategies/DbSelectIntoAndInsertSelectStrategies.Sequence.cs`)
- Extrair `DbDdlFunctionProcedureStrategy` — create/drop function/procedure/trigger ✅ (implementado em `Strategies/DbSelectIntoAndInsertSelectStrategies.FunctionProcedure.cs`)
- Extrair `DbExecuteBlockStrategy` — execute block, exception handlers, loop control ✅ (implementado em `Strategies/DbSelectIntoAndInsertSelectStrategies.ExecuteBlock.cs`)
- Manter `DbSelectIntoAndInsertSelectStrategies` como dispatcher central (switch expression)

### 1.5 Decomposição do `CommandScalarExecutionPrelude` (~1.360 linhas)

**Problema:** Mistura fast-path scalar evaluation, temporal function handling, JSON function evaluation, CASE expression evaluation, e date arithmetic.

**Plano:**
- Extrair `ScalarTemporalEvaluator` — date construction, DATEADD, interval parsing ✅ (implementado em `CommandScalarExecutionPrelude.Evaluators.cs`)
- Extrair `ScalarJsonEvaluator` — JSON_EXTRACT, JSON_VALUE, JSON_QUERY, JSON_MERGE ✅ (implementado em `CommandScalarExecutionPrelude.Evaluators.cs`)
- Extrair `ScalarExpressionEvaluator` — constante/CASE/binary/boolean evaluation ✅ (implementado em `CommandScalarExecutionPrelude.Expressions.cs`; o entry point do arquivo principal já delega)
- Manter `CommandScalarExecutionPrelude` como entry point que delega

---

## 🟢 Onda 2 — Qualidade de Código (Média Prioridade)

### 2.1 Remover supressão global de CS1591

**Estado atual:** A supressão global de `CS1591` já foi removida do `.csproj`; `SqlConst.cs`, `BatchMetricKeys.cs`, `DbPerformanceMetricKeys.cs`, `SqlUnsupported.cs`, `CommandScalarExecutionPrelude`, `LinqQueryExecutor`, `TranslationResult`, `FrameworkPolyfills.cs`, `RangeIndexPolyfill.cs`, `TableResultMock.cs`, `DbConnectionExecutionPlanManager.cs`, `DbConnectionSchemaSnapshotBridge.cs`, `DbConnectionTransactionStateManager.cs` e `DbConnectionSessionStateManager.cs` já foram documentados, e a próxima etapa é fechar os summaries que o compilador ainda apontar quando a validação for executada.

**Plano:**
- Remover a supressão do `.csproj`
- Fazer build para levantar quais membros públicos/protected estão sem XML doc
- Preencher os summaries seguindo o padrão EN/PT do `AGENTS.md`
- Usar `<inheritdoc />` em overrides quando a base já documenta o contrato
- Meta: **zero CS1591** sem supressão

### 2.2 Corrigir typos em nomes de arquivo e classe

| Arquivo | Problema | Correção | Estado |
|---|---|---|---|
| `SqlStringExtencions.cs` | "Extencions" | `SqlStringExtensions.cs` | ✅ Feito |
| Comentário em `TableMock.cs` L723, L779 | "Vollumn" | "Column" | ✅ Feito |

### 2.3 Habilitar analyzers .NET comentados

**Estado atual:** No `Directory.Build.props`, as linhas de `EnableNETAnalyzers`, `AnalysisModeSecurity`, `AnalysisMode`, `AnalysisLevel` estão comentadas (L27–L31).

**Plano:**
- Descomentar gradualmente, começando por `AnalysisModeSecurity=All` (security rules)
- Depois habilitar `AnalysisMode=Recommended` (não `All`) para evitar ruído excessivo
- Tratar ou suprimir seletivamente os warnings gerados

### 2.4 Reduzir uso de `static readonly ConcurrentDictionary` como cache sem eviction

**Problema:** `AstQueryExecutorBase` tem ~6 `ConcurrentDictionary` estáticos usados como cache (`_dateTimeParseCache`, `_dateTimeExactParseCache`, `_dateTimeOffsetParseCache`, etc.) com soft limits que nunca são enforced.

**Plano:**
- Implementar eviction policy baseado em `TemporalParseCacheSoftLimit` ✅ (bounded trim implementado nas caches temporais em `Query/Execution/AstQueryExecutorBase.Temporal.cs`)
- Usar `TrimExcess()` ou substituir por bounded cache quando o limite for atingido
- Considerar `MemoryCache` para cenários de lifetime-management mais sofisticados

---

## 🟡 Onda 3 — Melhorias Arquiteturais (Média Prioridade)

### 3.1 Reduzir acoplamento da `ISqlDialect` (361 linhas, ~100 membros)

**Problema:** Interface monolítica com ~100 membros misturando parser capabilities, runtime semantics, DDL support, e function registries. Qualquer novo provider precisa implementar/override dezenas de membros.

**Plano:**
- Extrair `ISqlDialectFunctions` — lookup de funções escalares, agregadas, com retorno em tabela, de janela e temporais ✅ (implementado em `Interfaces/ISqlDialectFunctions.cs`)
- Extrair `ISqlDialectParser` — quoting, strings, keywords, operadores e compatibilidade de parser ✅ (implementado em `Interfaces/ISqlDialectParser.cs`)
- Extrair `ISqlDialectDdl` — DDL, sequence e create table support ✅ (implementado em `Interfaces/ISqlDialectDdl.cs`)
- Extrair `ISqlDialectQueryFeatures` — pagination, DML, CTE, query/window functions e aggregates ✅ (implementado em `Interfaces/ISqlDialectQueryFeatures.cs`)
- Extrair `ISqlDialectRuntime` — sequencias, compatibilidade de parser, comparacao e comportamentos especificos de provider ✅ (implementado em `Interfaces/ISqlDialectRuntime.cs`)
- Extrair `ISqlDialectSemantics` — comparacao, strings, regex e helpers semanticos ✅ (implementado em `Interfaces/ISqlDialectSemantics.cs`)
- Extrair `ISqlDialectCompatibility` — sequence syntax, table hints, naming e operator mapping ✅ (implementado em `Interfaces/ISqlDialectCompatibility.cs`)
- Agrupar membros em sub-interfaces: `ISqlDialectParser`, `ISqlDialectRuntime`, `ISqlDialectDdl`, `ISqlDialectQueryFeatures`, `ISqlDialectSemantics`, `ISqlDialectCompatibility`, `ISqlDialectFunctions` ✅
- `ISqlDialect` compõe as sub-interfaces (backward compatible) ✅
- Mover cada grupo de `SupportsOracle*` para `IOracleDialectFeatures` ✅ (implementado em `Interfaces/IOracleDialectFeatures.cs`)
- Mover cada grupo de `SupportsSqlServer*` para `ISqlServerDialectFeatures` ✅ (implementado em `Interfaces/ISqlServerDialectFeatures.cs`)

### 3.2 Introduzir `partial class` para arquivos grandes existentes

Para arquivos que não podem ser decompostos sem breaking changes imediatos, usar `partial class` para separar logicamente:

- `DbConnectionMockBase` → `DbConnectionMockBase.Transaction.cs`, `DbConnectionMockBase.DebugTrace.cs`, etc.
- `TableMock` → `TableMock.Partition.cs`, `TableMock.Trigger.cs`, etc.
- `CommandScalarExecutionPrelude` → `CommandScalarExecutionPrelude.cs`, `CommandScalarExecutionPrelude.Evaluators.cs` (parcial criado; `Temporal`, `Json` e `Expressions` já estão implementados e o arquivo principal ficou como entry point)

> [!TIP]
> Essa abordagem pode ser usada como step intermediário antes da decomposição completa da Onda 1.

- `SqlDialectBase` -> `SqlDialectBase.Identifiers.cs` ✅ (identificadores e aspas separados em parcial próprio)
- `SqlDialectBase` -> `SqlDialectBase.Semantics.cs` ✅ (semantics, window e function capability checks separados em parcial próprio)
- `SqlDialectBase` -> `SqlDialectBase.Capabilities.cs` ✅ (DDL, query compatibility e provider flags separados em parcial próprio)
- `SqlDialectBase` -> `SqlDialectBase.FunctionRegistry.cs` ✅ (lookup de funções e helpers temporais separados em parcial próprio)
- `SqlDialectBase` -> `SqlDialectBase.ParserSettings.cs` ✅ (parser flags e lexer settings separados em parcial próprio)
- `SqlDialectBase` -> `SqlDialectBase.Core.cs` ✅ (constructor, name/version e registries centrais separados em parcial próprio)

### 3.3 Unificar tratamento de thread-safety nas Strategies

**Problema:** Padrão repetitivo de lock condicional em todas as strategies DDL:

```csharp
if (!connection.Db.ThreadSafe)
    affected = ExecuteXxxImpl(connection, query);
else
{
    lock (connection.Db.SyncRoot)
        affected = ExecuteXxxImpl(connection, query);
}
```

**Plano:**
- Helper `DbMock.ExecuteWithLock<T>(Func<T> action)` já existe e foi aplicado nas strategies DDL ✅
- Substituir todas as ocorrências restantes nas strategies DDL ✅

---

## 🔶 Onda 4 — Evoluções de Funcionalidade (Baixa Prioridade)

### 4.1 Adicionar target `net9.0`

**Estado atual:** Targets de produção incluem `net462`, `netstandard2.0`, `net8.0` e `net9.0`; testes e testtools continuam em `net462`, `net6.0`, `net8.0`.

**Plano:**
- `net9.0` já foi adicionado aos `TargetFrameworks` de produção e aos pacotes condicionais que o suportam
- Usar APIs mais eficientes quando disponíveis via `#if NET9_0_OR_GREATER`
- Benefícios: `SearchValues<T>`, `FrozenDictionary`, melhorias em `Span<T>`, `Lock` type

### 4.2 Expandir `SchemaSnapshot` com suporte a check constraints e computed columns

**Estado atual:** O snapshot já carrega `CheckConstraints` por tabela e `ComputedExpression` por coluna; a criação de tabelas temporárias, `CREATE TABLE` bruto, `ALTER TABLE ADD COLUMN` e o replay de snapshot já preservam esses metadados, e o perfil de suporte já reflete `check-constraints` como suportado.

**Plano:**
- Popular `CheckConstraints` nos fluxos restantes de DDL que ainda não expõem essa informação
- Popular `ColumnDef.ComputedExpression` nos fluxos restantes de parser/DDL que ainda não expõem a expressão textual
- Revisar `UnsupportedObjectKinds` quando a cobertura de export/import estiver completa

### 4.3 Lazy initialization do Dialect com Function Registries

**Problema:** Cada dialeto monta seu `FunctionDictionaryProcess` eagerly no construtor, registrando centenas de funções escalares, de janela, e de tabela.

**Plano:**
- Tornar o registro lazy (on-first-access por categoria) ✅ (o registry base e os registries dos dialetos agora são inicializados sob demanda em `Dialect/SqlDialectBase.Core.cs`)
- Ou usar registros estáticos compartilhados (`SqlSharedScalarFunctionRegistry` já existe, expandir o padrão)
- Benefício: reduzir tempo de startup em cenários com muitas instâncias de conexão

### 4.4 Implementar `ReadOnlySpan<char>` onde possível no Parser

**Problema:** O tokenizer e expression parser usam `string` extensivamente, gerando muitas alocações intermediárias.

**Plano:**
- Substituir substrings por `ReadOnlySpan<char>` / `ReadOnlyMemory<char>` no tokenizer e nos helpers de alias/parsing de valores ✅ (alias parsing, simple value parsing e hot paths do tokenizer já usam spans internos)
- Usar `StringComparison` com spans em comparações hot-path ✅ (o tokenizer e o parser de default de parametro já usam spans para matching de keyword e literais)
- Aproveitar polyfills existentes em `Compatibility/ReadOnlySpanCompatibility.cs` ✅ (as rotinas de parse agora usam span nativo em `NET8_0_OR_GREATER`)
- Cobrir parsing de tipos de parametro e de valores simples com spans no hot path ✅ (o parser de DbType de parametro agora compara spans sem `ToUpperInvariant`)
- Cobrir o parsing de corpo de function com spans no hot path ✅ (o helper de body PostgreSQL agora usa `ReadOnlySpan<char>` para trim/prefix/suffix)
- Cobrir o parsing de parametros de function com spans no hot path ✅ (o helper de parametros agora valida o tipo via span antes de materializar a string final)
- Cobrir o splitter bruto por vírgula com spans no hot path ✅ (o helper agora percorre `ReadOnlySpan<char>` e materializa só os itens finais)
- Cobrir o parser de contexto de query com spans no hot path ✅ (o contexto agora materializa apenas o texto final nos cortes de cláusulas e listas)
- Cobrir o parser de `INSERT VALUES` com spans no hot path ✅ (a validação de operador pendente agora usa `ReadOnlySpan<char>` antes do parse da expressao)
- Cobrir o parser de `ORDER BY` com spans no hot path ✅ (o helper agora remove sufixos e monta o item final via `ReadOnlySpan<char>`)
- Cobrir o parser de `INSERT PARTITION` com spans no hot path ✅ (o helper agora normaliza nomes com span antes de materializar a lista final)
- Cobrir o parser de `UPDATE`/`DELETE` com spans no hot path ✅ (a normalização de cláusulas `WHERE` agora usa o helper compartilhado baseado em span)
- Cobrir o parser de `ON CONFLICT`/`ON DUPLICATE KEY` com spans no hot path ✅ (as cláusulas e expressões agora passam pelo normalizador compartilhado em span)
- Cobrir o parser de `JOIN ... ON` com spans no hot path ✅ (a cláusula ON agora usa o texto de cláusula compartilhado normalizado por span)
- Cobrir o parser de `WHERE`/`HAVING` em `SELECT` com spans no hot path ✅ (o parser agora normaliza as cláusulas via `ReadOnlySpan<char>` antes de chamar `ParseWhere`)
- Cobrir o parser de `RETURNING` com spans no hot path ✅ (a separação de alias agora aceita `ReadOnlySpan<char>` antes de montar a expressão final)
- Cobrir o parser de `OPENJSON WITH` com spans no hot path ✅ (o tipo agora é reduzido ao primeiro token via `ReadOnlySpan<char>` antes de mapear o `DbType`)
- Cobrir o parser de `JSON_TABLE` com spans no hot path ✅ (o bloco parenthesizado agora é extraído via `ReadOnlySpan<char>` antes do parse final)
- Cobrir o parser de `PIVOT`/`UNPIVOT` com spans no hot path ✅ (o splitter de itens agora percorre `ReadOnlySpan<char>` e materializa apenas os segmentos finais)
- Cobrir o parser de table/index hints com spans no hot path ✅ (a lista de índices de MySQL agora é normalizada via `ReadOnlySpan<char>` antes da validação)
- Cobrir o parser de `PARTITION` com spans no hot path ✅ (a lista de partitions agora é normalizada via `ReadOnlySpan<char>` antes da validação)
- Cobrir o parser de table-valued functions com spans no hot path ✅ (os argumentos agora são normalizados via `ReadOnlySpan<char>` antes da construção da lista)
- Cobrir o parser de itens de `SELECT` com spans no hot path ✅ (cada item agora é normalizado via `ReadOnlySpan<char>` antes da separação de alias)
- Consolidar o fluxo raw de `ORDER BY`/`UNION` com spans no hot path ✅ (o parser agora usa um único delegado para os cortes de lista)
- Cobrir o parser de hex literals com spans no hot path ✅ (o leitor de bytes agora usa `ReadOnlySpan<char>` por par de dígitos hex)
- Cobrir o unquoting de string literals em `OPENJSON` com spans no hot path ✅ (o helper agora remove prefixo/sufixo via `ReadOnlySpan<char>`)
- Cobrir o parser de `MATCH ... AGAINST` com spans no hot path ✅ (a validação do modo agora compara tokens sem materializar caixa alta intermediária)
- Cobrir o parser de intervalos compactos com spans no hot path ✅ (a validação do token agora evita `ToUpperInvariant()` intermediário)
- Cobrir o operador `::` no tokenizer com span/hot-path check ✅ (a checagem agora usa o span restante com verificação direta do operador)
- Remover `ToUpperInvariant()` redundante do validator de funções ✅ (as mensagens de erro agora usam o nome original sem materialização auxiliar)
- Cobrir as comparações de agregados e o sufixo de sequência no parser de expressão com comparações diretas ✅ (o parser agora evita `ToUpperInvariant()` nas checagens de `WITHIN GROUP`, `SEPARATOR`, `quantified comparison` e sequência)
- Remover `ToUpperInvariant()` redundante do parser de `TRY_PARSE`/`PARSE` especial ✅ (as mensagens de erro agora usam o nome original sem materialização auxiliar)
- Preservar o texto original de keywords no tokenizer ✅ (o hot path agora evita materializar caixa alta ao emitir `SqlTokenKind.Keyword`)

### 4.5 Source Generators para registrar funções escalares

**Problema:** Os registries de funções escalares usam chamadas manuais repetitivas para registrar cada função.

**Plano:**
- Criar source generator que lê atributos `[ScalarFunction("UPPER")]` em métodos estáticos ✅ (infra criada e os primeiros casos reais `OPENJSON`, `JSON_OBJECT`, o bloco simples e o bloco condicional do `SqliteScalarFunctionRegistry`, parte do `MariaDbScalarFunctionRegistry`, os blocos de compatibilidade, temporais, texto, utilitários, rede, binário, arrays, JSON, `CBRT`, regex, `STARTS_WITH`, `CURRENT_*`/`CURRENT_QUERY`, `EXTRACT`, `CAST` e os JSON condicionais/versionados do `NpgsqlScalarFunctionRegistry`, além do bloco de `ROWCOUNT`/`SCOPE_IDENTITY`, dos utilitários simples/versionados, checksum, GUID, `STR`, compressão, temporais zero-arg, `EOMONTH`, `NEXT_VALUE_FOR`, parte do bloco temporal (`DATENAME`, `DATEPART`, `DATETRUNC`, `DATEADD`, `DATEDIFF`, `DATEDIFF_BIG`, `DAY`, `MONTH`, `YEAR`), das funções de conversão (`FORMAT`, `PARSE`, `TRY_PARSE`, `CAST`, `TRY_CAST`, `TRY_CONVERT`) e dos identificadores de usuário/sessão/metadata (`CURRENT_USER`, `SESSION_USER`, `SYSTEM_USER`, `CONNECTIONPROPERTY`, `CONTEXT_INFO`, `ERROR_*`, `DATABASEPROPERTYEX`, `DATABASE_PRINCIPAL_ID`, `COLUMNPROPERTY`, `COL_LENGTH`, `COL_NAME`, `DB_ID`, `DB_NAME`, `OBJECT_ID`, `OBJECTPROPERTY`, `OBJECTPROPERTYEX`, `OBJECT_NAME`, `OBJECT_SCHEMA_NAME`, `ORIGINAL_DB_NAME`, `TYPEPROPERTY`, `CURRENT_REQUEST_ID`, `CURRENT_TRANSACTION_ID`, `IS_MEMBER`, `IS_ROLEMEMBER`, `IS_SRVROLEMEMBER`, `ORIGINAL_LOGIN`, `SESSION_ID`, `SERVERPROPERTY`, `XACT_STATE`) do `SqlServerScalarFunctionRegistry`, e do bloco de temporais simples do `SqliteScalarFunctionRegistry`; o gerador também passou a suportar `MinVersion` para registros condicionais. O restante do bloco de metadata/conversão do `SqlServer` ainda depende de registracões manuais sem `AstExecutor` explícito e ficou para um corte futuro)
- Gerar automaticamente o código de registro no `FunctionDictionaryProcess` (em andamento: o gerador já produz o método parcial de registro no registrador alvo)
- Benefício: reduzir boilerplate e riscos de esquecer de registrar uma função

---

## 🟣 Onda 5 — Dívida Técnica e Housekeeping

### 5.1 Consolidar polyfills de compatibilidade

**Estado atual:** 8 arquivos em `Compatibility/` com polyfills para `Range`/`Index`, `HashSet`, `ReadOnlySpan`, `StringBuilder`, `ITuple`, etc.

**Plano:**
- Revisar quais polyfills ainda são necessários dado que `net462` é o menor target
- Considerar usar o pacote `PolySharp` para simplificar
- Documentar quais polyfills são para qual target
- Enxugar helpers de compatibilidade sem uso real, como sobrecargas mortas de `HashSetCompatibilityExtensions` ✅ (restam apenas os factories que ainda são chamados no código)

### 5.2 Revisar `InternalsVisibleTo` no `.csproj`

**Estado atual:** 41 entradas `InternalsVisibleTo` listadas manualmente.

**Plano:**
- Verificar se todas as entradas são ainda necessárias (projetos podem ter sido removidos)
- Considerar usar `[assembly: InternalsVisibleTo]` em um único `AssemblyInfo.cs` com comentários por grupo
- Migração para arquivo de atributos concluída ✅ (as entradas do assembly raiz agora vivem em `Properties/InternalsVisibleTo.cs`)

### 5.3 Unificar padrão de `ArgumentExceptionCompatible` / `ArgumentNullExceptionCompatible`

**Problema:** Uso inconsistente entre `ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace` e `ArgumentNullExceptionCompatible.ThrowIfNull`.

**Plano:**
- Padronizar em um único helper ou migrar para os métodos built-in do .NET 8+ com `#if`
- Remover duplicação
- Migracao parcial para built-ins do runtime moderno ✅ (os helpers agora chamam `ArgumentNullException.ThrowIfNull` e `ArgumentException.ThrowIfNullOrWhiteSpace` quando disponiveis)

---

## 📋 Priorização Sugerida

| Ordem | Item | Esforço | Impacto | Risco |
|:---:|---|:---:|:---:|:---:|
| 1 | 2.2 — Corrigir typos | Trivial | Baixo | Nulo |
| 2 | 3.2 — Partial classes (step intermediário) | Baixo | Alto | Baixo |
| 3 | 3.3 — Unificar lock pattern nas Strategies | Baixo | Médio | Baixo |
| 4 | 2.1 — Remover supressão CS1591 | Médio | Alto | Baixo |
| 5 | 1.4 — Decompor `DbSelectIntoAndInsertSelectStrategies` | Médio | Alto | Médio |
| 6 | 1.5 — Decompor `CommandScalarExecutionPrelude` | Médio | Alto | Médio |
| 7 | 1.2 — Decompor `TableMock` | Alto | Alto | Médio |
| 8 | 1.1 — Decompor `DbConnectionMockBase` | Alto | Alto | Alto |
| 9 | 1.3 — Decompor `AstQueryExecutorBase` | Alto | Alto | Alto |
| 10 | 3.1 — Segregar `ISqlDialect` | Alto | Alto | Alto |
| 11 | 2.4 — Bounded caches | Médio | Médio | Baixo |
| 12 | 2.3 — Habilitar analyzers | Médio | Médio | Baixo |
| 13 | 4.1 — Target net9.0 | Baixo | Médio | Baixo |
| 14 | 5.1 — Consolidar polyfills | Baixo | Baixo | Baixo |
| 15 | 5.2 — Revisar InternalsVisibleTo | Trivial | Baixo | Nulo |
| 16 | 5.3 — Unificar argument helpers | Baixo | Baixo | Baixo |
| 17 | 4.2 — Schema snapshot check constraints | Médio | Médio | Médio |
| 18 | 4.3 — Lazy dialect function registry | Médio | Médio | Médio |
| 19 | 4.4 — Span no parser | Alto | Médio | Médio |
| 20 | 4.5 — Source generators | Alto | Médio | Alto |

---

> [!NOTE]
> Todas as refatorações devem preservar os testes existentes sem modificação. Execute os testes fidelity de todos os providers após cada onda. A Onda 1 é a mais impactante mas também a mais arriscada — a abordagem de partial classes (3.2) pode servir como step intermediário seguro.
