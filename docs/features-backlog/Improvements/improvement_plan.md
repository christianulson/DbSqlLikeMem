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

**Plano:**
- Extrair `DbConnectionTransactionJournal` — encapsula journal de rollback, savepoints, e replay
- Extrair `DbConnectionDebugTraceManager` — gerencia traces, retenção, e formatação
- Extrair `DbConnectionExecutionPlanCapture` — captura, registro, e cache de planos
- Extrair `DbConnectionSchemaSnapshotBridge` — import/export de schema snapshots
- Extrair `DbConnectionSessionState` — session sequences, context values, transaction context
- Manter `DbConnectionMockBase` como fachada que delega para componentes

> [!IMPORTANT]
> Não alterar a API pública da classe. Os componentes devem ser `internal`.

### 1.2 Decomposição do `TableMock` (~3.360 linhas)

**Problema:** Mistura armazenamento de dados, gerenciamento de índices/PK, triggers, particionamento, colunas computadas, foreign keys, e bulk operations.

**Plano:**
- Extrair `TablePartitionRouter` — parsing e lookup de partições (já existe `PartitionRoutingInfo`, mas a lógica de `TryInferRequestedPartitionNames*` está no `TableMock`)
- Extrair `TableTriggerManager` — registro, remoção, execução de triggers
- Extrair `TableIndexManager` — PK index, secondary indexes, unique constraints
- Extrair `TableForeignKeyManager` — foreign keys, validação referencial
- Manter `TableMock` como orquestrador de dados e colunas

### 1.3 Decomposição do `AstQueryExecutorBase` (~3.400 linhas)

**Problema:** Classe base monolítica com inicialização lazy de ~15 sub-evaluators internos, lógica de select/join/filter/group/project/order, e binding manual de delegates.

**Plano:**
- Já existe alguma decomposição (helpers, evaluators), mas a classe ainda concentra `ExecuteSelect`, `ExecuteUnion`, `BuildFrom`, `ApplyJoin`, `ProjectRows`, `ExecuteGroup`
- Extrair `AstQuerySelectPipeline` — orchestração do pipeline select (from → join → filter → group → project → order → limit)
- Extrair `AstQueryEvalContext` — unifica a resolução de parâmetros, scopes locais, e outer rows
- Considerar tornar os lazy evaluators injetáveis via construtor ao invés de propriedades lazy

### 1.4 Decomposição do `DbSelectIntoAndInsertSelectStrategies` (~3.900 linhas)

**Problema:** Arquivo monolítico que concentra dispatch de NonQuery, DDL handlers (create/drop view, table, index, sequence, function, procedure, trigger), e execução de EXECUTE BLOCK.

**Plano:**
- Extrair `DbDdlViewStrategy` — create/drop view
- Extrair `DbDdlIndexStrategy` — create/drop index
- Extrair `DbDdlSequenceStrategy` — create/drop/alter sequence
- Extrair `DbDdlFunctionProcedureStrategy` — create/drop function/procedure/trigger
- Extrair `DbExecuteBlockStrategy` — execute block, exception handlers, loop control
- Manter `DbSelectIntoAndInsertSelectStrategies` como dispatcher central (switch expression)

### 1.5 Decomposição do `CommandScalarExecutionPrelude` (~1.360 linhas)

**Problema:** Mistura fast-path scalar evaluation, temporal function handling, JSON function evaluation, CASE expression evaluation, e date arithmetic.

**Plano:**
- Extrair `ScalarTemporalEvaluator` — date construction, DATEADD, interval parsing
- Extrair `ScalarJsonEvaluator` — JSON_EXTRACT, JSON_VALUE, JSON_QUERY, JSON_MERGE
- Extrair `ScalarExpressionEvaluator` — constante/CASE/binary/boolean evaluation
- Manter `CommandScalarExecutionPrelude` como entry point que delega

---

## 🟢 Onda 2 — Qualidade de Código (Média Prioridade)

### 2.1 Remover supressão global de CS1591

**Estado atual:** O `.csproj` contém `<NoWarn>$(NoWarn);CS1591</NoWarn>` que suprime warnings de documentação em todo o projeto.

**Plano:**
- Remover a supressão do `.csproj`
- Fazer build para levantar quais membros públicos/protected estão sem XML doc
- Preencher os summaries seguindo o padrão EN/PT do `AGENTS.md`
- Usar `<inheritdoc />` em overrides quando a base já documenta o contrato
- Meta: **zero CS1591** sem supressão

### 2.2 Corrigir typos em nomes de arquivo e classe

| Arquivo | Problema | Correção |
|---|---|---|
| `SqlStringExtencions.cs` | "Extencions" | `SqlStringExtensions.cs` |
| Comentário em `TableMock.cs` L723, L779 | "Vollumn" | "Column" |

### 2.3 Habilitar analyzers .NET comentados

**Estado atual:** No `Directory.Build.props`, as linhas de `EnableNETAnalyzers`, `AnalysisModeSecurity`, `AnalysisMode`, `AnalysisLevel` estão comentadas (L27–L31).

**Plano:**
- Descomentar gradualmente, começando por `AnalysisModeSecurity=All` (security rules)
- Depois habilitar `AnalysisMode=Recommended` (não `All`) para evitar ruído excessivo
- Tratar ou suprimir seletivamente os warnings gerados

### 2.4 Reduzir uso de `static readonly ConcurrentDictionary` como cache sem eviction

**Problema:** `AstQueryExecutorBase` tem ~6 `ConcurrentDictionary` estáticos usados como cache (`_dateTimeParseCache`, `_dateTimeExactParseCache`, `_dateTimeOffsetParseCache`, etc.) com soft limits que nunca são enforced.

**Plano:**
- Implementar eviction policy baseado em `TemporalParseCacheSoftLimit`
- Usar `TrimExcess()` ou substituir por bounded cache quando o limite for atingido
- Considerar `MemoryCache` para cenários de lifetime-management mais sofisticados

---

## 🟡 Onda 3 — Melhorias Arquiteturais (Média Prioridade)

### 3.1 Reduzir acoplamento da `ISqlDialect` (361 linhas, ~100 membros)

**Problema:** Interface monolítica com ~100 membros misturando parser capabilities, runtime semantics, DDL support, e function registries. Qualquer novo provider precisa implementar/override dezenas de membros.

**Plano:**
- Agrupar membros em sub-interfaces: `ISqlDialectParser`, `ISqlDialectRuntime`, `ISqlDialectDdl`, `ISqlDialectFunctions`
- `ISqlDialect` compõe as sub-interfaces (backward compatible)
- Mover cada grupo de `SupportsOracle*` para um `IOracleDialectExtensions` (extension methods ou interface segregada)
- Mover cada grupo de `SupportsSqlServer*` para `ISqlServerDialectExtensions`

### 3.2 Introduzir `partial class` para arquivos grandes existentes

Para arquivos que não podem ser decompostos sem breaking changes imediatos, usar `partial class` para separar logicamente:

- `DbConnectionMockBase` → `DbConnectionMockBase.Transaction.cs`, `DbConnectionMockBase.DebugTrace.cs`, etc.
- `TableMock` → `TableMock.Partition.cs`, `TableMock.Trigger.cs`, etc.

> [!TIP]
> Essa abordagem pode ser usada como step intermediário antes da decomposição completa da Onda 1.

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
- Criar helper `DbMock.ExecuteWithLock<T>(Func<T> action)` que encapsula o padrão
- Substituir todas as ocorrências (estimativa: ~15 métodos DDL)

---

## 🔶 Onda 4 — Evoluções de Funcionalidade (Baixa Prioridade)

### 4.1 Adicionar target `net9.0`

**Estado atual:** Targets são `net462`, `netstandard2.0`, `net8.0`.

**Plano:**
- Adicionar `net9.0` ao `TargetFrameworks` (ou `net10.0` se o .NET 10 já estiver estável)
- Usar APIs mais eficientes quando disponíveis via `#if NET9_0_OR_GREATER`
- Benefícios: `SearchValues<T>`, `FrozenDictionary`, melhorias em `Span<T>`, `Lock` type

### 4.2 Expandir `SchemaSnapshot` com suporte a check constraints e computed columns

**Estado atual:** `UnsupportedObjectKinds` lista explicitamente `check-constraints`, `computed-default-expressions`, `computed-column-generators` como não suportados.

**Plano:**
- Adicionar `CheckConstraints` a `SchemaSnapshotTable`
- Serializar/deserializar `ColumnDef.ComputedExpression` quando presente
- Mover da lista de `UnsupportedObjectKinds` para `SupportedObjectKinds`

### 4.3 Lazy initialization do Dialect com Function Registries

**Problema:** Cada dialeto monta seu `FunctionDictionaryProcess` eagerly no construtor, registrando centenas de funções escalares, de janela, e de tabela.

**Plano:**
- Tornar o registro lazy (on-first-access por categoria)
- Ou usar registros estáticos compartilhados (`SqlSharedScalarFunctionRegistry` já existe, expandir o padrão)
- Benefício: reduzir tempo de startup em cenários com muitas instâncias de conexão

### 4.4 Implementar `ReadOnlySpan<char>` onde possível no Parser

**Problema:** O tokenizer e expression parser usam `string` extensivamente, gerando muitas alocações intermediárias.

**Plano:**
- Substituir substrings por `ReadOnlySpan<char>` / `ReadOnlyMemory<char>` no tokenizer
- Usar `StringComparison` com spans em comparações hot-path
- Aproveitar polyfills existentes em `Compatibility/ReadOnlySpanCompatibility.cs`

### 4.5 Source Generators para registrar funções escalares

**Problema:** Os registries de funções escalares usam chamadas manuais repetitivas para registrar cada função.

**Plano:**
- Criar source generator que lê atributos `[ScalarFunction("UPPER")]` em métodos estáticos
- Gerar automaticamente o código de registro no `FunctionDictionaryProcess`
- Benefício: reduzir boilerplate e riscos de esquecer de registrar uma função

---

## 🟣 Onda 5 — Dívida Técnica e Housekeeping

### 5.1 Consolidar polyfills de compatibilidade

**Estado atual:** 8 arquivos em `Compatibility/` com polyfills para `Range`/`Index`, `HashSet`, `ReadOnlySpan`, `StringBuilder`, `ITuple`, etc.

**Plano:**
- Revisar quais polyfills ainda são necessários dado que `net462` é o menor target
- Considerar usar o pacote `PolySharp` para simplificar
- Documentar quais polyfills são para qual target

### 5.2 Revisar `InternalsVisibleTo` no `.csproj`

**Estado atual:** 41 entradas `InternalsVisibleTo` listadas manualmente.

**Plano:**
- Verificar se todas as entradas são ainda necessárias (projetos podem ter sido removidos)
- Considerar usar `[assembly: InternalsVisibleTo]` em um único `AssemblyInfo.cs` com comentários por grupo

### 5.3 Unificar padrão de `ArgumentExceptionCompatible` / `ArgumentNullExceptionCompatible`

**Problema:** Uso inconsistente entre `ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace` e `ArgumentNullExceptionCompatible.ThrowIfNull`.

**Plano:**
- Padronizar em um único helper ou migrar para os métodos built-in do .NET 8+ com `#if`
- Remover duplicação

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
