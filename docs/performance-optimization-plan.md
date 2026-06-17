# Plano de Otimização de Performance

## Contexto

Análise arquitetural aprofundada do repositório `DbSqlLikeMem`, com foco no pipeline de execução SQL (especialmente Sqlite, mas aplicável a todos os providers). Cada item lista o local exato do código, o problema identificado, a abordagem de correção, o impacto esperado e as dependências.

---

## 1. Streaming DataReader — Eliminar Materialização Antecipada

### Problema

O pipeline atual materializa **todas as linhas** em `TableResultMock` (`List<Dictionary<int, object?>>`) antes de qualquer leitura pelo `DbDataReaderMockBase`. Isso acontece em `ProjectRows` (`AstQueryExecutorBase.Projection.cs:12-51`), que itera cada `EvalRow`, aloca um `Dictionary<int, object?>` do pool e o adiciona à `TableResultMock`.

O `DbDataReaderMockBase` apenas incrementa `_currentIndex` em `Read()` (`DbDataReaderMockBase.cs:462-467`) — não há lazy evaluation.

**Localizações:**
- `AstQueryExecutorBase.Projection.cs:12-51` — `ProjectRows()` aloca `TableResultMock` e popula cada linha
- `AstQueryExecutorBase.Execution.cs:158` — `var projectedRows = rows as List<EvalRow> ?? [.. rows];` força materialização
- `TableResultMock.cs:9` — herda de `List<Dictionary<int, object?>>`
- `DbDataReaderMockBase.cs:13-15` — `_resultSets`, `_columnsDic`, `_columnOrdinalByNormalizedName` pré-construídos
- `DbDataReaderMockBase.cs:462-467` — `Read()` apenas incrementa índice

### Abordagem

Introduzir um **LazyDataReader** que implemente `DbDataReader` e envolva o `IEnumerable<EvalRow>` diretamente, sem materializar:

1. Criar `LazyTableResult : ITableResult` que envolve o enumerador de `EvalRow` + `SelectPlan` (avaliadores de projeção)
2. O `LazyTableResult` avalia cada linha sob demanda em `Read()` — chama os `evaluators[i](row)` apenas quando a linha é lida
3. Manter `TableResultMock` para os casos onde acesso não-sequencial é necessário (ORDER BY, DISTINCT, etc.)
4. `AstQueryExecutorBase.ExecuteSelectCore` retorna `ITableResult` (interface nova) em vez de `TableResultMock` concreto
5. O pipeline atual de ORDER BY / DISTINCT / LIMIT força materialização para `TableResultMock` quando necessário

### Condições para ativar o streaming

O streaming só é possível quando o SELECT **não** tem:
- `ORDER BY` (exceto se puder ser resolvido por índice)
- `DISTINCT` / `DISTINCT ON`
- `GROUP BY`
- Window functions
- `FOR JSON`
- Múltiplos result sets que dependem de ordenação global

Nesses casos, manter o comportamento atual de materialização completa.

### Impacto esperado

- **Latência da primeira linha**: reduzida a zero (não precisa processar N linhas antes de retornar a primeira)
- **Alocação de memória**: eliminada a `List<Dictionary<int, object?>>` inteira — o resultado nunca está todo em memória
- **GC pressure**: drasticamente reduzida (não aloca N dicionários por query)
- **Caminho crítico**: toda query SELECT passa por `ProjectRows`

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Query/Support/TableResultMock.cs` | Criar interface `ITableResult`, `LazyTableResult : ITableResult` |
| `src/.../Query/Execution/AstQueryExecutorBase.Projection.cs` | `ProjectRows` retorna `ITableResult` |
| `src/.../Query/Execution/AstQueryExecutorBase.Execution.cs` | `ExecuteSelectCore` retorna `ITableResult` |
| `src/.../Base/DbDataReaderMockBase.cs` | Aceitar `IList<ITableResult>`, ler lazy quando possível |
| `src/.../Query/Execution/Plan/AstQueryOrderLimitHelper.cs` | Forçar materialização se ORDER BY/DISTINCT presente |
| `src/.../Sqlite/SqliteDataReaderMock.cs` | Passar `IList<ITableResult>` |

---

## 2. Expansão do Expression JIT — Compilar Mais Expressões

### Problema

O `CompilePredicate` atual (`AstQueryExecutorBase.cs:429-558`) compila apenas predicados do WHERE com padrões limitados:
- `AND` / `OR` / `NOT`
- `Column = Literal`
- `BETWEEN column AND lit1 AND lit2`
- `LIKE column 'pattern'`
- `IS NULL` / `IS NOT NULL`

Toda expressão que foge desses padrões cai no interpretador `Eval()` (`AstQueryExecutorBase.ExpressionEval.cs:7-132`), que tem um `switch` gigante por tipo de nó e avalia recursivamente.

**Localizações:**
- `AstQueryExecutorBase.cs:429-558` — `TryCompilePredicate` (padrões atuais)
- `AstQueryExecutorBase.ExpressionEval.cs:7-132` — `Eval()` switch
- `AstQueryExecutorBase.cs:417-427` — `CompilePredicate()` cache lookup
- `AstQueryExecutorBase.SelectPipeline.cs:301-326` — `ApplyRowPredicate` usa compiled predicate ou fallback

### Abordagem

Expandir `TryCompilePredicate` para suportar:

1. **Expression trees completas**: Em vez de matching de padrão fixo, usar `System.Linq.Expressions` para compilar qualquer `SqlExpr` em um `Func<EvalRow, object?>` delegate
2. **Incluir expressões do SELECT list**: Criar `TryCompileProjection` para compilar cada `selectItem.Evaluator` em delegate (não só WHERE)
3. **Compilar expressões ORDER BY**: Especialmente colunas simples, para evitar `Eval()` no sort comparator
4. **Cache de expressões compiladas**: Já existe `_compiledPredicateCache` para WHERE; estender para projection/order caches separados

### Alternativa mais pragmática

Em vez de expression trees completos (que exigem lidar com subqueries, window functions, etc.), expandir o pattern matching atual para cobrir:
- `Column + Literal` (arithmetic)
- `Column = Parameter`
- `CAST(Column AS Type)` quando o tipo é compatível
- `COALESCE(Column, Literal)`
- `CASE WHEN Column = Literal THEN Literal ELSE Literal END`

### Impacto esperado

- **WHERE filtering**: já compilado para padrões comuns (~60% coverage). Expandir para ~80%+
- **Projeções**: atualmente nunca compiladas. Cada `selectItem` chama `Eval()` por linha. Compilar reduziria drasticamente o custo de `SELECT` lists grandes (>10 colunas)
- **ORDER BY**: expressões de ordenação atualmente são reavaliadas por comparação

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Query/Execution/AstQueryExecutorBase.cs` | `TryCompilePredicate` expandido, novos métodos `TryCompileProjection`, `TryCompileOrderBy` |
| `src/.../Query/Execution/AstQueryExecutorBase.Projection.cs` | Usar compiled projection delegates quando disponíveis |
| `src/.../Query/Execution/AstQueryExecutorBase.SelectPipeline.cs` | `ApplyRowPredicate` usa compiled predicate (já faz) |

---

## 3. Short-Circuit para Queries Simples (Fast Path)

### Problema

Toda query SELECT passa pelo pipeline completo de `ExecuteSelectCore` (`AstQueryExecutorBase.Execution.cs:106-180`), mesmo queries que poderiam ser resolvidas com um único índice lookup. Exemplo:
```sql
SELECT * FROM T WHERE PK = @p
```
Atualmente passa por: CTE check → BuildFrom → ApplyJoin → WHERE filter → ProjectRows → ORDER BY → LIMIT → FOR JSON.

Já existem alguns short-circuits:
- `TryEvaluateSimpleUnionCount` (Execution.cs:290)
- `TryEvaluateSimpleStringAggregate` (SelectPipeline.cs:5)
- `TryCountSimpleRows` (SelectPipeline.cs:78)
- `TryRowsFromIndex` (SourcePipeline.cs:39) — tenta resolver WHERE via índice

Mas não há um atalho que pule o pipeline inteiro para queries de lookup por PK.

**Localização:**
- `AstQueryExecutorBase.Execution.cs:106-180` — pipeline completo
- `AstQueryExecutorBase.SourcePipeline.cs:39` — `IndexHelper.TryRowsFromIndex`
- `SelectPipeline.cs:78-170` — `TryCountSimpleRows`, `TryCountRowsFromPrimaryKey`

### Abordagem

Adicionar `TryExecuteSimpleSelect` no início de `ExecuteSelectCore`:

1. **Detectar query elegível**: SELECT sem JOIN, sem GROUP BY, sem CTE, sem DISTINCT, sem subquery, sem window function, sem ORDER BY complexo
2. **Detectar acesso por PK**: WHERE = `PKCol = @p` (única condição, operador `=`, coluna é PK)
3. **Fast path**: `_pkIndex.TryGetValue(key, out rowIdx)` → materializar só essa linha
4. **Detectar acesso por índice único**: WHERE = `UniqueIndexCol = @p` → `index.LookupMutable(key)`
5. **Detectar COUNT(*) sem WHERE**: `_items.Count` sem scan

### Impacto esperado

- **Lookup por PK**: de `O(n)` scan para `O(1)` hash lookup (embora `BuildFrom` já use índice parcialmente)
- **COUNT(*)**: de scan completo para `_items.Count` (instantâneo)
- **Beneficiados**: testes de unidade que fazem `SELECT * FROM T WHERE Id = @id` repetidamente
- **Maior ganho**: quando `ProjectRows` tem muitas colunas (>20), eliminar completamente a projeção

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Query/Execution/AstQueryExecutorBase.Execution.cs` | Adicionar `TryExecuteSimpleSelect` check antes do pipeline |
| `src/.../Query/Execution/AstQueryExecutorBase.SelectPipeline.cs` | Expandir `TryCountSimpleRows` para `TryExecuteSimpleSelect` |
| `src/.../Query/Execution/Joins/` | Detector de "query não usa JOIN" |

---

## 4. ArrayPool Generalizado — Reduzir GC Pressure

### Problema

`ArrayPool` é usado em apenas **2 locais** em todo o repositório:
1. `TableMock.BatchInsertContext` — matriz de `IndexKey` (TableMock.cs:827-908)
2. `AstQueryAggregateEvaluator` — buffer `double[]` para PERCENTILE_CONT (AggregateEvaluator.cs:588-589)

Enquanto isso, `ProjectRows` aloca um `Dictionary<int, object?>` por linha via `IntDictPool` (que é um pool de dicionários, não de arrays), e `EvalRow` é alocado como classe.

**Localizações:**
- `TableMock.cs:174` — `List<object?[]> _items` (cada row é array)
- `TableMock.cs:827-908` — único uso de `ArrayPool<IndexKey>`
- `AstQueryExecutorBase.Projection.cs:35` — `IntDictPool.Get()`
- `AstQueryExecutorBase.cs:2278-2327` — pools `SqlRowPool`, `IntDictPool`

### Abordagem

1. **`ArrayPool<object?>` para rows em `ProjectRows`**: alugar arrays em vez de alocar `Dictionary<int, object?>` para cada linha projetada
2. **`ArrayPool<object?>` para colunas de `EvalRow.OrdinalValues`**: reutilizar arrays de valores entre avaliações
3. **`ArrayPool<ColumnDef>` para materializações temporárias de colunas** em joins e subqueries
4. **`StringBuilderPool`** para o parser (SqlTokenizer.cs:225, SqlQueryParser.cs) — usar `StringBuilder` cacheado em vez de alocar novo a cada string literal
5. **Revisar `SqlRowPool`** para usar `ArrayPool` internamente

### Impacto esperado

- **GC Gen-0 collections**: reduzidas significativamente em pipelines de alta vazão
- **Alocação por row em `ProjectRows`**: de `Dictionary<int, object?>` + entry overhead para array rented
- **Parser**: `StringBuilderPool` reduz alocações de `StringBuilder` em batches SQL com muitas strings literais

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Query/Execution/AstQueryExecutorBase.Projection.cs` | `ProjectRows` usar `ArrayPool<object?>` rented arrays |
| `src/.../Parser/SqlTokenizer.cs` | Adicionar `StringBuilderPool` |
| `src/.../Models/TableMock.cs` | Expandir `ArrayPool` uso para `_items.Add` |
| `src/.../Query/Execution/AstQueryExecutorBase.cs` | Revisar pools existentes |

---

## 5. Redução de String Allocations no Tokenizer

### Problema

O `SqlTokenizer` aloca **uma string por token** porque `SqlToken.Text` é `string` (SqlToken.cs). As alocações acontecem em:
- `ReadIdentifierOrKeyword`: `_sql.Substring(startPos, _pos - startPos)` (SqlTokenizer.cs:337)
- `ReadNumber`: `RemainingSlice()` → `AsSpan().ToString()` (SqlTokenizer.cs:246, 257)
- `ReadParameter`: `RemainingSlice()` → `AsSpan().ToString()` (SqlTokenizer.cs:274)
- `TryReadSqlServerSystemVariable`: `_sql[startPos.._pos]` (range operator) (SqlTokenizer.cs:184)
- String literals: `StringBuilder.ToString()` (SqlTokenizer.cs:225)

Para um SQL de ~500 caracteres, tipicamente 50-100 tokens → 50-100 strings alocadas. O cache de prelude (`SqlQueryParsePreludeCache`) evita isso na reexecução do mesmo SQL.

O parser também aloca strings no split de statements: `sql[start..end].Trim()` → `.ToString()` (`SqlStatementSplitter.cs:80`).

**Localizações:**
- `SqlTokenizer.cs:337` — `Substring` para identifiers/keywords
- `SqlTokenizer.cs:184` — range operator para system variables
- `SqlTokenizer.cs:246,257,274` — `AsSpan().ToString()` para numbers/parameters
- `SqlTokenizer.cs:225` — `StringBuilder.ToString()` para string literals
- `SqlStatementSplitter.cs:80` — `sql[start..endExclusive].Trim()` alloc

### Abordagem

1. **Mudar `SqlToken.Text` de `string` para `ReadOnlyMemory<char>`**: adiar a alocação de string até que o texto seja necessário. O token carrega um slice da string original + offset + length
2. **Criar `TokenText` helper** que converte para string sob demanda (cacheando o resultado)
3. **Onde o `Token.Text` é usado** (comparações, keyword detection), continuar usando `Span.Equals(..., OrdinalIgnoreCase)` sem alocar string
4. **Onde uma string é inevitável** (entrada para o AST, como `SqlTableSource.Name`), alocar apenas nesse ponto e reaproveitar
5. **`SqlStatementSplitter`**: usar `ReadOnlySpan<char>` para o split e adiar Trim allocation

### Impacto esperado

- **Parser miss (sem cache)**: elimina 50-100+ alocações de string por SQL parseado
- **GC pressure**: reduzido proporcionalmente
- **Cuidado**: mudança estrutural no tipo `SqlToken.Text` — afeta ~50+ locais de consumo

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Parser/SqlToken.cs` | `Text` de `string` para `ReadOnlyMemory<char>` + helper `ToString()` |
| `src/.../Parser/SqlTokenizer.cs` | Todos os métodos de leitura (8+) |
| `src/.../Parser/SqlQueryParser.cs` | Ajustar consumo de `SqlToken.Text` |
| `src/.../Parser/SqlQueryParserContext.cs` | Ajustar `IsWord()`, `ExpectIdentifier()`, etc. |
| `src/.../Parser/Shared/SqlStatementSplitter.cs` | Usar spans |
| `src/.../Parser/SqlSyntaxDetector.cs` | Ajustar consumo |
| `src/.../Parser/Select/`, `Dml/`, `Ddl/`, `Expression/` | Ajustar consumo de token text (~10+ arquivos) |

---

## 6. Otimização do Transaction Journal — Snapshot Diferencial

### Problema

Cada `UPDATE` durante uma transação gera **duas cópias completas** da linha: `OldRowSnapshot` e `NewRowSnapshot` no `TransactionJournalEntry` (`DbConnectionMockBase.cs:52-67`, linhas 2627-2630). Cada cópia clona o `object?[]` inteiro via `TableMock.CloneRow()`.

Para uma tabela com 50 colunas de strings, cada UPDATE triplica temporariamente a memória da linha (original + old snapshot + new snapshot).

**Localizações:**
- `DbConnectionMockBase.cs:52-67` — `TransactionJournalEntry` record com `Row`, `OldRowSnapshot`, `NewRowSnapshot`
- `DbConnectionMockBase.cs:2627-2630` — `NewRowSnapshot: TableMock.CloneRow(new ArrayRow(mutation.Row))`
- `DbConnectionMockBase.cs:2604-2631` — `OnTableMutationApplied` onde os snapshots são capturados
- `DbConnectionTransactionJournalManager.cs:176-193` — `ReplayUpdateJournalEntry` restaura snapshots completos

### Abordagem

1. **Snapshot diferencial**: em vez de clonar o array inteiro, armazenar apenas as colunas modificadas como `List<(int, object?)>` (pares coluna-indice + old-value)
2. **Rollback adaptativo**: em vez de restaurar todo o array com `Array.Clear` + iteração (`TableStateManager.cs:20-27`), iterar apenas as colunas modificadas
3. **Manter snapshot completo** apenas para Delete (precisa da linha inteira para re-inserir)
4. **NewRowSnapshot**: não precisa ser armazenado — é o estado atual da linha, que está em `_items[rowIdx]`. Só precisa do old value diferencial

### Impacto esperado

- **Memória por UPDATE**: de `2 * rowWidth` para `modifiedColumns * (sizeof(int) + sizeof(ptr))`
- **Rollback mais rápido**: menos dados para iterar e copiar
- **Relevante**: transações longas com muitos updates parciais (e.g., UPDATE T SET Status = @s WHERE ...)

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Base/DbConnectionMockBase.cs` | `TransactionJournalEntry` muda `OldRowSnapshot` para `List<(int, object?)>?` |
| `src/.../Base/DbConnectionMockBase.cs` | `OnTableMutationApplied` captura apenas colunas modificadas |
| `src/.../Base/DbConnectionTransactionJournalManager.cs` | `ReplayUpdateJournalEntry` restaura por coluna |
| `src/.../Models/TableMock.cs` | Helper `CaptureChangedColumns(oldRow, newRow)` |

---

## 7. ReaderWriterLockSlim para Leituras Concorrentes

### Problema

O bloqueio usa exclusivamente `lock (SyncRoot)` (`DbMock.cs:64-86`), que é um `Monitor` exclusivo. Mesmo queries de leitura (`SELECT`) adquirem o lock de escrita. Em cenários multi-thread com várias conexões lendo do mesmo `DbMock`, isso serializa todas as operações.

Não há `ReaderWriterLockSlim` em nenhum lugar do código.

**Localizações:**
- `DbMock.cs:64-86` — `ExecuteWithLock<T>`, `ExecuteWithLock` (sempre exclusivo)
- `DbConnectionMockBase.cs:1443,1990,2290,2341,2424,2450,2481,2498,2515` — 9 pontos de lock
- `SchemaMock.cs:320` — lock em AddUnsafe
- `DbMock.cs:55` — `SyncRoot` field

### Abordagem

1. **Substituir `Monitor` por `ReaderWriterLockSlim`** no `DbMock`
2. **Operações de leitura** (SELECT, GetSchema, etc.) usam `EnterReadLock`/`ExitReadLock`
3. **Operações de escrita** (INSERT, UPDATE, DELETE, DDL) usam `EnterWriteLock`/`ExitWriteLock`
4. **Manter `ThreadSafe` flag**: quando `false`, bypass total; quando `true`, usar RWLS
5. **Cuidado**: métodos que leem e depois escrevem (e.g., INSERT com validação de FK) precisam de upgrade lock ou write lock direto
6. **Benchmark** para garantir que o overhead do RWLS compensa o ganho de concorrência

### Impacto esperado

- **Leitura concorrente**: N threads lendo simultaneamente sem bloqueio
- **Escrita bloqueia leitura**: mas leitura não bloqueia outra leitura
- **Relevante**: testes de integração multi-thread que compartilham `DbMock`
- **Trade-off**: RWLS tem overhead ~2-3x maior que `lock()` para aquisição. Em single-thread, pode ser mais lento

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Models/DbMock.cs` | `SyncRoot` → `ReaderWriterLockSlim` |
| `src/.../Models/DbMock.cs` | `ExecuteWithLock<T>` → `ExecuteWithReadLock`, `ExecuteWithWriteLock` |
| `src/.../Base/DbConnectionMockBase.cs` | Classificar cada operação como read ou write |
| `src/.../Models/SchemaMock.cs` | Lock de escrita |

---

## 8. Cache Key Case-Insensitive — Aproveitar Cache com Variações de Caixa

### Problema

`SqlQueryAstCache.BuildKey()` (`SqlQueryAstCache.cs:32-51`) normaliza o SQL apenas com `NormalizeSql()` (colapsa whitespace, não altera caixa). O `ConcurrentDictionary` usa `StringComparer.Ordinal` (case-sensitive). Portanto:
- `SELECT * FROM T` e `select * from t` têm **cache keys diferentes**
- `SELECT * FROM T` e `SELECT *  FROM  T` têm keys diferentes (whitespace)

Isso reduz a efetividade do cache em cenários onde o SQL varia em caixa/espaçamento.

**Localizações:**
- `SqlQueryAstCache.cs:32-51` — `BuildKey()` com `NormalizeSql(sql)`
- `SqlQueryAstCache.cs:114-141` — `NormalizeSql()` apenas colapsa whitespace
- `SqlQueryAstCache.cs:12` — `ConcurrentDictionary` com `StringComparer.Ordinal`
- `SqlQueryParsePreludeCache.cs:34-53` — mesmo padrão

### Abordagem

1. **Adicionar normalização de caixa em `NormalizeSql()`**: fazer `ToUpperInvariant()` (ou `ToLowerInvariant()`) depois de colapsar whitespace
2. **Trade-off**: `ToUpperInvariant()` aloca uma string extra. Compensar porque:
   - Cache hit evita parser inteiro (tokenização + AST construction)
   - A string normalizada é armazenada como parte da key e reutilizada
3. **Alternativa**: manter case-sensitive mas adicionar normalização opcional controlada por env var
4. **Cuidado**: strings literais em SQL (e.g., `WHERE Name = 'Value'`) não devem ser normalizadas — apenas a estrutura SQL. Mas `NormalizeSql` opera sobre o SQL inteiro. Solução: normalizar antes de construir a key, não o SQL original

### Impacto esperado

- **Cache hit rate**: aumenta significativamente em cenários onde o SQL é semanticamente idêntico mas difere em caixa
- **Parser evitado**: para cada cache hit adicional, economiza tokenização + AST build
- **Risco**: colisão de key entre SQLs que diferem apenas em caixa de string literal (raro em mocks)

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Parser/SqlQueryAstCache.cs` | `NormalizeSql()` adiciona normalização de caixa |
| `src/.../Parser/SqlQueryParsePreludeCache.cs` | Reusa mesma normalização |

---

## 9. Cache Local por Conexão — Evitar Contenção em Cache Estático

### Problema

`SqlQueryAstCache` e `SqlQueryParsePreludeCache` são `ConcurrentDictionary` estáticos (process-wide). Em cenários multi-tenant (N projetos de teste em paralelo):
- O cache cresce sem limite (configurável, mas padrão 256)
- Chaves de diferentes `DbMock` identidades colidem no mesmo dicionário
- Contenção de lock no `ConcurrentDictionary` interno

**Localizações:**
- `SqlQueryAstCache.cs:12` — `ConcurrentDictionary<string, SqlQueryBase>` estático
- `SqlQueryParsePreludeCache.cs:12` — `ConcurrentDictionary<string, Prelude>` estático
- `SqlQueryParser.cs:18-19` — referências estáticas para os caches

### Abordagem

1. **Adicionar cache local (connection-scoped)**: cada `DbConnectionMockBase` pode ter um cache LRU local (opcional)
2. **Política de 2 níveis**: checkar cache local primeiro (mais rápido, sem contenção), depois global
3. **Eviction LRU com TTL**: em vez de apenas count-based, adicionar time-based expiration
4. **Configurável por conexão**: `Connection.CacheOptions { AstCacheSize, PreludeCacheSize, Ttl }`
5. **Cache local é limpo no `Close()`**: evita vazamento de memória entre testes

### Impacto esperado

- **Contenção reduzida**: cada conexão tem seu cache local, reduzindo acesso ao dicionário global
- **Isolamento**: schemas diferentes não poluem o cache uma da outra
- **Memória**: cada conexão mantém seu próprio cache (configurável)

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Parser/SqlQueryAstCache.cs` | Adicionar suporte a cache local + global |
| `src/.../Parser/SqlQueryParsePreludeCache.cs` | Idem |
| `src/.../Base/DbConnectionMockBase.cs` | Propriedade `QueryCache` |
| `src/.../Parser/SqlQueryParser.cs` | Aceitar cache opcional por conexão |

---

## 10. `ISqlDialect` Feature Gates — Constantes em Vez de Propriedades Virtuais

### Problema

`SqliteDialect` (e outras) expõe versões mínimas de features como propriedades virtuais:
```csharp
public virtual int WithCteMinVersion => 300;
public virtual int OnUpsertMinVersion => 324;
// etc.
```
Cada chamada para essas propriedades durante o parse é uma chamada virtual (vtable lookup). Embora o custo individual seja baixo, essas propriedades são verificadas múltiplas vezes no pipeline de parse para features diferentes.

**Localizações:**
- Provável em `SqliteDialect.cs` — propriedades `*MinVersion`
- `SqlDialectBase.cs` — propriedades virtuais base

### Abordagem

1. **Converter para `readonly int` fields** inicializados no construtor
2. **Ou usar `const`** onde o valor é fixo para o dialeto
3. **Feature flags bitmap**: `[Flags] enum SqliteFeatures { WithCte = 1, OnUpsert = 2, Returning = 4, ... }` e um único field `Features` — testado com `Features.HasFlag()`. Reduz branch misprediction e chamadas virtuais

### Impacto esperado

- **Mínimo**: shaves de nanossegundos por verificação
- **Benefício marginal**: mas sem custo de implementação significativo

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/.../Dialect/SqliteDialect.cs` | Features → `[Flags] enum` + field readonly |
| `src/.../Interfaces/ISqlDialect.cs` | Opcional: adicionar `Features` property |
| `src/.../Dialect/SqlDialectBase.cs` | Refatorar base |

---

## 11. Remover Cópias `ReadOnly` em `IndexDef` API Pública

### Problema

Conforme documentado em `performance-review-work-branch.md`, a API pública de `IndexDef` faz `ToDictionary + ReadOnlyDictionary` em cada acesso a `Lookup`, `TryGetValue`, indexer e enumeração (IndexDef.cs). Isso gera alocações desnecessárias em cenários de lookup intenso.

**Localizações:**
- `IndexDef.cs` — `Lookup`, `TryGetValue`, indexer, `GetEnumerator`
- `performance-review-work-branch.md:35-38`

### Abordagem

(Já documentada no backlog existente, incluída aqui por completude.)

1. Criar `IReadOnlyDictionary` wrapper sem cópia que exponha o `Dictionary` interno diretamente
2. Ou expor método interno `GetLookupUnsafe()` para uso pelo executor (onde a imutabilidade não é crítica)

### Impacto esperado

- **Alocação por lookup**: eliminada a cópia do dicionário
- **Já no backlog**: prioridade média

### Arquivos afetados

- `src/.../Models/IndexDef.cs`

---

## 12. Microbenchmarks para Validar Ganhos

### Problema

Não há um conjunto abrangente de microbenchmarks para medir o impacto das otimizações. O diretório `src/benchmark/DbSqlLikeMem.Benchmarks.Test/` tem apenas guards iniciais (SqliteHotPathGuardTests.cs).

### Abordagem

Criar benchmarks com BenchmarkDotNet para cada otimização:

1. **DataReader streaming**: comparar alocação e tempo entre materializado vs lazy
2. **Expression JIT**: SELECT com 10 projeções vs compilado
3. **Short-circuit PK lookup**: `SELECT * FROM T WHERE PK = @p` em tabela com 10k linhas
4. **Token string alloc**: parse de SQL de 1KB com vs sem `ReadOnlyMemory<char>`
5. **ArrayPool**: `ProjectRows` com 100k linhas vs rented arrays
6. **Transaction journal**: UPDATE 1000 linhas em transação vs snapshot diferencial
7. **RWLS**: 10 threads lendo concorrentemente vs lock exclusivo

### Impacto esperado

- **Tomada de decisão baseada em dados**: cada otimização tem medição antes/depois
- **Prevenção de regressão**: benchmarks são executados em CI para detectar degradação
- **Priorização correta**: o que parece relevante pode não ser (e vice-versa)

### Arquivos afetados

| Arquivo | Mudança |
|---|---|
| `src/benchmark/DbSqlLikeMem.Benchmarks.Test/` | Novos arquivos de benchmark (12+) |

---

## Priorização

| Prioridade | Item | Impacto | Esforço | Dependências |
|---|---|---|---|---|
| **P0** | 1. Streaming DataReader | Alto | Alto | Nenhuma |
| **P0** | 11. IndexDef ReadOnly copy removal | Médio | Baixo | Nenhum (já no backlog) |
| **P1** | 2. Expression JIT expansion | Alto | Alto | Item 1 (pode ser independente) |
| **P1** | 3. Short-circuit para queries simples | Alto | Médio | Item 1 (pode ser independente) |
| **P1** | 4. ArrayPool generalizado | Médio | Médio | Nenhum |
| **P2** | 5. String allocations no tokenizer | Médio | Alto | Testes extensivos de regressão |
| **P2** | 6. Snapshot diferencial no journal | Baixo-Médio | Médio | Nenhum |
| **P2** | 8. Cache key case-insensitive | Baixo | Baixo | Nenhum |
| **P3** | 7. ReaderWriterLockSlim | Baixo | Médio | Nenhum |
| **P3** | 9. Cache local por conexão | Baixo | Médio | Nenhum |
| **P3** | 10. Feature gates constantes | Baixo | Baixo | Nenhum |
| **P4** | 12. Microbenchmarks | — | Médio | Todos os itens acima |

---

## Riscos e Considerações

1. **Item 1 (Streaming DataReader)** tem o maior impacto mas também o maior risco de regressão. O comportamento do `DbDataReader` deve ser idêntico — a diferença é apenas *quando* os dados são avaliados. Testes de fidelidade existentes devem continuar passando sem alteração.

2. **Item 5 (Token string alloc)** muda o tipo de `SqlToken.Text`. Isso afeta ~50+ arquivos que consomem token text. Um teste de regressão completo é obrigatório.

3. **Item 2 (Expression JIT)** pode aumentar a memória do executor (cada expressão compilada gera um delegate + closure). Cache com limite é essencial.

4. **Item 7 (RWLS)**: em single-thread (caso mais comum em testes), RWLS é 2-3x mais lento que `lock()`. Deve ser configurável ou ativado apenas quando `ThreadSafe=true` e detecta contenção.

5. **Nenhuma otimização deve alterar a semântica visível ao usuário**. Resultados de queries, exceções, e comportamento transacional devem permanecer idênticos. Microbenchmarks + testes de fidelidade são obrigatórios antes do merge de cada item.
