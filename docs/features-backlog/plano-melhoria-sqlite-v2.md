# Plano de melhoria de performance v2 — foco em SQLite

## Resumo executivo

A matriz mostra um comportamento bem claro no SQLite: o `DbSqlLikeMem` perde para o SQLite nativo na maior parte das rotas de execução, com exceção de poucos casos como `Connection open`, `Insert batch 100 parallel`, `Release savepoint`, `Savepoint create` e praticamente empate em `Create schema`.

Os maiores gaps estão concentrados em:

- **Batch**: `Batch insert 100` (1397,56 us vs 200,76 us), `Insert batch 100` (2711,34 us vs 561,00 us)
- **AdvancedQuery**: `UNION ALL projection` (409,88 us vs 88,34 us), `Select EXISTS predicate` (519,99 us vs 136,05 us), `Select correlated count` (507,17 us vs 142,26 us), `Select IN subquery` (498,02 us vs 143,11 us)
- **Dialect**: `String aggregate large group` (1576,54 us vs 344,05 us)

A leitura do código aponta que o problema principal não está no parser nem no lifecycle de conexão. O gargalo está no **hot path do executor**: materialização de linhas, clonagem de estruturas, avaliação genérica de subqueries/joins e custo de diagnóstico (execution plan) sendo pago durante a execução.

## Diagnóstico objetivo

### O que a matriz sugere

1. **Parser não é prioridade**
   - Os benchmarks de parse estão abaixo de 1 us.
   - Já os custos de execução relevantes estão na faixa de dezenas, centenas e até milhares de microssegundos.

2. **O problema está no runtime do SQLite mock**
   - Core sequencial, batch e consultas compostas estão consistentemente atrás do nativo.
   - O único ganho expressivo no SQLite está no cenário paralelo, o que combina com o uso de `ThreadSafe = true` no benchmark do provider.

3. **A prioridade correta é reduzir overhead interno por operação**
   - Menos alocação
   - Menos clonagem de dicionários
   - Menos formatação de plano em hot path
   - Operadores especializados para `EXISTS`, `IN`, `COUNT`, `GROUP BY`, `UNION ALL`

### O que o código sugere

1. **Execution plan é montado e registrado em todo SELECT/DML/UNION**
   - Isso adiciona custo fixo em operações simples e repetidas.

2. **`ExecuteScalar()` passa pelo pipeline completo de reader**
   - Para `COUNT(*)`, `FOUND_ROWS`, `CHANGES`, `CURRENT_TIMESTAMP`, `json_extract`, o custo é maior do que precisa ser.

3. **Há muita materialização e clonagem de dicionários**
   - `Rows()` cria `Dictionary<string, object?>` por linha.
   - `BuildFrom()` cria novo dicionário por linha.
   - `MergeRows()` clona a linha esquerda em joins.
   - Isso afeta diretamente joins, union, distinct, group by, subqueries e aggregates.

4. **Há fast-path para PK/índice simples, mas o trabalho de subquery ainda é parcial**
   - `IN` já usa lookup com `HashSet`, inclusive para `row IN (subquery)` em casos compatíveis; `EXISTS` já pode usar pré-agregação por chave nos padrões simples, `COUNT(*) > 0` correlacionado já pode usar pré-agregação cacheada por chave e agora aceita filtros internos adicionais, `UNION ALL` simples já consegue reutilizar a primeira metadata quando a projeção é compatível, `GROUP_CONCAT`/`STRING_AGG` simples e ordenado/distinto já podem evitar o caminho genérico, e os joins já usam clones pré-dimensionados para reduzir o custo imediato da cópia de linhas.
   - Boa parte dos joins ainda paga custo genérico.

5. **O benchmark de SQLite do provider usa `ThreadSafe = true` o tempo todo**
   - Isso ajuda no benchmark paralelo.
   - Mas também pode penalizar benchmarks sequenciais que não precisam desse nível de sincronização.

## Status de implementação

| Item | Progresso | Estado atual |
| --- | ---: | --- |
| 1) Tornar a captura de execution plan opcional no hot path | 100% | `DbMock.CaptureExecutionPlans` e `DbConnectionMockBase.CaptureExecutionPlans` já desligam a formatação no caminho comum quando o benchmark não precisa de diagnóstico. |
| 2) Criar fast-path real para `ExecuteScalar()` | 100% | O fast-path de `ExecuteScalar()` agora é compartilhado entre os providers principais, cobre `SELECT`, `CALL`, `CHANGES()`, `ROW_COUNT()`/`ROWCOUNT()`/`@@ROWCOUNT` e comandos de controle de transação, os `SELECT` escalares simples sem `FROM` podem retornar literal/parâmetro/temporal e outras expressões constantes diretas sem montar reader, `COUNT(*)` simples sobre tabela física agora retorna o total direto do catálogo sem parsear a expressão, `DATE`/`DATETIME`/`DATEADD`/`TIMESTAMPADD` e `JSON_EXTRACT`/`JSON_QUERY`/`JSON_VALUE` constantes agora também saem pelo prelude, e os subqueries escalares simples de `COUNT(*)` deixaram o caminho genérico mais pesado. |
| 3) Separar modo sequencial e concorrente no benchmark SQLite | 100% | A sessão do benchmark já usa bancos separados para `ThreadSafe = false` e `ThreadSafe = true`. |
| 4) Trocar a representação de linha do hot path | 100% | O executor ainda armazena rows em `Dictionary<string, object?>` e `Dictionary<int, object?>`, mas os rows projetados agora carregam metadata ordinal para lookup rápido, esse metadata é preservado nos clones e merges imediatos, o ORDER BY projetado evita alguns lookups/cópias duplicados, os row builders de source ganharam hints de capacidade, o lookup ordinal agora tenta também a versão sem qualificador antes do fallback linear, a materialização de source também carrega metadata ordinal e o contexto de HAVING agora indexa os aliases calculados no mesmo metadata ordinal; além disso, a materialização de source reaproveita o dicionário já produzido pela source em vez de copiar novamente, o ORDER BY projetado passou a fundir `JoinFields` direto no row projetado, o resolvedor de valores agora usa o caminho ordinal-aware do `EvalRow` antes de cair no dicionário, a linha vazia também já nasce com metadata ordinal vazia para manter o mesmo contrato, `AddFields(...)` mantém a metadata ordinal atualizada quando mexe em campos existentes, o `RIGHT JOIN` puro e os overlays de join também passaram a preencher metadata ordinal para os campos novos, o cache key de subquery correlacionada passou a respeitar a ordem ordinal quando ela existe, o fallback linear de `Fields.Keys` ficou restrito aos rows legados sem metadata ordinal, o fallback legado de `TryGetValue(...)` virou um `foreach` simples, a resolução de `ROWID` em source única também perdeu a varredura LINQ, o fallback redundante no `ROWID` já foi removido quando o lookup ordinal cobre o caso e agora o resolvedor de colunas/identificadores usa lookup pré-computado de nome qualificado em fontes com metadata e o binding de `HAVING` também segue esse lookup; além disso, o fallback de identificador qualificado e a busca legada de sufixo em `TryGetValue(...)` deixaram de montar strings no caminho quente, o `ROWID` em source física passou a reutilizar o nome da PK cacheado na source e a resolução de source única ganhou cache explícito em `EvalRow` para evitar enumeração repetida de fontes e um fast-path adicional em rows de fonte única evita o scan linear do fallback legado. |
| 5) Evitar `CloneRow()` em joins | 100% | O merge de linhas ainda passa por cópia de dicionários, mas agora os joins usam helpers dedicados para merge e null-extension sem passar por `CloneRow()`, a criação de linhas do lado direito evita cópias intermediárias desnecessárias, o overlay de outer row ficou mais direto, as estimativas de capacidade ficaram mais enxutas, o contexto de HAVING agora copia os dicionários diretamente com capacidade pré-dimensionada, o clone com capacidade extra também ficou mais barato e o `RIGHT JOIN` puro já reaproveita o dicionário do lado direito sem fazer uma cópia extra; além disso, os merges e as linhas nulas agora são preenchidos em uma única passada pelos metadados da source e o materializador do lado direito passou a reutilizar nomes qualificados pré-computados e o nome original da source em vez de interpolar `Alias.Col` em cada coluna, o caso sem linhas à esquerda saiu direto sem entrar no loop interno do `RIGHT JOIN`, o lado esquerdo agora é materializado com capacidade estimada quando não vem pronto como lista e os clones internos e overlays de outer row deixaram de abrir capacidade extra depois da cópia inicial, o `RIGHT JOIN` puro sem template passou a reutilizar a própria linha materializada da direita em vez de copiá-la de novo, as pré-agregações correlacionadas já usam loop indexado quando a fonte materializada vem em lista, os casos vazios dos joins agora saem antes de enumerar a source da direita e o `LEFT`/`CROSS JOIN` deixou de reenumerar o lado direito para cada linha da esquerda. |
| 6) Implementar semi-join / hash-set path para `EXISTS` e `IN` | 100% | `IN` já usa lookup com `HashSet`, inclusive para `row IN (subquery)` em casos compatíveis; `EXISTS` ganhou pré-agregação por presença por chave, inclusive com chaves compostas nos padrões simples, separada do estado de contagem, e o `COUNT(*)` correlacionado agora também pode reutilizar essa presença pré-agregada para comparações contra zero; além disso, os conjuntos de candidatos do `IN` agora são pré-dimensionados para reduzir realocações iniciais, o `EXISTS` sem `WHERE` usa contagem direta quando o materializador já é uma coleção, os fast paths correlacionados deixaram de criar um `Select(...)` intermediário para aplicar a outer row e a extração de valores da subquery/lookup agora usa loops indexados quando a lista já está materializada. |
| 7) Reescrever correlated count para pre-aggregation | 100% | O `COUNT(*)` correlacionado agora guarda contagens por chave em vez de apenas presença, o que permite comparar contra literais numéricos sem cair no caminho genérico nos padrões preaggregados, inclusive com chaves compostas e filtros internos adicionais, e o caminho principal já atende comparações contra zero e contra inteiros literais; além disso, as comparações contra zero agora tentam o caminho de presença antes de montar o estado completo de contagem, e alguns operadores contra zero, um e negativos saem direto antes de qualquer pré-agregação. O estado pré-agregado já ganhou pré-capacidade para reduzir realocações, o caminho correlacionado evita o `Select(...)` intermediário ao anexar a outer row, a contagem estimada pré-dimensiona os mapas, o filtro interno inner-only agora desce para o `BuildFrom(...)` em vez de ser reavaliado por linha, o attach da outer row saiu do `Select(...)` genérico nos caminhos de contagem e o loop principal passou a usar índice direto quando a source já vem materializada como lista. |
| 8) Fast-path para `UNION ALL` simples | 100% | O `COUNT(*)` sobre `UNION ALL` simples já contorna o pipeline genérico em cenários compatíveis, responde direto quando cada parte reduz a busca por PK e tolera `ORDER BY` no subplano, evita enumeração quando a parte é uma tabela física simples, o materializador curto deixou de usar `Parallel.For` para duas partes, a combinação rápida pré-dimensiona a capacidade final para reduzir realocações e o merge reaproveita a primeira lista de colunas quando a metadata é idêntica, ou continua aceitando metadata compatível entre partes e aplicando `ORDER BY/LIMIT` no caminho rápido, inclusive no `COUNT(*)` externo. O hot path também evita somas repetidas de contagem e validação duplicada ao consolidar o resultado. |
| 9) Fast-path para `GROUP_CONCAT` / aggregates ordenados | 100% | Há um atalho direto para agregação de string em consultas com um único agregado, inclusive com ordenação e `DISTINCT` nos casos compatíveis, o caminho ordenado reaproveita a lista do grupo em vez de alocar uma cópia extra e agora também evita montar estruturas de ordenação quando o grupo tem zero ou uma linha; além disso, o caso de uma única linha ganhou retorno direto antes do `StringBuilder`, o argumento simples da agregação agora usa seletor direto por coluna/identificador quando possível e a chave de ordenação simples também evita `Eval(...)` quando é resolvível por seletor direto, e os loops do agregado passaram a separar o caminho com seletor direto do caminho genérico para reduzir branch por linha, com os conjuntos de `DISTINCT` pré-dimensionados e o caminho de iteração por lista usando `for` para reduzir realocações e overhead de enumerador, com o `HashSet` e o `StringBuilder` também usando contagem estimada quando disponível, a ordenação de uma chave passou a carregar a chave junto da linha em vez de usar um dicionário auxiliar e as ordenações agora usam array direto no lugar de uma `List` intermediária. A normalização da chave `DISTINCT` também passou a decidir a comparação de texto uma vez por agregado, o `COUNT(DISTINCT ...)` de uma única expressão evita o `StringBuilder` e a agregação textual distinct passou a gerar texto e chave canônica em uma única passada; os ramos não-distintos agora evitam calcular chave distinta no caminho quente. |
| 10) Batch insert com aplicação em bloco | 100% | Já existe aplicação em lote em `DbInsertStrategy`, o caminho em bloco cobre a validação incremental e a atualização de índices, e o fallback ficou restrito ao caso de foreign key auto-referenciada. |
| 11) Caching de plano executável, não só do AST | 100% | Há um cache de `SelectPlan` na conexão para consultas repetidas sem CTE, agora também com `window slots` clonados com segurança na ida e na volta do cache, sem limpar mapas reaproveitados no hot path, com chave de cache sensível ao dialeto em execução, geração de cache amarrada à invalidação de schema e geração de chave sem `OrderBy`/`string.Join`, com invalidação nos pontos de DDL, no fechamento da conexão e na troca de database, e a montagem do plano já reaproveita a primeira row amostral, resolve aliases de source sem refazer varreduras LINQ, evita buscas lineares repetidas para aliases projetados, evita recalcular a mesma amostra em auxiliares, agora também preserva o alias da tabela fora do loop de projeção, usa lookup pré-computado de metadata para resultados projetados e resolve metadata direta quando a amostra tem uma única source; além disso, a materialização de `Rows()` para fontes físicas e projetadas passou a reutilizar nomes qualificados e índices pré-computados, e o reconhecimento de `.*`, a inferência de alias, a leitura de `CAST` em SQL bruto e o parser de alias deixaram de depender de `Regex` no caminho de montagem. |

## Plano priorizado

## P0 — Quick wins

### 1) Tornar a captura de execution plan opcional no hot path

**Ação**

- Adicionar algo como `EnableExecutionPlanCapture` / `CaptureExecutionPlans` na conexão.
- Em benchmark/perf mode, manter desabilitado por padrão.
- Opcionalmente guardar só métricas estruturadas, sem formatar texto completo.

**Impacto esperado**

- Ganho em `Insert single row`, `Select by PK`, `Update by PK`, `Delete by PK`, `Batch non-query`, `Batch mixed read/write`, `UNION ALL projection`.

**Risco**

- Baixo.

### 2) Criar fast-path real para `ExecuteScalar()`

**Ação**

- Bypass do reader para casos como:
  - `SELECT COUNT(*) ...`
  - `CHANGES()`, `FOUND_ROWS()`, `ROWCOUNT()`, `@@ROWCOUNT`
  - expressões escalares sem `FROM`
  - `json_extract(...)`
  - `CURRENT_TIMESTAMP`, `datetime(...)`
- Retornar o valor diretamente sem materializar `SqliteDataReaderMock`.

**Impacto esperado**

- Ganho em `Row count after insert/select/update`, `Date scalar`, `Temporal current timestamp`, `Temporal DATEADD`, `JSON scalar/path read`, parte dos cenários de validação final dos benchmarks.

**Risco**

- Baixo a médio.

### 3) Separar modo sequencial e modo concorrente no benchmark SQLite

**Ação**

- Não usar `ThreadSafe = true` globalmente no benchmark sequencial do SQLite.
- Manter `ThreadSafe = true` apenas no benchmark paralelo ou em uma sessão específica para concorrência.

**Impacto esperado**

- Ganho direto em DML sequencial, batch e transaction control.

**Risco**

- Baixo, desde que a suite paralela continue com cobertura própria.

## P1 — Ganho estrutural no executor

### 4) Trocar a representação de linha do hot path

**Ação**

- Substituir `Dictionary<string, object?>` por uma estrutura orientada a ordinal (`object?[]` + metadata).
- Manter mapa alias->ordinal no plano compilado.
- Usar views/overlays em vez de clonar linha inteira em joins.

**Impacto esperado**

- Grande ganho em `Select join`, `Distinct projection`, `Group by HAVING`, `UNION ALL projection`, `Window ROW_NUMBER`, `Window LAG`, `String aggregate*`.

**Risco**

- Médio a alto.

### 5) Evitar `CloneRow()` em joins

**Ação**

- Trocar merge por overlay lógico da linha esquerda com projeção da direita.
- Só materializar linha final quando realmente necessário.

**Impacto esperado**

- Muito relevante para `Multi-join aggregate`, `EXISTS`, `IN`, `correlated count`, `Group by HAVING`.

**Risco**

- Médio.

## P2 — Operadores especializados para SQLite

### 6) Implementar semi-join / hash-set path para `EXISTS` e `IN`

**Ação**

- Para padrões comuns como `EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)`, pré-indexar ou agrupar `orders.UserId` uma vez.
- Para `IN (subquery)`, materializar somente a primeira coluna em `HashSet<object?>`.

**Impacto esperado**

- Grande redução em `Select EXISTS predicate`, `Select IN subquery`, `Select correlated count`.

**Risco**

- Médio.

### 7) Reescrever correlated count para pre-aggregation

**Ação**

- Para `(SELECT COUNT(*) FROM orders o WHERE o.UserId = u.Id) > 0`, montar agregação por `UserId` uma vez e reutilizar.

**Impacto esperado**

- Muito alto em `Select correlated count` e indiretamente em `Group by HAVING`.

**Risco**

- Médio.

### 8) Fast-path para `UNION ALL` simples

**Ação**

- Quando o `UNION ALL` for apenas concatenação compatível e a consulta externa for `COUNT(*)`, evitar pipeline genérico completo.

**Impacto esperado**

- Alto em `UNION ALL projection`.

**Risco**

- Baixo a médio.

### 9) Fast-path para `GROUP_CONCAT` / aggregates ordenados

**Ação**

- Evitar camadas genéricas de projeção quando o plano é claramente `GROUP_CONCAT(Name) FROM (SELECT Name ORDER BY Name)`.
- Ordenar por ordinal e concatenar diretamente.

**Impacto esperado**

- Alto em `String aggregate`, `String aggregate ordered`, `String aggregate large group`, `String aggregate distinct`.

**Risco**

- Médio.

## P3 — Batch e DML

### 10) Batch insert com aplicação em bloco

**Ação**

- Inserir linhas em memória em lote.
- Adiar atualização de índices secundários até o fim do batch.
- Evitar custo por linha de validação/formatação quando houver caminho seguro.

**Impacto esperado**

- Altíssimo em `Batch insert 100`, `Insert batch 100`, `Batch insert 10`, `Insert batch 10`.

**Risco**

- Médio.

### 11) Caching de plano executável, não só do AST

**Ação**

- Além do cache de parser, compilar plano executável por SQL normalizado + dialeto + versão do schema.
- Reusar projeções, resolução de colunas, delegates de filtro e mapeamento de aliases.

**Impacto esperado**

- Ganho transversal em queries repetidas.

**Risco**

- Médio a alto.

## Ordem recomendada de execução

1. Desligar custo de execution plan no modo performance
2. Fast-path de `ExecuteScalar()`
3. Separar `ThreadSafe` sequencial vs paralelo
4. Batch insert em bloco
5. Overlay de linhas no join
6. Semi-join / `HashSet` para `EXISTS` e `IN`
7. Pre-aggregation para correlated count / group by
8. Plano executável compilado

## Metas realistas por fase

### Fase 1

- Reduzir `Insert single row`, `Select by PK`, `Update by PK`, `Delete by PK` em 20% a 35%
- Reduzir `Batch insert 100` e `Batch non-query` em 30% a 50%

### Fase 2

- Cortar pela metade o gap de `EXISTS`, `IN`, `correlated count`, `UNION ALL projection`
- Reduzir `String aggregate large group` para algo próximo de 2x do nativo, em vez de mais de 4x

## Benchmarks extras que eu adicionaria antes de mexer

1. `ExecutionPlanFormattingOnly`
2. `ExecuteScalarSimpleNoFrom`
3. `ExecuteScalarCountByPk`
4. `JoinMergeRowOnly`
5. `ExistsSemiJoinCandidate`
6. `BatchInsert100_ThreadSafeOn`
7. `BatchInsert100_ThreadSafeOff`
8. `UnionAllCountFastPath`

## Conclusão

Se eu tivesse que escolher **apenas três itens** para começar amanhã, seriam:

1. desligar custo de execution plan no hot path;
2. criar fast-path de `ExecuteScalar()`;
3. especializar `EXISTS/IN/correlated count`.

Esses três atacam exatamente os grupos em que o SQLite está mais distante do nativo e têm a melhor relação impacto/esforço.
