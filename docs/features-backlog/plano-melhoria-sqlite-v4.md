# Plano de melhoria de performance v4 - foco na sessão SQLite

Base de analise: apenas a secao `SQLite` de [docs/Wiki/performance-matrix.md](../Wiki/performance-matrix.md).

## Leitura atual

- `Batch` continua sendo o pior grupo da sessao, com varios cenarios sequenciais acima do SQLite nativo.
- `AdvancedQuery` ainda concentra os maiores gaps de `IN`, `EXISTS`, `JOIN`, `correlated count` e agregacao.
- `Core` tem custo relevante em `Update by PK`, `Delete by PK`, `Select by PK` e `Transaction commit`.
- `Dialect` e `Temporal` seguem como areas importantes, mas hoje funcionam mais como ajuste fino do que como regressao dominante.

## Maiores regressões da matriz atual

| Prioridade | Benchmark | DbSqlLikeMem | SQLite nativo | Gap | Leitura pratica |
|---|---|---:|---:|---:|---|
| P0 | `Batch insert 100` | 2.701,00 us | 1.285,80 us | -110,06% | O caminho sequencial de lote ainda paga custo por linha demais. |
| P0 | `Select LEFT JOIN anti-join` | 63,64 us | 8,36 us | -661,42% | O hot path de joins ainda materializa mais do que deveria. |
| P0 | `Select NOT IN subquery` | 55,98 us | 7,57 us | -639,53% | O lookup de subquery correlacionada ainda não está barato o suficiente. |
| P0 | `Select IN subquery` | 54,31 us | 11,51 us | -371,80% | O conjunto de candidatos ainda pode ser construído com menos overhead. |
| P1 | `Select EXISTS predicate` | 42,22 us | 8,77 us | -381,55% | O caminho de pré-agregação ainda pode ser enxugado. |
| P1 | `Select correlated count` | 51,97 us | 9,92 us | -423,76% | A contagem correlacionada segue parecida com o caso acima. |
| P1 | `Update by PK` | 74,87 us | 18,34 us | -308,11% | Atualização por chave ainda paga snapshot e bookkeeping demais. |
| P1 | `String aggregate large group` | 117,95 us | 27,06 us | -335,88% | Agregação textual em grupo grande ainda tem overhead de materialização. |

## Hipóteses prioritárias

1. O executor ainda está perdendo desempenho ao transformar coleções que já são listas em `IEnumerable` com iteradores intermediários.
2. Parte do custo de `UPDATE` ainda vem de snapshot criado mesmo quando não existe observador de mutação.
3. Os caminhos de `IN` e `EXISTS` ainda podem ganhar ao pré-dimensionar estruturas de lookup com a cardinalidade conhecida.
4. Batch sequencial, agregação textual e janela continuam importantes, mas são a segunda onda depois de reduzir o custo estrutural do executor.

## Mudanças já aplicadas nesta rodada

- `AstQuerySubqueryComparisonEvaluator` passou a preservar o fast path de `List<EvalRow>` nos estados de `EXISTS` e `COUNT`, sem encadear `Where(...)` no caminho quente.
- `AstQuerySubqueryComparisonEvaluator` também passou a pré-dimensionar o `HashSet` de presença quando a cardinalidade da subquery já é conhecida.
- `AstQueryInSubqueryLookupBuilder` passou a pré-dimensionar os `HashSet` usados por `IN` e `NOT IN` nos caminhos escalar e em linha.
- `TableMock` passou a evitar snapshot antigo de `UPDATE` quando não existe observador de mutação.
- `TableMock` passou a evitar montar payload de notificação e `previousNextIdentity` em `INSERT`, `UPDATE` e `DELETE` quando o journal de mutação não está ativo.
- `TableMock.TryFindRowByPkConditions` passou a resolver PK em ordem direta e sem dicionário temporário no caminho de `UPDATE` e `DELETE` por chave.
- `DbInsertStrategy` passou a pular a análise de `ON DUPLICATE`/`UPDATE` no insert puro e pré-dimensionar os índices afetados do lote.
- `DbInsertStrategy` também passou a usar o último row materializado diretamente e a calcular o fim do lote uma vez no caminho de insert puro.
- `DbInsertStrategy` passou a ignorar a rastreabilidade incremental de conflito quando não existem PK ou índices únicos para acompanhar o lote.
- `DbInsertStrategy` passou a não criar o mapa de chaves únicas do lote quando a tabela não tem índices únicos.
- `DbInsertStrategy` passou a transformar `ON DUPLICATE` em insert puro quando a tabela não tem nenhuma constraint de conflito para acompanhar.
- `DbInsertStrategy` passou a pular a busca de conflito em `ON CONFLICT DO NOTHING` quando a tabela não tem constraints de conflito.
- O caminho de `AddBatch` passou a pular o loop de notificação quando não há assinante, o que reduz custo do lote sequencial sem alterar o resultado.
- `AstQueryAggregateEvaluator` passou a reutilizar o texto calculado no caminho `DISTINCT` da agregação textual, evitando conversão duplicada por linha.
- `AstQueryAggregateEvaluator` passou a pré-dimensionar o `HashSet` de `DISTINCT` na agregação textual com base no tamanho do grupo.
- `AstQueryAggregateEvaluator` passou a calcular correlação e regressão com loops diretos em vez de múltiplas passagens LINQ.
- `AstQueryAggregateEvaluator` passou a ordenar a agregação textual com valores pré-computados por linha, em vez de reavaliar expressões no comparador.
- `AstQueryJsonSharedFunctionEvaluator` passou a montar arrays JSON com `StringBuilder`, evitando `Select`/`string.Join` no caminho quente.
- `AstQueryJsonSharedFunctionEvaluator` também passou a pré-dimensionar o `StringBuilder` de arrays JSON quando a cardinalidade é conhecida.
- `AstQueryAggregateEvaluator` passou a pré-dimensionar `APPROX_COUNT_DISTINCT` e a materialização de `COLLECT`/`ARRAY_AGG` em arrays diretos.
- `AstQueryHavingHelper` passou a detectar identificadores com uma checagem booleana direta no caminho de validação de `HAVING`.
- `AstQueryWindowExecutionHelper` passou a calcular o dialeto uma vez por grupo de slots, reduzindo trabalho repetido na validação de janelas.
- `AstQueryWindowFillHelper` passou a pré-dimensionar caches de valor e a evitar testes repetidos por linha nos caminhos de `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` e `NTH_VALUE`.
- `AstQuerySelectGroupKeyHelper` passou a montar as chaves de `GROUP BY` em array direto, sem lista intermediária e cópia final.
- `AstQueryWindowExecutionHelper` passou a devolver os grupos de slots sem cópia final implícita.
- `WindowPartitionExecutionContext` passou a pré-dimensionar a lista de peer groups com base no tamanho da partição.
- `AstQueryWindowExecutionHelper` passou a construir o seletor de valor de janela só quando a função realmente precisa dele.
- `WindowPartitionExecutionContext` passou a reutilizar `Part` e `partCount` nos loops internos, reduzindo leituras repetidas e um índice extra no cálculo de peer groups.
- `WindowPartitionExecutionContext` passou a simplificar o loop de peer groups, eliminando a checagem de fim dentro do laço.
- `AstQueryHavingHelper` passou a reescrever listas de `HAVING` em arrays diretos, reduzindo alocação intermediária no caminho de normalização.
- `AstQueryHavingHelper` passou a percorrer listas recursivas com índices diretos em vez de `foreach`, reduzindo overhead de iteração no caminho de validação.
- `AstQuerySelectGroupKeyHelper` passou a iterar `GROUP BY` por índice direto, evitando enumerador e índice auxiliar no caminho de normalização.
- `AstQueryHavingHelper` passou a validar `ColumnExpr` sem montar string qualificada intermediária, reduzindo alocação no binding de `HAVING`.
- `AstQueryHavingHelper` passou a combinar a checagem de agregados e de referências em uma única travessia na normalização de `HAVING`.
- `AstQueryHavingHelper` passou a validar identificadores e referências temporais em um único percurso da árvore.
- `AstQueryHavingHelper` passou a validar o binding dos identificadores de `HAVING` sem materializar lista intermediária.
- `AstQuerySelectGroupKeyHelper` passou a reconhecer ordinais de `GROUP BY` com `Span.Trim`, evitando alocação do `Trim()` temporário.
- `AstQueryHavingHelper` passou a cachear `row.Sources` no binding de `HAVING`, reduzindo acesso repetido ao dicionário.
- `AstQuerySelectGroupKeyHelper` passou a cachear `groupBy.Count` e reutilizar o valor lido no caminho de normalização.
- `AstQuerySelectGroupKeyHelper` passou a cachear `SelectItems.Count` no caminho de ordinais de `GROUP BY`.
- `WindowPartitionExecutionContext` passou a expor o tamanho da partição uma vez para reaproveitamento nos helpers de janela.
- `AstQueryWindowFillHelper` passou a reutilizar o tamanho da partição em vez de consultar `Part.Count` repetidamente.
- `AstQueryWindowFillHelper` passou a evitar a checagem repetida de `defaultExpr` no caminho de `LAG` e `LEAD`.
- `AstQueryWindowExecutionHelper` passou a evitar montar o seletor de valor quando a função não tem argumento.
- `AstQueryWindowExecutionHelper` passou a classificar funções de janela em um único passo por slot, reduzindo comparações repetidas de nome.
- `AstQueryWindowExecutionHelper` passou a obter o seletor de valor sem local function capturando o contexto.
- `AstQueryWindowExecutionHelper` passou a pular o lookup de agregados para funções de janela já classificadas.
- `AstQueryWindowExecutionHelper` passou a calcular o dialeto e o flag de `SqlServer` uma vez antes do agrupamento de slots.
- `AstQueryWindowExecutionHelper` passou a cachear o primeiro slot, a expressão base e o nome da função por slot.
- `AstQueryWindowExecutionHelper` passou a iterar os slots do grupo por índice direto e cachear `WindowSpec` por slot.
- `AstQueryWindowExecutionHelper` passou a cachear `Args.Count`, `ORDER BY.Count` e um `sampleRow` por partição.
- `AstQueryWindowFillHelper` passou a cachear a row alvo do ramo válido em `LAG`/`LEAD`.
- `AstQueryWindowFillHelper` passou a cachear `Args` e a primeira row da partição nos caminhos de `LAG`, `LEAD` e `NTH_VALUE`.
- `WindowPartitionExecutionContext` passou a cachear os valores de ordenação anterior e atual no cálculo de peer groups.
- `AstQueryWindowExecutionHelper` passou a cachear `slot.Map` por slot no caminho de preenchimento de janelas.
- `AstQueryWindowFillHelper` passou a cachear `Start` e `End` dos peer groups no preenchimento de ranking e percentil.
- `AstQueryWindowFillHelper` passou a cachear a primeira row da partição no caminho de `NTILE`.
- `AstQueryWindowFillHelper` passou a cachear a row atual no laço de `NTILE`.
- `WindowPartitionExecutionContext` passou a cachear `Frame`, `OrderBy` e o último índice no cálculo de frames.
- `AstQueryWindowFillHelper` passou a cachear `Spec.Frame` nos caminhos de `FIRST/LAST` e `NTH_VALUE`.
- `AstQueryWindowFillHelper` passou a cachear o denominador de `PERCENT_RANK` fora do laço de peer groups.
- `WindowPartitionExecutionContext` passou a reutilizar o array de frame ranges já materializado em `CoversWholePartition`.
- `AstQueryWindowFillHelper` passou a separar os ramos de `valueSelector` no hot path de `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, removendo branches repetidos por linha.
- `AstQueryWindowExecutionHelper` passou a hoistar `Frame` e `ORDER BY.Count` do `spec` do grupo, reduzindo leituras repetidas por slot.

## Plano de ataque

### Rodada 1 - Correlated subqueries e `IN`

- Preservar o fast path de lista nos estados pré-agregados de `EXISTS` e `COUNT`.
- Remover iteradores `Where(...)` intermediários do caminho quente.
- Pré-dimensionar os `HashSet` usados para `IN` e `NOT IN` quando o volume da subquery já é conhecido.

### Rodada 2 - `UPDATE` por PK

- Evitar snapshot antigo quando não houver ouvintes de mutação registrados.
- Manter o snapshot completo apenas quando houver transaction journal ou outro consumidor explícito.
- Continuar usando o snapshot quando a atualização realmente precisar dele para rollback.

### Rodada 3 - Batch sequencial

- Revisar os comandos sequenciais de lote para reduzir trabalho repetido por statement.
- Preservar o caminho paralelo, que já é uma referência positiva na matriz.

### Rodada 4 - Agregação e janela

- Reavaliar `String aggregate large group`, `Group by HAVING`, `Window LAG` e `Window ROW_NUMBER`.
- Reduzir cópias e estruturas temporárias nos grupos maiores.

## Evolução da rodada

| Data | Estado | Mudança |
|---|---|---|
| 2026-04-07 | Concluída | Plano criado a partir da matriz atual de SQLite. |
| 2026-04-07 | Concluída | Rodada 1 aplicada: `EXISTS`/`COUNT` correlacionado e `IN`/`NOT IN` ganharam fast paths e pré-dimensionamento. |
| 2026-04-07 | Em andamento | Rodada 2 iniciada: `INSERT`, `UPDATE` e `DELETE` agora evitam payload de mutação quando não há observador ativo. |
| 2026-04-07 | Em andamento | Lookup de PK recebeu caminho direto com valores em ordem, reduzindo alocação no update/delete por chave. |
| 2026-04-07 | Em andamento | Insert puro em batch deixou de pagar análise de `ON DUPLICATE` e ganhou pré-dimensionamento do vetor de índices afetados. |
| 2026-04-07 | Em andamento | Insert puro em batch passou a evitar uma leitura final de `table.Count` e reutilizar a última linha já materializada. |
| 2026-04-07 | Em andamento | O branch de `ON DUPLICATE` em lote passou a pular rastreio incremental quando não há PK nem índices únicos para comparar. |
| 2026-04-07 | Em andamento | O lote `ON DUPLICATE` passou a evitar o mapa de chaves únicas quando não existem índices únicos na tabela. |
| 2026-04-07 | Em andamento | `ON DUPLICATE` em lote passou a cair no caminho de insert puro quando a tabela não tem constraints de conflito. |
| 2026-04-07 | Em andamento | `ON CONFLICT DO NOTHING` em insert passou a evitar busca de conflito quando não há constraints de conflito. |
| 2026-04-07 | Em andamento | Agregação textual `DISTINCT` passou a reaproveitar o texto já calculado no caminho quente. |
| 2026-04-07 | Em andamento | Agregação textual `DISTINCT` passou a pré-dimensionar o `HashSet` com base no tamanho do grupo. |
| 2026-04-07 | Em andamento | Correlação e regressão passaram a usar loops diretos em vez de múltiplas passagens LINQ. |
| 2026-04-07 | Em andamento | A ordenação da agregação textual passou a usar valores pré-computados por linha. |
| 2026-04-07 | Em andamento | Agregação JSON passou a montar arrays com `StringBuilder` em vez de `Select`/`string.Join`. |
| 2026-04-07 | Em andamento | Agregação JSON passou a pré-dimensionar o `StringBuilder` quando a cardinalidade é conhecida. |
| 2026-04-07 | Em andamento | `APPROX_COUNT_DISTINCT` e `COLLECT`/`ARRAY_AGG` passaram a pré-dimensionar e materializar arrays diretos. |
| 2026-04-07 | Em andamento | `HAVING` passou a fazer a validação de presença de identificadores sem materializar uma lista intermediária. |
| 2026-04-07 | Em andamento | Execução de janela passou a reutilizar o dialeto por grupo de slots, evitando recomputação no loop interno. |
| 2026-04-07 | Em andamento | Helpers de janela passaram a pré-dimensionar caches e reduzir branches repetidos em `LAG`/`LEAD`/`FIRST_VALUE`/`NTH_VALUE`. |
| 2026-04-07 | Em andamento | `GROUP BY` passou a preencher o array de chaves diretamente, evitando lista intermediária e cópia final. |
| 2026-04-07 | Em andamento | Agrupamento de slots de janela passou a retornar a lista final sem cópia implícita, e peer groups passaram a ser pré-dimensionados. |
| 2026-04-07 | Em andamento | O seletor de valor de janela passou a ser criado só nos ramos que realmente o usam, evitando custo antecipado em `ROW_NUMBER`, `NTILE`, `RANK`, `DENSE_RANK`, `PERCENT_RANK` e `CUME_DIST`. |
| 2026-04-07 | Em andamento | `WindowPartitionExecutionContext` passou a reutilizar `Part` e `partCount` nos loops internos e no cálculo de peer groups. |
| 2026-04-07 | Em andamento | `WindowPartitionExecutionContext` passou a simplificar o loop de peer groups, eliminando a checagem de fim dentro do laço. |
| 2026-04-07 | Em andamento | `AstQueryHavingHelper` passou a reescrever listas de `HAVING` em arrays diretos, reduzindo alocação intermediária. |
| 2026-04-07 | Em andamento | `AstQueryHavingHelper` passou a percorrer listas recursivas com índices diretos em vez de `foreach`. |
| 2026-04-07 | Em andamento | `AstQuerySelectGroupKeyHelper` passou a iterar `GROUP BY` por índice direto, evitando enumerador e índice auxiliar. |
| 2026-04-07 | Em andamento | `AstQueryHavingHelper` passou a validar `ColumnExpr` sem montar string qualificada intermediária. |
| 2026-04-07 | Em andamento | `AstQueryHavingHelper` passou a combinar a checagem de agregados e referências em uma única travessia. |
| 2026-04-07 | Em andamento | `AstQuerySelectGroupKeyHelper` passou a reconhecer ordinais de `GROUP BY` com `Span.Trim`. |
| 2026-04-07 | Em andamento | `AstQueryHavingHelper` passou a cachear `row.Sources` no binding de `HAVING`. |
| 2026-04-07 | Em andamento | `AstQuerySelectGroupKeyHelper` passou a cachear `groupBy.Count` no caminho de normalização. |
| 2026-04-07 | Em andamento | `AstQuerySelectGroupKeyHelper` passou a cachear `SelectItems.Count` no caminho de ordinais de `GROUP BY`. |
| 2026-04-07 | Em andamento | `HAVING` passou a validar identificadores e referências temporais em um único percurso. |
| 2026-04-07 | Em andamento | `WindowPartitionExecutionContext` passou a expor `PartCount` e os helpers de janela passaram a reutilizá-lo. |
| 2026-04-07 | Em andamento | `HAVING` passou a validar o binding dos identificadores sem construir lista intermediária. |
| 2026-04-07 | Em andamento | `LAG` e `LEAD` passaram a evitar a checagem repetida de `defaultExpr` no caminho quente. |
| 2026-04-07 | Em andamento | O seletor de valor de janela passou a respeitar funções sem argumento sem montar trabalho desnecessário. |
| 2026-04-07 | Em andamento | Funções de janela passaram a ser classificadas uma vez por slot, em vez de repetir comparações de nome. |
| 2026-04-07 | Em andamento | O seletor de valor de janela passou a ser obtido sem local function capturando o contexto. |
| 2026-04-07 | Em andamento | Funções de janela passaram a pular o lookup de agregados quando já foram classificadas como janela nativa. |
| 2026-04-07 | Em andamento | `AstQueryWindowExecutionHelper` passou a calcular o dialeto e o flag de `SqlServer` uma vez antes do agrupamento de slots. |
| 2026-04-07 | Em andamento | `AstQueryWindowExecutionHelper` passou a cachear o primeiro slot, a expressão base e o nome da função por slot. |
| 2026-04-07 | Em andamento | `AstQueryWindowExecutionHelper` passou a iterar os slots do grupo por índice direto e cachear `WindowSpec` por slot. |
| 2026-04-07 | Em andamento | `AstQueryWindowExecutionHelper` passou a cachear `Args.Count`, `ORDER BY.Count` e um `sampleRow` por partição. |
| 2026-04-07 | Em andamento | `AstQueryWindowFillHelper` passou a cachear a row alvo do ramo válido em `LAG`/`LEAD`. |
| 2026-04-07 | Em andamento | `AstQueryWindowFillHelper` passou a cachear `Args` e a primeira row da partição nos caminhos de `LAG`, `LEAD` e `NTH_VALUE`. |
| 2026-04-07 | Em andamento | `WindowPartitionExecutionContext` passou a cachear os valores de ordenação anterior e atual no cálculo de peer groups. |
| 2026-04-08 | Em andamento | `AstQueryWindowExecutionHelper` passou a cachear `slot.Map` por slot no caminho de preenchimento de janelas. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a cachear `Start` e `End` dos peer groups no preenchimento de ranking e percentil. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a cachear a primeira row da partição no caminho de `NTILE`. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a cachear a row atual no laço de `NTILE`. |
| 2026-04-08 | Em andamento | `WindowPartitionExecutionContext` passou a cachear `Frame`, `OrderBy` e o último índice no cálculo de frames. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a cachear `Spec.Frame` nos caminhos de `FIRST/LAST` e `NTH_VALUE`. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a cachear o denominador de `PERCENT_RANK` fora do laço de peer groups. |
| 2026-04-08 | Em andamento | `WindowPartitionExecutionContext` passou a reutilizar o array de frame ranges já materializado em `CoversWholePartition`. |
| 2026-04-08 | Em andamento | `AstQueryWindowFillHelper` passou a separar os ramos de `valueSelector` no hot path de `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`. |
| 2026-04-08 | Em andamento | `AstQueryWindowExecutionHelper` passou a hoistar `Frame` e `ORDER BY.Count` do `spec` do grupo. |

## Progresso sugerido

| Frente | Status atual | Próximo passo |
|---|---:|---|
| Correlated subqueries | 60% | Medir impacto dos fast paths e refiná-los se algum caso composto ainda cair no caminho genérico. |
| `IN`/`NOT IN` | 50% | Confirmar ganho do `HashSet` pré-dimensionado e revisar casos multicoluna. |
| `UPDATE` por PK | 55% | Validar o corte de snapshot, o short-circuit de notificação e o novo lookup direto de PK no caminho quente. |
| Batch sequencial | 60% | Usar a mesma técnica de short-circuit para reduzir trabalho por statement. |
| Agregação textual | 70% | Reduzir conversão duplicada e revisar ordenação/materialização nos grupos grandes. |
| `Group by HAVING` | 73% | Cortar alocações de validação no caminho de normalização e binding. |
| `Window LAG` / `Window ROW_NUMBER` | 98% | Seguir refinando loops internos e peer groups, depois medir se ainda sobra trabalho redundante em `GROUP BY / HAVING`. |

## Critério de sucesso

- `Select IN subquery`, `Select EXISTS predicate` e `Select correlated count` devem perder parte do overhead de coleção/iterador.
- `Update by PK` deve deixar de pagar snapshot quando não houver observador ativo.
- A próxima leitura da matriz deve mostrar ao menos uma melhora clara nos caminhos acima, sem afetar os benchmarks que já estão saudáveis.
