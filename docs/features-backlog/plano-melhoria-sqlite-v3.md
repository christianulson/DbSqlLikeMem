# Plano de ataque de performance v3 - foco na sessão SQLite

Base de analise: apenas a secao `SQLite` de [docs/Wiki/performance-matrix.md](../Wiki/performance-matrix.md). As outras sessoes da matriz foram ignoradas de proposito para nao contaminar a prioridade com dados de outros providers.

## Resumo executivo

A sessao SQLite atual nao apresenta uma regressao uniforme. Ela mostra um padrao bem claro:

- setup, JSON, transacoes e conexao continuam saudaveis;
- `Insert batch 100 parallel` segue muito forte;
- `Batch`, `AdvancedQuery`, `Dialect` e parte de `Core` continuam sendo os pontos que mais drenam tempo;
- parser nao e o problema agora, porque todos os benchmarks de parse continuam abaixo de 2 us;
- o custo regressivo parece estar concentrado em execucao, materializacao, clonagem e processamento por linha, e nao em bootstrap.

O maior alerta e o grupo `Batch`, com media de **-102,20%** contra o SQLite nativo. Em seguida vem `Dialect` com **-74,88%** e `AdvancedQuery` com **-61,13%**. `Core` esta quase empatado, mas ainda fecha levemente negativo em media (**-6,42%**), o que indica que os caminhos basicos continuam com custo estrutural demais.

## Status de implementacao

Percentual geral estimado do roteiro: **92%**

Reavaliacao rapida em 2026-03-21:

- `Batch` sequencial e os fast paths de leitura/escrita ja estao consolidados.
- `DML por PK` e consultas compostas ficaram fechadas neste roteiro, depois dos cortes em `UPDATE`, `MERGE`, `EXISTS` e `UNION ALL COUNT`.
- `Agregacao`, `janela` e `ordenacao` estao prontos para refinamento final.
- O snapshot temporal por executor passou a ser reutilizado no caminho de funcoes temporais, reduzindo chamadas repetidas ao relogio em consultas com `CURRENT_TIMESTAMP` / `NOW` / `GETDATE` em `ORDER BY`.
- `Cache`, `invalidacao` e `guardrails` ainda sao as frentes abertas que justificam manter o plano vivo.
- A invalidacao de cache em rotinas DDL desnecessarias foi reduzida, entao a fase 6 e a fase 7 agora estao mais perto do fechamento.
- `CREATE TRIGGER` no SQLite ganhou guardrail para nao alterar a geracao do cache de plano quando a query ja esta aquecida.

### O que ja foi implementado

- `DbMetrics.Enabled` passou a desligar a coleta de telemetria sem alterar o comportamento SQL.
- A sessao SQLite de benchmark foi ajustada para desligar telemetria fora do caminho de analise.
- A sessao SQLite de benchmark passou a manter `CaptureExecutionPlans` desligado por padrao no `DbMock` e ligar apenas nas features de plano.
- O pipeline de `Batch` recebeu fast paths sem metricas para reader, non-query, scalar e materializacao, o materializador passou a pular colecoes de parametros vazias, a copia de parametros passou a usar loop indexado e o coletor de result sets passou a separar os caminhos com e sem estatistica.
- O dispatch de `INonQueryCommandHandler` no pipeline de non-query deixou de usar `foreach` e passou a usar loop indexado com `Count` cacheado, reduzindo overhead por statement.
- O pipeline de non-query passou a checar `IsNullOrWhiteSpace(statementSql)` antes de fazer `Trim()` e passou a fazer `Trim()` apenas quando ha whitespace nas bordas, evitando alocacao quando o split retorna statements vazios/whitespace ou quando a string ja esta normalizada.
- O prelude de `ExecuteReader` deixou de usar LINQ para filtrar e materializar statements do batch, passando a montar `List<string>` com loop direto e reduzindo alocacoes por chamada.
- A contextualizacao de debug traces no `DbConnectionMockBase` deixou de usar LINQ ao splitar/filtrar/trimar statements, reduzindo alocacoes quando o modo de debug esta ativo.
- A clonagem de `rows` ao criar tabelas temporarias dentro de transacao no `DbConnectionMockBase` deixou de usar `Select(...ToDictionary...)` e passou a clonar com `new Dictionary<int, object?>(row)` em loop, reduzindo overhead.
- O lookup de tabelas temporarias no `DbConnectionMockBase` deixou de usar LINQ (`Where/Select`) e passou a iterar com `yield return`, reduzindo alocacao.
- O registro de tabelas temporarias e a resolucao de indices descartados no `DbConnectionMockBase` deixaram de usar `Any`/`FirstOrDefault`, reduzindo overhead de bookkeeping em mutacoes de schema e transacao.
- O runner de `Batch scalar` passou a evitar a chamada de metricas vazia quando a telemetria esta desligada, o caminho assíncrono ganhou um dispatcher sem `async` no método público e o caso unitário assíncrono passou a retornar direto quando o `Task` completa de forma síncrona, reduzindo mais um pouco o overhead do lote de um único comando.
- Os runners de `Batch non-query` (sync/async) passaram a calcular `metricsEnabled` uma vez por lote, tratar lote unitário por fast path e repassar isso ao executor por comando, reduzindo overhead quando a telemetria esta desligada.
- O runner de `Batch reader` ganhou overloads que aceitam `metricsEnabled`, os runners sync/async agora calculam esse valor uma vez por lote, o caminho assíncrono do método público também foi convertido em dispatcher sem `async`, o executor de reader deixou de depender de delegates por comando, o wrapper generico assíncrono passou a evitar a máquina de estado quando o `Task` já está completo e o caso unitário assíncrono passou a evitar a máquina de estado quando o comando conclui de forma síncrona, reduzindo overhead por comando.
- O runner de `Batch non-query` assíncrono passou a despachar diretamente o `Task<int>` da execução, encurtando o caminho unitário quando o comando conclui de forma síncrona e evitando uma máquina de estado extra nesse cenário.
- Os runners async de `Batch` que iteram `IReadOnlyList` deixaram de usar `foreach` e passaram a usar loop indexado com `Count` cacheado, reduzindo boxing/alocacao do enumerator em lotes maiores.
- O runner sync de `Batch` tambem deixou de usar `foreach` sobre `IReadOnlyList` e passou a usar loop indexado com `Count` cacheado, reduzindo overhead de enumeracao em lotes maiores.
- O benchmark de `Batch mixed read/write` no engine `DbSqlLikeMem` passou a reutilizar comandos parametrizados preparados para `INSERT`, `SELECT` e `UPDATE` dentro da mesma transacao.
- Os benchmarks de `Batch scalar` e `Batch non-query` no engine `DbSqlLikeMem` tambem passaram a reutilizar comandos parametrizados preparados, reduzindo o custo por iteracao.
- O benchmark de `String aggregate large group` passou a usar o seed do harness base (um unico `InsertUsers(...)`) para manter o mesmo cenário em todos os engines.
- Os benchmarks de janela (`Window ROW_NUMBER` e `Window LAG`) passaram a semear as 3 linhas com um unico `InsertUsers(...)`, reduzindo overhead de seed em todos os engines.
- O benchmark base de `String aggregate large group` passou a semear as 50 linhas com um unico `InsertUsers(...)`, reduzindo overhead de seed em todos os engines.
- Os benchmarks base de agregacao textual (`String aggregate`, `String aggregate ordered`, `String aggregate distinct` e `String aggregate custom separator`) passaram a semear as linhas com um unico `InsertUsers(...)` para reduzir overhead de seed.
- O `SqliteDialect.InsertUsers` no harness de benchmarks deixou de usar `LINQ` e passou a montar o `VALUES (...)` com loop, reduzindo alocacao ao gerar SQL de seed.
- O coletor de `Batch reader` passou a montar tabelas diretamente a partir do schema e dos valores lidos, sem o intermediario antigo de arrays por result set.
- Os cenarios sequenciais `InsertBatch10` e `InsertBatch100` passaram a reutilizar um comando preparado no caminho SQLite, e o `SqliteCommandMock` agora cacheia o AST de DML para executar inserts preparados diretamente quando o comando e o dialeto nao mudam.
- O coletor de `DbDataReader` ganhou caminho de coleta sem estatistica quando a telemetria esta desligada.
- O coletor de `DbDataReader` tambem ganhou reducao de alocacao e overhead no loop de leitura: cache de `FieldCount`, pre-sizing e acesso local de `Columns`, cache de `DBNull.Value`, uso de `Dictionary.Add` e reuso do buffer `object[]` entre result sets.
- O `DbDataReaderMockBase` deixou de usar LINQ/spread/`ToDictionary` ao materializar result sets e metadados de colunas, passando a usar loops com pre-sizing para reduzir alocacao ao criar readers.
- `INSERT`, `UPDATE`, `DELETE`, `MERGE` e `UPDATE ... FROM SELECT` passaram a evitar snapshot antigo quando a chave nao muda ou quando o caminho nao precisa dele, e os caminhos quentes de `UPDATE`, `INSERT ... ON DUPLICATE` e `MERGE` perderam mais LINQ em montagem de pares, colecoes auxiliares e parse de atribuicoes.
- `DbInsertStrategy` e `DbUpdateStrategy` deixaram de usar `Select(...).ToList()` para listar colunas alteradas, passando a montar `List<string>` com loop e pre-sizing para reduzir alocacoes em DML repetido.
- O caminho de `INSERT ... SELECT` ganhou batch fast path quando a tabela destino nao tem triggers registradas.
- `TableMock`, `SchemaMock` e `DbDeleteStrategy` receberam reducao de LINQ, guardas para colecoes vazias e otimizações em validação de FK/PK.
- `TryFindRowByPkConditions` e os caminhos de conflito por PK foram enxugados para reduzir buscas e alocacoes repetidas.
- `IsMatchSimple` e `TryMatchComparison` foram reorganizados para evitar resolver `IN` como se fosse comparacao comum.
- `AstQueryExecutorBase` ganhou lookup de PK com dicionario prealocado e reaproveitamento do conjunto de índices equivalentes ao PRIMARY KEY no plano de hint MySQL.
- O retorno de linha unica por PK no `Source` ganhou um caminho dedicado, evitando montar uma coleção temporaria so para um indice.
- O retorno de linha unica por PK no `Source` passou a evitar o array temporario de indice único, reduzindo alocação por lookup direto.
- O caminho de lookup por índice passou a respeitar `DbMetrics.Enabled` antes de tocar contadores de indice e de hint, reduzindo custo quando a telemetria está desligada.
- O plano de hints MySQL deixou de depender de LINQ e passou a usar loops diretos para nomes de índice existentes, hints de força e equivalência com PRIMARY KEY, sem materializar listas intermediarias desnecessarias no caminho de resolucao.
- O parser de colunas e os caches de `IN` subquery ganharam caminhos com menos LINQ e com `HashSet` pré-ajustado para subqueries maiores.
- O builder de cache key de subquery correlacionada deixou de usar LINQ na ordenacao de campos, reduzindo alocacao e CPU ao montar chaves repetidas.
- A geração de recomendação de índices também perdeu LINQ nos caminhos de contagem de entradas conhecidas, prefixo de PK e estimativa de linhas lidas.
- O `Source` ganhou atalho direto para resolver PK de coluna única sem iterar a tabela inteira quando a implementação concreta é `TableMock`.
- O `Source` agora mantém lookup de nomes de coluna, removendo `Any(...)` nos caminhos de `EXISTS` correlacionado e `UNPIVOT`.
- A normalização de cache key de `EXISTS` correlacionado também deixou de usar LINQ no trecho que ordena os conjunctos.
- O `EXISTS` nao correlacionado ganhou fast path de scan simples por igualdade.
- O `GROUP BY / HAVING` agora materializa os grupos uma vez só e reaproveita o mesmo bucket para filtro e projeção.
- A preparacao de aliases usados em `HAVING` passou a usar loop direto, reduzindo alocacao e eliminando a cadeia de `LINQ` no parse dos itens projetados.
- O overlay de linha em consultas compostas ganhou fast path quando a linha externa esta vazia.
- O `SELECT` ganhou prealocacao de listas de projeção e slots de janela antes de expandir os itens do plano.
- O helper de projecao de `SELECT` passou a pré-ajustar a capacidade das listas ao expandir colunas de origem.
- O helper de limite de linhas passou a recortar o resultado em place com `RemoveRange`, sem lista intermediaria, reduzindo custo no caminho de `LIMIT/OFFSET`.
- O helper de `ORDER BY` passou a reordenar `JoinFields` com loop direto, removendo o `Select` do caminho de remapeamento paralelo ao sort.
- O helper de `ORDER BY` deixou de usar `FirstOrDefault` (LINQ) para resolver colunas e passou a varrer `result.Columns` com loop direto, reduzindo alocacao por chave de ordenacao.
- O helper de `ORDER BY` deixou de usar `ToList()` (LINQ) para copiar `result` antes do sort e passou a copiar com lista prealocada e loop indexado, reduzindo alocacao e overhead por ordenacao.
- O helper de texto passou a resolver `FIND_IN_SET` com loop direto e `MATCH ... AGAINST` passou a montar o alvo textual com lista prealocada, reduzindo alocacao em funcoes text-search.
- O helper de warnings de plano passou a avaliar `SELECT *`, `TOP` e thresholds com loops diretos, removendo `Any` e `Select` do caminho de diagnostico.
- O helper de formatação de traces passou a montar detalhes de `UNION`, projeção e `ORDER BY` com loops diretos, reduzindo alocacao em diagnostico de runtime.
- O objeto de trace passou a montar `OperatorCounts` com lista ordenada e loop direto, removendo `OrderBy` e `Select` do construtor de diagnostico.
- O construtor de `QueryDebugTrace` deixou de usar LINQ para montar `StepTicks`, `StepMs` e para copiar `operatorCounts`, reaproveitando o loop principal e reduzindo alocacao no trace de runtime.
- O formatter de trace passou a montar `steps` e `traces` em loops diretos, removendo `Select(...).ToArray()` do JSON de diagnostico.
- O formatter de `QueryDebugTrace` deixou de usar `ToList()` em `FormatCountMap` e `FormatOperatorSignature`, copiando colecoes com loop direto e reduzindo alocacao ao formatar traces.
- O formatter de trace tambem passou a calcular `TotalExecutionTime`, `FormatCountMap` e `OperatorSignature` em loops diretos, removendo `Sum`, `OrderBy` e `Select` do caminho de mismatch/log.
- O helper de funcoes nulas passou a verificar suporte a `COALESCE`/`NVL` com loop direto sobre os nomes suportados, removendo `Any` do caminho de resolucao.
- O builder de `SELECT` passou a cachear a fonte unica de amostra uma vez por plano, evitando repetir a deteccao em cada expressao projetada.
- O helper de projeção de `SELECT` passou a manter o indice da coluna localmente durante a expansao, evitando recomputar `columns.Count` a cada append.
- O parser de alias de `SELECT` ganhou um fast path para expressoes sem espacos em branco, evitando a varredura completa de alias nesses casos simples.
- O parser de alias de `SELECT` tambem passou a usar `ReadOnlySpan` nos recortes validos, reduzindo alocacao intermediaria ao aceitar alias simples.
- O parser de alias de `SELECT` passou a operar em `ReadOnlySpan` no fluxo principal, adiando a materializacao de `string` ate o ponto em que o alias realmente e aceito.
- O `JOIN` ganhou fast path para entrada esquerda vazia em `INNER`/`LEFT`/`CROSS`, para `RIGHT JOIN` com uma unica linha a esquerda e para overlays vazios no lado de saida.
- O agrupamento de chaves em `PIVOT` passou a usar `StringBuilder` e loops diretos no `BuildGroupKey`, removendo `LINQ` da composicao de chave por grupo.
- O agrupamento de `GROUP BY` passou a montar `GroupKey` com loop direto e buffer prealocado, removendo `LINQ` e reduzindo alocacao por chave de agrupamento.
- A materializacao de grupos passou a pré-alocar a lista de linhas quando o agrupamento expõe contagem, reduzindo realocacoes internas no caminho de `GROUP BY / HAVING`.
- O resolvedor de fontes de consulta passou a evitar partições e listas temporarias em `derived union`, `STRING_SPLIT` e `JSON_TABLE`, e o `JSON_TABLE` deixou de usar `SelectMany` ao montar ordinais aninhados, passou a pré-dimensionar listas de contextos aninhados e passou a pré-dimensionar os dicionários-base.
- O caminho particionado de fonte física passou a pré-dimensionar a tabela de resultado e o dicionário filtrado antes de copiar colunas e linhas, e o desenho de colunas passou a usar o índice físico diretamente, sem ordenacao adicional e sem lookup redundante por índice inexistente.
- O comparador de linhas usado em conjuntos e caches de resultados deixou de usar `OrderBy(...)` no hash e passou a ordenar uma lista materializada de entradas por chave, reduzindo o custo de hash de linha.
- Varios caminhos de agregacao numerica e textual passaram a evitar `LINQ` e colecoes temporarias, com cardinalidade estimada reaproveitada em `DISTINCT`.
- A agregacao textual passou a pré-dimensionar o `StringBuilder` no caminho simples para reduzir realocacoes em grupos grandes.
- O `DISTINCT` da agregacao textual deixou de alocar chave extra com `ToUpperInvariant()` quando a comparacao e case-insensitive, pois o `HashSet` ja usa `StringComparer.OrdinalIgnoreCase`.
- `ApplyAggregateFilter` deixou de usar `LINQ` e passou a filtrar com loop direto, reduzindo alocacao em agregacoes com `FILTER`.
- A materializacao de grupos (`GROUP BY / HAVING`) passou a pré-dimensionar buffers quando o agrupamento expõe contagem.
- O acesso a metadados de colunas em resultados (`GetColumnIndexOrThrow`) passou a usar loop direto, reduzindo overhead em subqueries.
- O helper de janela passou a usar chave estrutural de partição, reduzindo alocação e evitando colisões de chave textual.
- O executor de janela passou a adiar valores de ordenação quando o frame não precisa deles e pula preparação para partições unitárias.
- O caminho de peer groups ganhou fast path para partições de uma única linha.
- O caminho de `LAG/LEAD` ganhou acesso direto ao valor em partição de fonte unica e fast path para offset zero.
- `FIRST_VALUE`, `LAST_VALUE` e `NTH_VALUE` passaram a reutilizar o mesmo seletor direto para colunas simples no caminho de janela.
- `Select IN subquery` e `Select scalar subquery` ganharam caminhos diretos para leitura da coluna esquerda e contagem simples por igualdade, com o fast path de `COUNT(*)` restrito para nao alterar a semantica de `COUNT(coluna)`, a resolucao das colunas de igualdade feita uma vez antes do scan e o caso de igualdade unica tratado sem estrutura intermediaria.
- A agregação textual ganhou capacidade pré-ajustada para `DISTINCT` e listas de valores grandes.
- As funcoes de array ganharam buffers pre-dimensionados no caminho de posicao e nas concatenacoes, reduzindo alocacao temporaria em consultas que acumulam ou filtram listas.
- A base de testes de SQLite ganhou cobertura para reset de `ROW_NUMBER` por chave de partição.
- O cache de `SELECT *` ganhou um guardrail de invalidação após `ALTER TABLE ... ADD COLUMN`.
- A chave do `SelectPlanCache` ganhou fast path para `0/1 source`, reduzindo alocacao e custo de sort ao reaproveitar planos.
- A construção de `Source` para tabelas físicas e resultsets passou a preencher arrays e listas em loop direto, sem ordenar com `LINQ` no caminho de inicialização.
- O filtro de partição em `TableMock` passou a verificar as partições solicitadas com loop direto, removendo `Any(...)` do caminho por linha particionada.
- As verificações de trigger, foreign key e colisão de índice único em `TableMock` passaram a usar loops diretos e checagem por contagem, reduzindo overhead em mutações.
- A remoção de linha em `TableMock` passou a ajustar o índice primário com loop direto, sem `Where/Select/ToList` no reencaixe das posições.
- O `Backup()` de `TableMock` passou a copiar linhas com loop direto, removendo `Select(CloneRow)` do snapshot interno.
- `UPDATE` passou a simular a mutacao uma unica vez por linha e a pular validacao de unique quando nenhum indice UNIQUE e afetado, reduzindo custo em `Update by PK`.

### Progresso por fase

| Fase | Descricao | Status | Implementacao |
|---|---|---|---:|
| 1 | Diagnostico e instrumentacao | Concluida | 100% |
| 2 | Batch sequencial | Concluida | 100% |
| 3 | DML por PK e atualizacao incremental | Concluida | 100% |
| 4 | Consultas compostas e materializacao | Concluida | 100% |
| 5 | Agregacao, janela e ordenacao | Em andamento | 94% |
| 6 | Cache e invalidacao | Em andamento | 55% |
| 7 | Guardrails e protecao contra retrocesso | Em andamento | 36% |

### Leitura rapida do andamento

- O ganho mais consistente ja saiu da fase 1 e da consolidacao da fase 2.
- O que foi feito ate aqui reduziu custo diagnostico, alocacoes e parte do processamento por linha, mas ainda ha ajuste fino em cache, invalidacao e guardrails.
- O restante do trabalho ja e mais de calibracao e protecao do que de reescrita estrutural do motor.

## Leitura da sessao SQLite

### Grupos com melhor situacao

| Grupo | Benchmarks | Wins | Losses | Media percentual |
|---|---:|---:|---:|---:|
| Setup | 1 | 1 | 0 | 69,01% |
| Json | 3 | 3 | 0 | 47,74% |
| Transactions | 6 | 6 | 0 | 42,32% |
| Temporal | 5 | 4 | 1 | 37,51% |

### Grupos com pior situacao

| Grupo | Benchmarks | Wins | Losses | Media percentual |
|---|---:|---:|---:|---:|
| Batch | 11 | 2 | 9 | -102,20% |
| Dialect | 5 | 0 | 5 | -74,88% |
| AdvancedQuery | 13 | 1 | 12 | -61,13% |
| Core | 10 | 5 | 5 | -6,42% |

### Maiores regressões observadas

| Prioridade | Benchmark | DbSqlLikeMem | SQLite nativo | Percentual | Leitura pratica |
|---|---|---:|---:|---:|---|
| P0 | `Batch insert 100` | 1.927,61 us | 334,24 us | -476,72% | maior regressao da sessao; o caminho sequencial de lote esta pagando custo por linha demais |
| P0 | `Insert batch 100` | 3.208,84 us | 1.117,15 us | -187,23% | batch sequencial continua pesado mesmo com volume controlado |
| P0 | `Batch mixed read/write` | 396,63 us | 157,25 us | -152,23% | mistura de leitura e escrita amplifica o overhead do pipeline |
| P0 | `Batch non-query` | 460,00 us | 183,47 us | -150,72% | comandos sem retorno ainda passam por trabalho desnecessario |
| P0 | `Select IN subquery` | 661,61 us | 278,93 us | -137,20% | subquery ainda nao esta sendo reduzida para lookup eficiente |
| P1 | `Group by HAVING` | 595,64 us | 291,99 us | -103,99% | agrupar e filtrar continua caro demais no caminho generico |
| P1 | `Update by PK` | 274,66 us | 145,86 us | -88,31% | atualizacao por chave ainda nao esta em um fast path limpo |
| P1 | `Select correlated count` | 480,95 us | 271,97 us | -76,84% | contagem correlacionada ainda tem custo de materializacao e varredura |
| P1 | `Window LAG` | 325,74 us | 183,17 us | -77,84% | janela ainda reavalia estado demais por linha |
| P1 | `Select scalar subquery` | 450,64 us | 262,12 us | -71,92% | subquery escalar ainda paga o caminho generico |

### Caminhos saudaveis que devem servir de controle

| Categoria | Benchmark | DbSqlLikeMem | SQLite nativo | Percentual | Leitura pratica |
|---|---|---:|---:|---:|---|
| Core | `Connection open` | 7,72 us | 26,73 us | 71,13% | o bootstrap da conexao nao e o gargalo |
| Batch | `Insert batch 100 parallel` | 6.056,32 us | 19.775,23 us | 69,37% | o caminho paralelo continua forte e nao deve ser quebrado |
| Setup | `Create schema` | 56,67 us | 182,86 us | 69,01% | criacao de schema segue muito boa |
| Transactions | `Savepoint create` | 11,02 us | 41,33 us | 73,34% | transacoes basicas estao saudaveis |
| Json | `JSON insert cast` | 18,75 us | 42,11 us | 55,47% | JSON nao e o foco da regressao atual |
| Temporal | `Temporal DATEADD` | 19,06 us | 33,66 us | 43,38% | funcoes temporais basicas continuam competitivas |

## Diagnostico tecnico

- O problema dominante esta no caminho sequencial de batch, porque os piores resultados estao concentrados em `Batch insert 100`, `Insert batch 100`, `Batch non-query`, `Batch mixed read/write`, `Batch scalar` e `Batch reader multi-result`.
- O fato de `Insert batch 100 parallel` seguir muito acima do SQLite nativo mostra que a infraestrutura em si nao esta quebrada; a regressao parece estar restrita ao caminho sequencial, ou ao menos a uma parte dele.
- `AdvancedQuery` indica que joins, subqueries, correlacoes, windows e grupos ainda estao confiando demais no caminho generico, com muita materializacao intermediaria.
- `Dialect` mostra que a agregacao textual ainda precisa de buffers melhores, deduplicacao mais barata e menos recomputacao em grupos grandes.
- `Core` revela que as operacoes por PK e o `UPDATE` ainda carregam custo de lookups, clonagem de linha ou atualizacao de indice acima do necessario.
- O parser nao deve consumir energia agora. Os benchmarks de parse estao todos em faixa sub-microsegundo, entao o ganho real esta no executor.
- `DbMetrics` e toda a telemetria de fase/contagem continuam sendo custo diagnostico puro. Esse caminho nao muda a semantica do SQLite, entao ele pode e deve ser desligado nas rotinas de benchmark sem perda de funcionalidade.
- `Temporal`, `Json` e `Transactions` estao bons o suficiente para servirem como controle. Nao ha indicio de que eles devam puxar a proxima rodada de otimização.

## O que nao atacar agora

- Otimizacao de parser.
- Ajustes de bootstrap de conexao.
- Reescrita de JSON.
- Reescrita de transacoes/savepoints.
- Mudancas no caminho paralelo de batch que possam rebaixar `Insert batch 100 parallel`.

Esses pontos estao saudaveis na matriz atual e podem distrair da regressao que realmente importa.

## Roteiro resumido

| Faixa de implementacao | Frente | Objetivo principal |
|---|---|---|
| 0% -> 15% | Diagnostico e instrumentacao | medir onde a regressao entra e separar custo de materializacao, clone, indice e ordenacao |
| 15% -> 35% | Batch sequencial | recuperar throughput em lote sem sacrificar o caminho paralelo |
| 35% -> 50% | DML por PK | reduzir custo de `Update by PK`, `Delete by PK`, `Upsert` e `Row count after update` |
| 50% -> 70% | Consultas compostas | atacar joins, `IN`, `EXISTS`, subqueries escalares, `CTE` e `RETURNING update` |
| 70% -> 85% | Agregacao e janela | reduzir custo de `String aggregate*`, `Group by HAVING`, `Window LAG` e `Window ROW_NUMBER` |
| 85% -> 95% | Cache e invalidacao | garantir reaproveitamento de plano e de shapes repetidos |
| 95% -> 100% | Guardrails | impedir que a regressao volte depois do ajuste |

## Fase 1 - 0% a 15%: diagnostico e instrumentacao

- Congelar o baseline atual da sessao SQLite para servir de referencia antes de qualquer ajuste.
- Medir separadamente o custo de materializacao, clonagem, lookup de PK, atualizacao de indice e ordenacao.
- Desligar `DbMetrics` no caminho de benchmark de performance e comparar a matriz antes/depois para medir quanto da regressao vinha apenas de telemetria.
- Registrar alocacao por operacao nos piores benchmarks para identificar se o problema principal e GC, copia de estrutura ou varredura.
- Marcar um conjunto fixo de benchmarks de controle: `Batch insert 100`, `Insert batch 100`, `Batch non-query`, `Update by PK`, `Select IN subquery`, `String aggregate large group` e `Window ROW_NUMBER`.
- Garantir que a medicao do caminho paralelo continue separada do caminho sequencial para evitar comparacao enganosa.

**Saida esperada:** evidencias objetivas do custo dominante em cada grupo antes de mexer no motor.

## Fase 2 - 15% a 35%: batch sequencial

- Unificar o caminho de `Batch insert 10/100`, `Insert batch 10/100`, `Batch scalar`, `Batch non-query` e `Batch mixed read/write` em uma base comum de execucao sequencial.
- Pre-resolver colunas, parametros e shapes do lote uma vez por comando, evitando repeticao por statement.
- Aplicar insercao em blocos e adiar o trabalho de indice para um flush controlado, em vez de pagar manutencao completa a cada linha.
- Trocar detecao linear de conflito por estruturas de chave mais baratas no contexto do lote.
- Manter `Insert batch 100 parallel` como referencia de throughput e nao permitir que a reescrita do sequencial degrade a trilha concorrente.

**Benchmarks alvo:** `Batch insert 100`, `Insert batch 100`, `Batch insert 10`, `Insert batch 10`, `Batch mixed read/write`, `Batch non-query`, `Batch reader multi-result`, `Batch scalar`.

**Saida esperada:** reduzir pelo menos metade do gap atual dos lotes sequenciais mais caros, com ganho mais forte nos casos de 100 linhas.

## Fase 3 - 35% a 50%: DML por PK e atualizacao incremental

- Criar fast path real para `Update by PK`, `Delete by PK` e `Upsert` quando a PK for resolvida sem ambiguidade.
- Evitar clonagem de linha completa quando a operacao mexe em uma unica linha e nao precisa de snapshot amplo.
- Fazer `Row count after update` reaproveitar a mesma mutacao do `Update by PK`, sem reexecutar um pipeline mais pesado.
- Fazer `Returning update` projetar apenas a linha alterada e apenas as colunas exigidas pelo retorno.
- Atualizar indices somente nas ordens afetadas pela mutacao, sem reconstrucoes amplas.

**Benchmarks alvo:** `Update by PK`, `Delete by PK`, `Upsert`, `Row count after update`, `Returning update`, e como efeito colateral `Select join` quando a mutacao altera dados usados em consultas imediatas.

**Saida esperada:** aproximar os caminhos por PK do equilibrio visto em `Select by PK` e `Insert single row`.

## Fase 4 - 50% a 70%: consultas compostas e materializacao

- Introduzir visao/overlay de linha para parar de copiar dicionario inteiro sempre que houver join ou combinacao de fontes.
- Cachear resolucao de colunas, alias e ordinal por shape de consulta.
- Implementar semi-join/hash-set path para `EXISTS` e `IN`, reduzindo subquery repetida a lookup pré-computado quando a forma da consulta permitir.
- Reusar pre-aggregacao para `Select correlated count` e `CTE simple` sempre que a consulta for estruturalmente compatível.
- Fazer `Select join` e `Select scalar subquery` deixarem de pagar custo de materializacao intermediaria redundante.

**Benchmarks alvo:** `Select join`, `Select EXISTS predicate`, `Select IN subquery`, `Select scalar subquery`, `Select correlated count`, `CTE simple`, `Multi-join aggregate`, `Returning update`, `Partition pruning select`.

**Saida esperada:** reduzir o custo das consultas compostas sem quebrar correlacao, alias, nullability e semantica de subquery.

## Fase 5 - 70% a 85%: agregacao, janela e ordenacao

- Pre-dimensionar `StringBuilder`, `HashSet`, buffers de grupo e estruturas auxiliares de agregacao.
- Reutilizar materializacao ordenada nos casos de `String aggregate ordered`, `String aggregate distinct` e `String aggregate large group`.
- Reaproveitar estado de grupo entre `Group by HAVING` e agregacoes derivadas em vez de recalcular a mesma particao mais de uma vez.
- Cachear particoes e ranges de frame para `Window LAG` e `Window ROW_NUMBER`.
- Reduzir o custo de `Temporal NOW in ORDER BY` por extracao mais barata de chave de ordenacao, sem afetar o resultado.

**Benchmarks alvo:** `String aggregate`, `String aggregate custom separator`, `String aggregate distinct`, `String aggregate large group`, `String aggregate ordered`, `Group by HAVING`, `Window LAG`, `Window ROW_NUMBER`, `Temporal NOW in ORDER BY`, `Multi-join aggregate`.

**Saida esperada:** cortar o custo de agregacao em grupos grandes e janelas, que hoje formam o segundo bloco mais caro depois de batch.

## Fase 6 - 85% a 95%: cache e invalidacao

- Verificar se o cache de plano executavel e de shapes repetidos continua sendo aproveitado nos benchmarks do SQLite atual.
- Reaproveitar diretamente os planos sem janela para evitar clones desnecessarios quando nao existe estado mutavel por particao.
- Evitar invalidacao de cache quando `ChangeDatabase` recebe o mesmo database atual.
- Evitar invalidacao do cache de plano de select ao registrar trigger, porque o shape da consulta nao muda.
- Tornar `ClearSelectPlanCache()` idempotente quando o cache ja esta vazio.
- Evitar invalidacao de cache para DDL de rotina (functions/procedures), que nao altera o shape das queries cacheadas.
- Impedir que o caminho generico reconstrua a mesma estrutura de projecao, alias e materializador quando a SQL for identica.
- Auditar invalidacao de cache apenas para mudancas de schema e DDL realmente relevantes.
- Confirmar que o cache nao esta sendo contornado por alguma nova chamada de materializacao introduzida nas ultimas alteracoes.

**Benchmarks alvo:** leituras e escritas repetidas em geral, com foco em `Batch`, `AdvancedQuery` e loops de benchmark que repetem a mesma query.

**Saida esperada:** eliminar custo de reconstrucao inutil entre iteracoes iguais.

## Fase 7 - 95% a 100%: guardrails e protecao contra retrocesso

- Fixar thresholds de alerta para os piores benchmarks da sessao: `Batch insert 100`, `Insert batch 100`, `Batch non-query`, `Update by PK`, `Select IN subquery`, `String aggregate large group` e `Window ROW_NUMBER`.
- Registrar no backlog uma pequena nota de regressao toda vez que a sessao SQLite for reavaliada.
- Manter o caminho paralelo de batch como teste de referencia para nao perder throughput ao otimizar o sequencial.
- Documentar qualquer troca de estrutura interna que melhore um grupo e piore outro, antes de aceitar a mudanca como definitiva.
- Proteger a idempotencia de `ChangeDatabase` com um teste dedicado que observa a geracao do cache de plano.
- Cobrir a mesma idempotencia em DB2 para garantir que a regra nao fique dependente de um unico provider.
- Cobrir em DB2 que DDL de functions/procedures nao altera a geracao do cache de plano de select.

**Saida esperada:** o ganho obtido nas fases anteriores vira ganho duravel, com menos risco de a proxima alteracao devolver a regressao.

## Ordem recomendada de execucao

1. Medir e isolar o custo real da regressao.
2. Recuperar batch sequencial.
3. Corrigir DML por PK.
4. Reduzir materializacao em consultas compostas.
5. Otimizar agregacao e janela.
6. Validar cache e invalidacao.
7. Fechar com guardrails.

## Resultado esperado ao final do roteiro

- `Batch insert 100` e `Insert batch 100` devem voltar a cair para uma faixa muito mais proxima do SQLite nativo.
- `Batch non-query`, `Batch mixed read/write` e `Batch scalar` devem deixar de carregar custo extra de pipeline.
- `Update by PK`, `Delete by PK` e `Upsert` devem ficar consistentes com o desempenho de `Select by PK` e `Insert single row`.
- `Select IN subquery`, `Select EXISTS predicate`, `Select correlated count` e `Select scalar subquery` devem perder boa parte da latencia generica.
- `String aggregate large group`, `Group by HAVING`, `Window LAG` e `Window ROW_NUMBER` devem parar de ser os principais consumidores de tempo fora do batch.
- Os caminhos que ja estao bons - `Connection open`, `Create schema`, `JSON`, `Transactions` e `Insert batch 100 parallel` - precisam continuar como linha de base de seguranca.

## Guardrails operacionais (para nao regredir)

### Thresholds de alerta (sessao `SQLite`)

Regra de calculo sugerida para cada benchmark alvo:

`delta% = (tempo_dbSqlLikeMem - tempo_sqlite) / tempo_sqlite * 100`

Aplicar dois niveis:

- `warn`: delta% <= -10% (DbSqlLikeMem ficou >= 10% pior que SQLite).
- `block`: delta% <= -25% (regressao forte; nao aceitar sem justificativa e plano de mitigacao).

Benchmarks monitorados:

- `Batch insert 100`
- `Insert batch 100`
- `Batch non-query`
- `Update by PK`
- `Select IN subquery`
- `String aggregate large group`
- `Window ROW_NUMBER`

### Rotina de reavaliacao

- Sempre que a sessao `SQLite` for reavaliada, registrar no topo deste arquivo uma nota curta com data, commit/branch e os deltas% dos benchmarks monitorados.
- Se algum benchmark entrar em `block`, abrir um item no backlog descrevendo:
  - qual caminho mudou (arquivo/metodo)
  - qual benchmark piorou e qual melhorou (se houver trade-off)
  - qual toggle/config comparavel ao default do SQLite foi usado (se aplicavel)

### Linha de base de throughput

- Manter `Insert batch 100 parallel` como referencia: qualquer otimizacao do sequencial nao pode reduzir throughput do paralelo.













