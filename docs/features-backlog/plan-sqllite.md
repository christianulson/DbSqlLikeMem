**Título**
Melhoria de performance do SQLite no DbSqlLikeMem

**Resumo**
- Vou usar `docs\Wiki\performance-matrix.md` como linha de base e atacar primeiro os maiores gaps onde o SQLite nativo vence com mais folga.
- O objetivo é fazer a aplicação ficar igual ou mais rápida que o SQLite nativo nos casos comparáveis, sem regredir as vitórias atuais.
- Vou criar um arquivo de acompanhamento em `docs\Wiki\BenchmarkResults\sqlite-performance-improvement-tracker.md` e atualizá-lo com progresso total e por categoria a cada onda concluída.

**Mudanças de Implementação**
- Prioridade escolhida: atacar por maior gap de performance, não pela ordem da matrix.
- O primeiro bloco vai focar nos caminhos quentes de CRUD e alocação:
- `TableMock` e estruturas correlatas para reduzir wrappers e acessos repetidos em loops quentes.
- Insert, update, delete, upsert, row count e batch paths para diminuir custo por operação.
- Slice atual: reduzir o custo por linha em `INSERT ... VALUES` com colunas alvo pré-calculadas e fast paths para valores simples já normalizados.
- Próximo ajuste nesta faixa: alinhar os caminhos de `ON CONFLICT WHERE` e `ON DUPLICATE KEY UPDATE` ao resolvedor central de parâmetros e remover o scan manual redundante por parâmetro.
- Ajuste concluído nesta rodada: o resolvedor central de parâmetros deixou de alocar candidatos intermediários e passou a fazer uma única passada sobre a coleção.
- Ajuste concluído nesta rodada: a resolução de parâmetros posicionais `?` deixou de montar uma lista intermediária e passou a localizar o próximo placeholder em uma única passada.
- Ajuste concluído nesta rodada: `UPDATE` e `DELETE` agora evitam `Fork()` por linha quando o `WHERE` simples não usa placeholders posicionais.
- Ajuste concluído nesta rodada: `UPDATE SET` agora evita `Trim()` desnecessário na expressão quando o texto já chega normalizado.
- Ajuste concluído nesta rodada: o resolvedor de índices no caminho de consulta agora usa loops explícitos para validar cobertura e montar chaves sem LINQ.
- Ajuste concluído nesta rodada: o caminho de `COUNT` por índice agora conta direto sem materializar linhas intermediárias.
- Ajuste concluído nesta rodada: a normalização de caminhos JSON deixou de usar `Split/Select/Where` no formato brace-list.
- Ajuste concluído nesta rodada: o parser temporal deixou de usar `Trim()` alocador no caminho Firebird e no parse de offset.
- Ajuste concluído nesta rodada: o parser de `DATA SOURCE` e a normalização de savepoint passaram a evitar trabalho repetido no caminho de conexão/transação.
- Ajuste concluído nesta rodada: a normalização de caminhos JSON ficou sem lista temporária e a escolha do schema importado deixou de usar LINQ.
- Ajuste concluído nesta rodada: o normalizador de caminhos JSON passou a usar `Span` do início ao fim, incluindo o caso `RETURNING`.
- Ajuste concluído nesta rodada: o parser de JSON path passou a cortar o texto inicial com `Span` antes de materializar a spec.
- Ajuste concluído nesta rodada: o parser de JSON path passou a materializar a spec depois do corte de `lax/strict`, mantendo a normalização consistente.
- Ajuste concluído nesta rodada: o contextualizador de traces de debug passou a evitar `Trim()` quando a statement já vem normalizada.
- Ajuste concluído nesta rodada: o parser de JSON path passou a usar `Span` até a leitura de tokens, sem materializar o texto inteiro da rota modificada.
- Ajuste concluído nesta rodada: o parser de JSON path do helper comum passou a ler a spec em `Span` e só materializar a rota normalizada no fim do parse.
- Ajuste concluído nesta rodada: o contador correlacionado passou a usar a contagem direta do índice em vez de enumerar uma sequência materializada.
- Ajuste concluído nesta rodada: o correlacionador de EXISTS/COUNT passou a reutilizar qualquer `IReadOnlyList` materializada em vez de limitar o atalho ao tipo `List`.
- Ajuste concluído nesta rodada: os acumuladores correlacionados de EXISTS/COUNT passaram a aproveitar `IReadOnlyList` nos dois blocos de construção para evitar o fallback enumerador desnecessário.
- Próximo ajuste nesta faixa: reduzir o custo restante de projeção em `INSERT ... SELECT` e nos caminhos de consulta que ainda materializam mais do que precisam.
- O segundo bloco vai focar em query execution:
- Fast paths de PK, índices, subqueries correlacionadas, EXISTS/NOT EXISTS, IN/NOT IN, JOINs, CTE, UNION/UNION ALL e agregações.
- O terceiro bloco vai focar em funções especializadas:
- String aggregate, JSON e funções temporais.
- O quarto bloco vai focar em transações e DDL:
- Savepoints, rollback journal, commit/rollback e criação/remoção de schema/tabela.
- As vitórias atuais serão preservadas como critérios de não regressão:
- Returning insert
- Insert batch 100 parallel
- Insert single row
- Create table with FK
- Release savepoint
- Savepoint create

**Tracker**
- Vou registrar no tracker o baseline atual:
- 73 casos comparáveis
- 6 vitórias da aplicação
- 67 casos onde o SQLite nativo está mais rápido
- Vou manter percentual total e percentual por categoria.
- Vou incluir histórico curto de cada onda concluída e o próximo foco de otimização.
- A `performance-matrix.md` continuará sendo tratada como artefato gerado, não como documento manual de progresso.

**Validação**
- Nesta fase de planejamento, não vou rodar `dotnet build` nem `dotnet test`.
- Quando sairmos do plano, a validação será por benchmarks focados no SQLite e conferência do novo relatório/matrix.
- Cada onda só será considerada pronta quando os percentuais da tracker melhorarem e nenhuma vitória existente regredir.

**Assunções**
- Vou preservar alterações locais já existentes, inclusive o ajuste em `src\benchmark\DbSqlLikeMem.Benchmarks\README.Benchmarks.md`.
- Vou otimizar primeiro os maiores gaps para maximizar ganho global mais rápido.
- Se um caso estiver lento por custo do benchmark e não do código de produção, vou separar isso claramente antes de mudar a implementação.
