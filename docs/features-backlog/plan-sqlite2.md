# Plano SQLite - Ciclo 2

## Resumo
- O baseline antigo do tracker ainda está em 8,22%/6 vitórias, mas a matrix atual do SQLite caiu para 5,48%/4 vitórias em 73 casos comparáveis.
- O novo ciclo precisa ser rebased a partir da matrix mais recente, porque o tracker antigo já não representa o estado real do benchmark.
- A dívida está concentrada em `AdvancedQuery` 1/26, `Batch` 1/11, `Core` 0/13, `Dialect` 0/5, `Json` 0/3, `Setup` 0/4, `Temporal` 0/5 e `Transactions` 2/6.
- As vitórias que devem ficar travadas como não-regressão são `Returning insert`, `Insert batch 100 parallel`, `Release savepoint` e `Savepoint create`.
- A prioridade desta rodada é recuperar tração com ganhos rápidos no write path e depois atacar os clusters de execução mais caros.

## Mudanças de Implementação
- Wave 1: fechar vitórias rápidas em `Transactions`, `Batch` e `Core`, começando por `Rollback to savepoint`, `Batch insert 100`, `Parameter projection` e `Insert single row`, depois estendendo para `Parameter insert single`, `Insert batch 10`, `Row count after insert/select/update`, `Select by PK`, `Update by PK`, `Delete by PK`, `Upsert` e `Returning update`; o foco é cortar alocação, scans repetidos e cópias temporárias nos caminhos de escrita e contagem.
- Wave 2: atacar o motor de query nos clusters mais caros, cobrindo `Select IN/NOT IN`, `EXISTS/NOT EXISTS`, `Select correlated count`, `Select join`, `Select LEFT JOIN anti-join`, `Multi-join aggregate`, `Group by HAVING`, `Select scalar CASE matrix`, `CTE simple`, `UNION` e `Window*`; o foco fica nos helpers de join, subquery, aggregate, set-op e window.
- Wave 3: reduzir o custo dos helpers especializados, priorizando `String aggregate custom separator`, `String aggregate distinct`, `String aggregate large group`, `String aggregate ordered`, `JSON path read`, `JSON scalar read`, `JSON insert cast`, `Temporal DATEADD` e os demais `Temporal*`; o foco fica em JSON, temporal e agregação de strings.
- Wave 4: fechar a cauda de DDL e teardown, priorizando `Create schema`, `Create table with FK`, `Create table with FK insert`, `Drop table`, `Transaction commit`, `Transaction rollback` e `Nested savepoint flow`; o foco fica em `TableMock`, `SchemaMock`, `DbTransactionMockBase` e nos handlers de transação e DDL.
- Em todas as waves, qualquer mudança deve preservar o desempenho já conquistado nos quatro wins atuais e não reabrir regressões nos mesmos fluxos.

## Validação e Acompanhamento
- Depois de cada wave, rerodar somente o benchmark do SQLite, regenerar `performance-matrix.md` a partir do relatório novo e atualizar o tracker com baseline, tabela por categoria e log curto da onda concluída.
- Considerar a wave fechada apenas quando o grupo alvo melhorar e nenhum dos quatro wins atuais regredir.
- Tratar `performance-matrix.md` como artefato gerado; o progresso humano deve ficar no tracker, não na matrix.
- Se uma melhora vier apenas do benchmark harness e não do runtime de produção, registrar isso separadamente antes de contar a wave como concluída.

## Assunções
- O novo arquivo de plano pode ficar como `plan-sqllite-cycle2.md`, mantendo o prefixo atual do repositório.
- O tracker acompanhante deve começar do baseline atual da matrix, não do baseline antigo de 6 vitórias.
- O tracker novo deve registrar progresso por wave e por categoria, e a numeração pode reiniciar como `Ciclo 2` para não confundir com as waves já concluídas.
- Esta fase usa validação benchmark-only; build e test ficam fora do fluxo até você pedir execução explícita.
- Benchmarks de `Parse*` e outros casos já muito baratos não entram como alvo separado, a menos que sejam afetados por uma mudança maior.
