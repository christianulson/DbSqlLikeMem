Análise de performance da DbSqlLikeMem versus SQLite e plano de ação
Contexto

A DbSqlLikeMem implementa um banco de dados em memória compatível com a sintaxe SQL. Para aferir o desempenho da biblioteca, foi produzido um teste de performance comparando operações do DbSqlLikeMem contra o SQLite nativo. O comparativo evidencia onde o in-memory DB é competitivo e onde apresenta gargalos, especialmente em operações envolvendo grandes volumes ou funcionalidades avançadas.

A seguir analiso os principais resultados do teste e proponho um plano de ação para melhorar a performance sem sacrificar funcionalidades.

Principais descobertas dos testes

A tabela de desempenho comparou diversos grupos de operações, medindo o tempo médio (μs) de execução e o percentual de diferença em relação ao SQLite. Valores negativos indicam que a DbSqlLikeMem foi mais lenta que o SQLite.

1. Consultas avançadas

Operações com retorno (RETURNING) – Inserções e atualizações com cláusula RETURNING foram ~51‑52 % mais lentas que o SQLite.

Funções de janela – As funções WINDOW LAG e ROW_NUMBER apresentaram diferenças de ~52‑55 % negativas.

2. Lotes (batch)

Inserção em lote – Inserir 10 linhas apresentou diferença de 144 % (mais lento); inserir 100 linhas resultou em 766 % de diferença negativa. O cenário de escrita/leitura mista também foi ~82 % mais lento.

Chamadas de comando sem retorno – Os lotes de chamadas que não retornam linhas (batch non-query) ficaram 194 % mais lentos.

3. Operações básicas (core)

Seleção/Atualização/Remoção por chave primária – Seleções por PK foram ~74 % mais lentas; deleções por PK, ~63 %, e atualizações por PK, ~63 %.

Upsert (insert ou update) – Operação de upsert foi ~59 % mais lenta.

Inserção de linha única – Apesar de serem um pouco mais lentas, as diferenças foram pequenas e em alguns casos a biblioteca foi mais rápida que o SQLite.

4. Funcionalidades do dialeto e agregação de strings

Agregação de strings – A operação com separador customizado foi ~68 % mais lenta; agregação com DISTINCT, ~109 %; agregação em grupos grandes, 321 % mais lenta.

5. JSON

Inserção com CAST para JSON – ~27 % mais lenta.

Leitura de caminho JSON – ~80 % mais lenta.

Leitura de valor escalar JSON – ~160 % mais lenta.

6. Funções temporais

Funções de data/hora – DATEADD, CURRENT_TIMESTAMP e operações de data foram 64 % a 146 % mais lentas que o SQLite.

7. Transações

Savepoints aninhados – Operações savepoint são cerca de 40 % mais lentas e commits dentro de transações são ~59 % mais lentos.

Causas principais encontradas no código

A análise do repositório christianulson/DbSqlLikeMem revelou pontos que explicam as diferenças de desempenho:

Estruturas de dados – TableMock armazena as linhas em uma lista (_items) e mantém índices em dicionários para colunas indexadas. A verificação e atualização de índices ocorre a cada inserção, atualização ou exclusão. Operações por PK usam varreduras lineares ou buscas nos índices, o que explica a lentidão em Delete/Select/Update by PK quando há muitas linhas.

Atualização de índices em massa – Nos insert ou update, depois de cada linha inserida/alterada, a biblioteca atualiza todos os índices. Para upsert e inserts com RETURNING, o código chega a reconstruir todos os índices após cada operação.

Uso intensivo de reflexão – A construção de linhas e a conversão de propriedades utilizam reflection (GetProperties, Activator.CreateInstance), que é custosa quando executada repetidamente.

Parse de SQL e AST – Cada comando é convertido para uma árvore de sintaxe abstrata e interpretado. Não há cache para sentenças repetidas; cada batch executa novamente o parse e a construção da AST.

Funções JSON e temporais – A biblioteca utiliza System.Text.Json para serializar e desserializar valores e percorre o documento JSON a cada chamada de JSON_VALUE/JSON_QUERY. Já as funções temporais interpretam tokens (e.g., 'day', 'month') a cada chamada, sem caching.

Gatilhos e transações – As operações de savepoint e commit criam cópias completas das linhas para possibilitar rollback, o que gera sobrecarga nos testes de transação.

Plano de ação para otimizar a performance

A seguir está um conjunto de melhorias prioritárias que podem reduzir drasticamente o tempo de execução, mantendo as funcionalidades existentes:

1. Otimizar estruturas de dados e índices

Índice por chave primária com Dictionary<TKey, int>: manter um dicionário que mapeie o valor de PK para a posição (ou referência) da linha. Assim, Select/Update/Delete by PK serão operados em O(1) em vez de percorrer a lista. Este índice deve ser atualizado nas operações de inserção, update ou delete.

Atualização incremental de índices: ao invés de chamar RebuildAllIndexes() após cada inserção ou upsert, atualizar somente o índice alterado e adiar a reconstrução completa para casos específicos (por exemplo, quando o índice ficar muito desbalanceado). Para batch inserts, pode-se construir os índices de uma só vez ao final do lote.

Estrutura de dados para lotes: implementar uma estratégia de inserção em lote onde as linhas são adicionadas a uma lista temporária e os índices são atualizados em bloco, evitando overhead a cada linha.

2. Cache de planos e parsing

Cache de comandos preparados: manter um cache que mapeie a expressão LINQ ou a string SQL para a AST já analisada, reutilizando-a quando a mesma consulta for executada novamente. Isto beneficia lotes e operações repetidas (e.g., Insert batch e Upsert).

Cache de expressões de tradução: para consultas LINQ que resultam em SQL, armazenar o texto SQL e o delegado de materialização, evitando reconstruir StringBuilder e realizar reflexão.

3. Reduzir uso de reflexão

Compilar delegados para acesso a propriedades: utilizar Expression ou fontes como System.Reflection.Emit para gerar acesso direto às propriedades das entidades. Isso elimina chamadas repetidas a GetProperty e Convert.ChangeType ao mapear linhas da tabela para objetos.

Gerar classes proxy para as linhas, com métodos fortemente tipados, e tratar valores simples (int, string, datetime) sem conversão boxing/unboxing.

4. Melhorar operações de batch e returning

Bufferização de retornos: em INSERT … RETURNING e UPDATE … RETURNING evitar clonar toda a linha antes da execução; capturar apenas os valores necessários para retornar. Isso reduz o overhead de copiar dicionários grandes.

Batch insert/upsert: permitir que o usuário passe uma lista de objetos; a biblioteca deve construir as linhas e validar conflitos numa única passagem, atualizando os índices ao final. Isso reduz drasticamente os tempos de Insert batch 10/100, que hoje são 144 % e 766 % mais lentos.

5. Otimizações específicas para JSON e temporal

Parser de JSON otimizado: substituir operações repetitivas de serialização/desserialização por funções que utilizem Utf8JsonReader ou JsonDocument e mantenham caches de caminhos consultados; converter os documentos JSON apenas uma vez por linha e reusar o objeto parseado.

Cache de expressões JSON path: compilar os caminhos JSON ($.a.b) em delegates que navegam em um JsonElement evitando interpretações de string a cada leitura.

Temporal: nas funções DATEADD, DATEDIFF e CURRENT_TIMESTAMP, utilizar diretamente DateTime e TimeSpan da linguagem, evitando parsing de tokens. Pode-se criar um dicionário estático que mapeie tokens para ações de adição/subtração.

6. Funções de janela e agregação

Materialização incremental de grupos: ao invés de recalcular cada agregação de string para cada linha, usar buffers reutilizáveis. Para STRING_AGG com DISTINCT e grupos grandes, utilizar HashSet e estruturas de StringBuilder pré-alocadas, evitando reconcatenação completa. Isso ajudará a reduzir o overhead de 68 % a 321 % observado.

Implementar ROW_NUMBER e LAG sobre índices: ao criar índices para ordenação, recalcular apenas as colunas necessárias e reutilizar a ordenação entre múltiplas chamadas.

7. Otimização das transações e savepoints

Estratégia de log de alterações: em vez de clonar todas as linhas a cada savepoint, manter um log com as alterações (inserções, deleções e updates) e suas versões antigas. Em um rollback ou release de savepoint, aplicar ou descartar apenas as mudanças relevantes.

Granularidade de backup: permitir a configuração do nível de backup (linha/coluna) para que transações simples não precisem duplicar a tabela inteira.

8. Paralelização e multithreading (opcional)

Para operações intensivas em leitura, especialmente funções de agregação ou varreduras, explorar paralelização (e.g., Parallel.ForEach) pode reduzir o tempo de execução em máquinas com múltiplos núcleos. Deve-se avaliar as dependências de thread safety dos índices e das estruturas de linha.

9. Monitoramento e testes contínuos

Benchmarking incremental: incluir um modo de teste interno que registre o tempo de cada operação e identifique regressões durante o desenvolvimento. Manter a suíte de benchmarks atualizada ajudará a medir o efeito das mudanças.

Perfis e logs: instrumentar o código para registrar métricas de uso (tempo gasto em parse, reflexão, rebuild de índices) a fim de direcionar esforços de otimização.

Conclusão

Os testes mostram que a DbSqlLikeMem é suficientemente rápida em operações simples, mas carece de otimizações para lidar com cargas grandes, consultas avançadas e funções ricas. A maior parte do overhead vem de varreduras lineares, rebuild de índices a cada operação, reflexão excessiva e parsing sem cache. A aplicação das estratégias descritas — índices eficientes, atualizações em lote, caching de planos, otimizações JSON/temporal e uma revisão na gestão de transações — deve reduzir significativamente as diferenças observadas, alinhando o desempenho da biblioteca ao do SQLite enquanto preserva a compatibilidade funcional.