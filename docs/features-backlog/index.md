# Índice macro de funcionalidades da aplicação (DbSqlLikeMem)

Este documento organiza as funcionalidades do DbSqlLikeMem em camadas de profundidade: visão macro → áreas funcionais → submódulos → recursos específicos → casos de uso.

> Objetivo: servir como **mapa de backlog funcional**, **referência de arquitetura** e **guia de evolução** para parser/executor e integrações.

---

## 1) Núcleo da aplicação (engine em memória)

### 1.0 Contexto e objetivos do núcleo

- Entregar um banco em memória voltado para **confiabilidade de teste**, não para throughput de produção.
- Permitir que o time valide regra de negócio com SQL realista antes da etapa de integração com infraestrutura.
- Garantir previsibilidade: mesmo input deve gerar o mesmo estado final para facilitar investigação de falhas.

### 1.1 Motor de banco em memória

#### 1.1.1 Persistência temporária em memória

- Implementação estimada: **70%**.
- Estruturas para representar tabelas, colunas, linhas e metadados sem dependência de servidor externo.
- Armazenamento volátil por instância de banco mock, permitindo reset completo entre testes.
- Modelo ideal para testes unitários que exigem alta repetibilidade.

#### 1.1.2 Isolamento para testes unitários

- Implementação estimada: **70%**.
- Execução sem I/O de rede obrigatório.
- Cenários independentes de disponibilidade de banco real.
- Redução de flakiness em pipelines de CI.

#### 1.1.3 Estado e ciclo de vida

- Implementação estimada: **70%**.
- Estado de dados acoplado ao objeto de contexto/conexão mock.
- Facilita setup/teardown por teste, fixture ou suíte.
- Permite compor ambientes mínimos para validação de regra de negócio.

### 1.2 Parser SQL

#### 1.2.1 Interpretação de comandos DDL

- Implementação estimada: **70%**.
- Leitura e processamento de comandos de definição de schema.
- Suporte a operações estruturais comuns (criação e alteração de entidades).
- Aplicação de regras específicas por dialeto e versão simulada.

#### 1.2.2 Interpretação de comandos DML

- Implementação estimada: **96%**.
- Processamento de comandos de escrita e leitura.
- Tradução da consulta para operações no estado em memória.
- Hardening recente reforça parsing de DML com `RETURNING` (itens vazios, vírgula inicial e vírgula final) com mensagens acionáveis no dialeto suportado e gate explícito nos não suportados.
- Incremento desta sessão: `RETURNING` agora valida parênteses desbalanceados com mensagem acionável e mantém fronteira por `;` em projeções complexas, com cobertura adicional para gate de dialeto não suportado.
- Incremento desta sessão: cobertura de `RETURNING` com parênteses desbalanceados foi ampliada em DML (`INSERT/UPDATE/DELETE`) para reforçar erro acionável no Npgsql e gate explícito de dialeto em MySQL/SQL Server.
- Incremento desta sessão: `ON CONFLICT (...)` recebeu hardening de lista de alvo (vazio, vírgula inicial e vírgula final) com mensagens acionáveis no dialeto suportado e regressão explícita de gate para dialeto não suportado.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` recebeu validações acionáveis para lista de atribuições malformada (vazia, vírgula inicial/final e atribuição sem expressão).
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` passou a validar ausência de vírgula entre atribuições e a respeitar `;` como fronteira de statement após a lista.
- Incremento desta sessão: `ON CONFLICT` ganhou validações acionáveis para ramo `DO` ausente/inválido e para `DO UPDATE` sem `SET`, com regressão de gate em dialeto não suportado.
- Incremento desta sessão: `ON CONFLICT DO NOTHING` agora rejeita cláusulas adicionais indevidas antes de `RETURNING` com mensagem acionável no Npgsql e regressão de gate no SQL Server.
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO NOTHING` foi expandida para variantes com `WHERE` e `FROM`, mantendo diagnóstico acionável no Npgsql e gate no SQL Server.
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO NOTHING` foi ampliada também para variantes com `USING` e `SET`, mantendo diagnóstico acionável no Npgsql e gate no SQL Server.
- Incremento desta sessão: regressão positiva adicionada para `ON CONFLICT DO NOTHING RETURNING`, garantindo que o hardening de cláusulas indevidas não bloqueie o caminho válido no Npgsql.
- Incremento desta sessão: regressão positiva adicionada para `ON CONFLICT DO UPDATE SET ... RETURNING`, garantindo que o caminho válido continue aceito no Npgsql após os hardenings recentes.
- Incremento desta sessão: cobertura de gate adicionada no SQL Server para `ON CONFLICT DO NOTHING RETURNING`, garantindo bloqueio explícito da feature PostgreSQL em dialeto não suportado.
- Incremento desta sessão: cobertura de regressão adicionada para `ON CONFLICT DO NOTHING RETURNING` com expressão malformada, garantindo erro acionável de `RETURNING` no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT DO NOTHING/DO UPDATE ... RETURNING` foi estendida para parênteses desbalanceados (`RETURNING (id`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT DO NOTHING RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de regressão adicionada para `ON CONFLICT DO UPDATE ... RETURNING` com expressão malformada, garantindo erro acionável de `RETURNING` no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT DO UPDATE ... RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura composta de `ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING` foi reforçada no Npgsql (incluindo materialização de assignment/RETURNING) e no gate do SQL Server.
- Incremento desta sessão: cobertura de guidance no MySQL foi estendida para `ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING`, preservando precedência estável de mensagem de dialeto.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING` foi estendida para expressão malformada em `RETURNING`, mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING/DO UPDATE WHERE + RETURNING` e `ON CONFLICT DO UPDATE WHERE + RETURNING` foi estendida para parênteses desbalanceados (`RETURNING (id`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO UPDATE WHERE` foi estendida para variante sem `RETURNING`, mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi adicionada, mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi expandida para variantes com `RETURNING` (válida e expressão malformada), mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING + RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para continuação inesperada (`EXTRA`), mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para cláusula adicional indevida `FROM`, mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para cláusula adicional indevida `USING`, mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para cláusula adicional indevida `SET`, mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para cláusula adicional indevida `UPDATE`, mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT target WHERE + DO NOTHING` foi estendida para cláusula adicional indevida `WHERE`, mantendo erro acionável no Npgsql (com token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de regressão adicionada no MySQL para `ON CONFLICT DO NOTHING/DO UPDATE ... RETURNING` (incluindo expressão malformada), garantindo guidance de dialeto (`ON DUPLICATE KEY UPDATE`) mesmo quando a consulta mistura sintaxe PostgreSQL.
- Incremento desta sessão: cobertura composta de `ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE + RETURNING` foi adicionada, mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT + target WHERE + DO UPDATE WHERE + RETURNING` foi estendida para expressão malformada em `RETURNING`, mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT + target WHERE + DO NOTHING/DO UPDATE WHERE + RETURNING` também foi estendida para parênteses desbalanceados (`RETURNING (id`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT + target WHERE + DO UPDATE WHERE + RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT + DO UPDATE + WHERE` foi estendida para variante sem `RETURNING`, mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING` foi adicionada (incluindo expressão malformada), mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING/DO UPDATE ... RETURNING` foi estendida para parênteses desbalanceados (`RETURNING (id`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING` foi adicionada (com e sem `RETURNING`, incluindo expressão malformada em `RETURNING`), mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING` foi estendida para cláusula adicional indevida (`WHERE`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING` foi estendida para token de continuação inesperado (`EXTRA`), mantendo erro acionável no Npgsql (incluindo token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO NOTHING` foi ampliada para cláusulas adicionais indevidas `FROM`/`USING`/`SET`/`UPDATE`, mantendo erro acionável no Npgsql (incluindo token encontrado) e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING` foi adicionada (incluindo expressão malformada), mantendo caminho válido no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE` foi ampliada para cláusulas indevidas de table-source (`FROM`/`USING`), com erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi estendida também para variantes `SET FROM/USING` (sem atribuições), preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE` foi estendida para variante sem `SET`, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi ampliada para variante sem atribuições, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi estendida para lista de atribuições malformada com vírgula inicial/final, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi ampliada para ausência de separador por vírgula entre atribuições, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi ampliada para `SET` repetido e atribuição sem `=`, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE SET` foi estendida para expressão de atribuição malformada, preservando erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE WHERE` foi ampliada para predicado vazio/malformado (`WHERE;` / `WHERE id = RETURNING ...`), com erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de guidance no MySQL para `ON CONFLICT ON CONSTRAINT DO UPDATE WHERE` foi expandida também para `WHERE RETURNING ...` (sem predicado) e `WHERE id = RETURNING ...` (predicado malformado), garantindo precedência estável de mensagem de dialeto.
- Incremento desta sessão: cobertura de guidance no MySQL para `ON CONFLICT DO UPDATE WHERE` (sem `ON CONSTRAINT`) foi ampliada para `WHERE;`, `WHERE RETURNING ...` e `WHERE id = RETURNING ...`, preservando precedência estável de mensagem de dialeto.
- Incremento desta sessão: cobertura de `ON CONFLICT DO UPDATE WHERE ... RETURNING` (sem `ON CONSTRAINT`) foi estendida para expressão malformada em `RETURNING`, mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT DO UPDATE WHERE ... RETURNING` (sem `ON CONSTRAINT`) foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE WHERE ... RETURNING` foi estendida para lista vazia em `RETURNING` (`RETURNING;`), mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de guidance no MySQL para `ON CONFLICT DO UPDATE WHERE;` (sem `ON CONSTRAINT`) foi estendida também para variante sem `RETURNING`, preservando precedência estável de mensagem de dialeto.
- Incremento desta sessão: cobertura de `ON CONFLICT ON CONSTRAINT DO UPDATE WHERE;` foi estendida também para variante sem `RETURNING`, mantendo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO NOTHING` foi estendida para variante com `UPDATE` indevido após `DO NOTHING`, mantendo erro acionável no Npgsql e gate no SQL Server.
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO NOTHING` foi estendida para token de continuação inesperado (ex.: `EXTRA`), mantendo erro acionável no Npgsql e gate no SQL Server.
- Incremento desta sessão: mensagem de erro de `ON CONFLICT DO NOTHING` com continuação indevida passou a incluir o token encontrado para diagnóstico mais direto (ex.: `found 'EXTRA'`).
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO NOTHING` foi reforçada para verificar o token concreto encontrado também em variantes com cláusula (`FROM`), preservando diagnóstico acionável no Npgsql.
- Incremento desta sessão: `ON CONFLICT` passou a validar `WHERE` vazio no alvo e em `DO UPDATE`, com mensagens acionáveis em dialeto suportado e regressão de gate em não suportados.
- Incremento desta sessão: `ON CONFLICT ON CONSTRAINT` passou a validar ausência do nome da constraint com mensagem acionável e cobertura de gate para dialeto não suportado.
- Incremento desta sessão: cobertura de regressão foi ampliada para `ON CONFLICT ON CONSTRAINT` sem ramo `DO` e com continuação inválida após `DO`, garantindo erro acionável no Npgsql e guidance/gate explícitos em MySQL/SQL Server.
- Incremento desta sessão: `INSERT` passou a validar tokens inesperados após o statement (com tolerância a `;` final), evitando parse parcial silencioso em SQL malformado.
- Incremento desta sessão: `UPDATE` e `DELETE` também passaram a validar tokens inesperados após o statement (com tolerância a `;` final), alinhando boundary check de DML.
- Incremento desta sessão: `UPDATE` e `DELETE` agora rejeitam `WHERE` vazio com mensagens acionáveis (`... WHERE requires a predicate.`).
- Incremento desta sessão: cláusulas `WHERE` de `UPDATE`/`DELETE` e de `ON CONFLICT` agora normalizam `;` terminal antes da validação, rejeitando explicitamente casos como `WHERE;` com mensagem acionável de predicado ausente.
- Incremento desta sessão: cobertura de parser foi estendida para casos `ON CONFLICT ... WHERE;` e `ON CONFLICT DO UPDATE ... WHERE;`, garantindo erro acionável no dialeto suportado e preservando gate `NotSupported` no SQL Server.
- Incremento desta sessão: cobertura de parser foi expandida para `ON CONFLICT DO UPDATE ... WHERE;` sem `RETURNING`, garantindo erro acionável no Npgsql e gate de dialeto preservado no SQL Server.
- Incremento desta sessão: `ON CONFLICT target WHERE` e `ON CONFLICT DO UPDATE WHERE` agora validam também predicado malformado (não apenas vazio), com erro acionável no Npgsql e gate de dialeto preservado no SQL Server.
- Incremento desta sessão: cobertura de gate no SQL Server para `ON CONFLICT DO UPDATE WHERE` foi estendida também para `WHERE RETURNING ...` (sem predicado), preservando bloqueio consistente da feature PostgreSQL.
- Incremento desta sessão: cobertura de guidance no MySQL foi estendida para `ON CONFLICT target WHERE` vazio/malformado (`WHERE DO ...`, `WHERE;`, `WHERE id = DO ...`), preservando precedência estável de mensagem de dialeto.
- Incremento desta sessão: alvo `ON CONFLICT (...)` agora valida também expressão malformada com mensagem acionável (`ON CONFLICT target expression is invalid.`), com gate preservado no SQL Server.
- Incremento desta sessão: hardening defensivo passou a normalizar exceções inesperadas como erro acionável em `ON CONFLICT target/WHERE`, listas de atribuição DML (`UPDATE SET`/`ON CONFLICT DO UPDATE SET`/`ON DUPLICATE KEY UPDATE`) e `RETURNING`, evitando vazamento de exceções internas sem alterar o contrato de gate.
- Incremento desta sessão: cobertura de regressão foi estendida para o ramo `ON CONFLICT ON CONSTRAINT ... WHERE` com predicado malformado, reforçando mensagem acionável no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de regressão foi estendida para `ON CONFLICT ON CONSTRAINT ... WHERE` sem predicado (`WHERE DO ...`), reforçando erro acionável no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de regressão foi estendida para `ON CONFLICT ON CONSTRAINT ... WHERE;` (apenas `;`), reforçando erro acionável no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de guidance no MySQL foi estendida para `ON CONFLICT ON CONSTRAINT target WHERE` vazio/malformado (`WHERE DO ...`, `WHERE;`, `WHERE id = DO ...`), preservando precedência estável de mensagem de dialeto.
- Incremento desta sessão: cobertura do ramo `ON CONFLICT ON CONSTRAINT ... WHERE` foi reforçada também para variante que continua com `DO UPDATE SET`, garantindo validação antecipada do predicado no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: `UPDATE/DELETE WHERE` agora rejeitam predicado malformado (ex.: parêntese não fechado) com mensagem acionável (`... WHERE predicate is invalid.`) e removeram fallback silencioso de parsing, evitando aceitação de SQL inválido.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` agora rejeita expressão de atribuição malformada com mensagem acionável por coluna (`assignment for '<col>' has an invalid expression.`), com gate preservado no SQL Server.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` agora rejeita explicitamente cláusulas de table-source (`FROM`/`USING`) após as atribuições com mensagem acionável no Npgsql e regressão de gate no SQL Server.
- Incremento desta sessão: cobertura de regressão de `ON CONFLICT DO UPDATE` foi expandida para variante com `USING`, mantendo mensagem acionável no Npgsql e gate de dialeto no SQL Server.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` seguido diretamente por `FROM` (sem atribuições) agora também falha com mensagem acionável específica no Npgsql, com regressão de gate no SQL Server.
- Incremento desta sessão: cobertura de regressão do caso `ON CONFLICT DO UPDATE SET` sem atribuições foi estendida também para variante com `USING`, mantendo diagnóstico acionável no Npgsql e gate no SQL Server.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` passou a rejeitar também `SET` redundante (`... SET SET ...`) com mensagem acionável no Npgsql e regressão de gate no SQL Server.
- Incremento desta sessão: atribuições sem `=` em `ON CONFLICT DO UPDATE SET` e `UPDATE SET` agora geram mensagem acionável específica por coluna (`requires '=' between column and expression.`), com regressões no Npgsql/SQLServer/MySQL.
- Incremento desta sessão: `UPDATE SET` passou a rejeitar também `SET` redundante (`... SET SET ...`) com mensagem acionável, com regressões no Npgsql/SQLServer/MySQL.
- Incremento desta sessão: `UPDATE SET` também passou a rejeitar atribuições sem vírgula separadora e expressão malformada com mensagens acionáveis (`must separate assignments with commas` / `assignment for '<col>' has an invalid expression.`).
- Incremento desta sessão: caminhos DML de AST (`OnDupAssignsParsed`, `SetParsed` e `OnConflictUpdateWhereExpr`) removeram fallback silencioso de `TryParse...` e passaram a reutilizar parsing validado, garantindo materialização consistente de expressões em cenários válidos.
- Incremento desta sessão: parsing de `ON CONFLICT DO UPDATE WHERE` passou a materializar `UpdateWhereExpr` diretamente no contrato intermediário de UPSERT, evitando reparse duplicado na montagem final da AST.
- Incremento desta sessão: `RETURNING` agora rejeita expressão malformada com mensagem acionável (`RETURNING expression is invalid.`), com regressão no Npgsql e gate preservado no SQL Server.
- Incremento desta sessão: `ON DUPLICATE KEY UPDATE` passou a validar lista de atribuições com mensagens acionáveis (lista vazia, vírgula inicial/final, falta de separador por vírgula e expressão malformada), com regressão no MySQL e gate preservado no SQL Server.
- Incremento desta sessão: cobertura de gate no SQL Server foi ampliada para variantes malformadas de `ON DUPLICATE KEY UPDATE` (lista vazia e vírgula inicial), garantindo bloqueio consistente da sintaxe MySQL.
- Incremento desta sessão: gate de `ON CONFLICT` e `ON DUPLICATE KEY UPDATE` no SQL Server foi endurecido para contrato explícito de `NotSupportedException` (inclusive variantes malformadas), removendo aceitação ambígua de `InvalidOperationException` nos testes de regressão.
- Incremento desta sessão: `ON DUPLICATE KEY UPDATE` agora rejeita explicitamente cláusula `WHERE` e cláusulas de table-source (`FROM`/`USING`) com mensagens acionáveis no MySQL, com regressões de gate correspondentes no SQL Server e guidance preservado no Npgsql para sintaxe MySQL fora do dialeto.
- Incremento desta sessão: cobertura de regressão de `ON DUPLICATE KEY UPDATE` foi expandida para variante com `USING` no MySQL (erro acionável), SQL Server (gate) e Npgsql (guidance).
- Incremento desta sessão: `ON DUPLICATE KEY UPDATE` sem atribuições e seguido por `WHERE` agora falha com mensagem acionável específica de cláusula inválida no MySQL, com regressões de gate/guidance correspondentes em SQL Server e Npgsql.
- Incremento desta sessão: cobertura do caso `ON DUPLICATE KEY UPDATE` sem atribuições foi ampliada para variantes com `FROM` e `USING`, mantendo diagnóstico acionável no MySQL e cobertura de gate/guidance em SQL Server/Npgsql.
- Incremento desta sessão: `ON DUPLICATE KEY UPDATE` passou a rejeitar `SET` redundante (`... UPDATE SET ...`) com mensagem acionável no MySQL, com regressões de gate/guidance correspondentes em SQL Server e Npgsql.
- Incremento desta sessão: atribuições sem `=` em `ON DUPLICATE KEY UPDATE` agora geram mensagem acionável específica por coluna (`requires '=' between column and expression.`), com regressões no MySQL e cobertura de gate/guidance em SQL Server/Npgsql.
- Incremento desta sessão: regressão de parser adicionada para garantir guidance acionável ao Npgsql quando receber sintaxe MySQL `ON DUPLICATE KEY UPDATE` (direcionando para `ON CONFLICT`).
- Incremento desta sessão: cobertura de regressão foi estendida para `ON DUPLICATE KEY UPDATE ... RETURNING` (incluindo expressão malformada), garantindo gate explícito por dialeto no MySQL/SQL Server e guidance preservado no Npgsql.
- Incremento desta sessão: cobertura de `ON DUPLICATE KEY UPDATE ... RETURNING` foi ampliada também para lista vazia (`RETURNING;`) e parênteses desbalanceados (`RETURNING (id`), mantendo gate explícito por dialeto no MySQL/SQL Server e guidance preservado no Npgsql.
- Incremento desta sessão: cobertura de `ON DUPLICATE KEY UPDATE ... RETURNING` foi ampliada também para vírgula inicial/final na projeção (`RETURNING, id` / `RETURNING id,`), mantendo gate explícito por dialeto no MySQL/SQL Server e guidance preservado no Npgsql.
- Incremento desta sessão: cobertura de regressão foi ampliada para `ON DUPLICATE KEY UPDATE` sem atribuições e seguido por `RETURNING`, garantindo precedência estável de diagnóstico (MySQL acionável, SQL Server gate e Npgsql guidance).
- Incremento desta sessão: cobertura de precedência para `ON DUPLICATE KEY UPDATE` sem atribuições + `RETURNING` foi ampliada para `RETURNING;` e `RETURNING (id`, mantendo diagnóstico estável (MySQL acionável, SQL Server gate e Npgsql guidance).
- Incremento desta sessão: cobertura de gate de `RETURNING` foi estendida no MySQL para `INSERT/UPDATE/DELETE` (incluindo variantes com expressão malformada), garantindo bloqueio consistente da sintaxe PostgreSQL fora do dialeto suportado.
- Incremento desta sessão: cobertura de `INSERT/UPDATE/DELETE ... RETURNING` foi estendida para lista vazia (`RETURNING;`), com erro acionável no Npgsql e gate explícito de dialeto em MySQL/SQL Server.
- Incremento desta sessão: cobertura de `RETURNING` com vírgula inicial/final foi expandida em `INSERT/UPDATE/DELETE`, garantindo erro acionável no Npgsql e gate explícito de dialeto em MySQL/SQL Server.
- Incremento desta sessão: `INSERT VALUES` agora valida também expressão escalar malformada dentro da tupla com mensagem acionável por linha/posição (`row <n> expression <m> is invalid`), reduzindo parse parcial silencioso.
- Incremento desta sessão: cobertura de regressão de `INSERT VALUES` foi ampliada para falha em linhas posteriores (multi-row), preservando diagnóstico de linha/posição no erro acionável.
- Incremento desta sessão: `UPDATE SET` ganhou boundary check para `RETURNING` sem `WHERE` e validações acionáveis de lista de atribuições (vírgula final/falta de separador), evitando captura indevida de `RETURNING` como expressão.
- Incremento desta sessão: `INSERT VALUES` ganhou validações acionáveis de lista de tuplas (linha vazia, vírgula inicial/final e separação obrigatória por vírgula), reduzindo parse parcial em sintaxe malformada.
- Incremento desta sessão: `INSERT (colunas) VALUES (...)` passou a validar cardinalidade entre colunas alvo e expressões por linha, com mensagem acionável por linha divergente.
- Incremento desta sessão: `INSERT VALUES` também passou a validar cardinalidade consistente entre múltiplas linhas (row arity), mesmo sem lista explícita de colunas.
- Incremento desta sessão: `INSERT VALUES` passou a rejeitar expressão vazia dentro da tupla (ex.: `(1,,2)` e `(1,)`) com mensagem acionável.
- Incremento desta sessão: `INSERT (col1, ...)` passou a validar lista de colunas malformada (vazia, vírgula inicial/final e separação obrigatória por vírgula) com mensagens acionáveis.
- Incremento desta sessão: `INSERT VALUES` passou a validar fechamento de parênteses na tupla da linha, com erro acionável para tupla não encerrada.
- Incremento desta sessão: lista de colunas em `INSERT` ganhou cobertura de vírgula inicial e fechamento ausente antes de `;`, com mensagens acionáveis consistentes.
- Incremento desta sessão: `INSERT VALUES` passou a detectar tuplas consecutivas sem vírgula separadora (`VALUES (1) (2)`) com mensagem acionável específica.
- Incremento desta sessão: alvo `ON CONFLICT (...)` interrompido por `;` passou a falhar com mensagem acionável de fechamento incorreto da lista.
- Incremento desta sessão: mensagens de erro de cláusulas inválidas em `ON CONFLICT DO UPDATE` e `ON DUPLICATE KEY UPDATE` passaram a incluir o token encontrado (`found '<token>'`), com regressão explícita em Npgsql/MySQL para tornar o diagnóstico mais direto.
- Incremento desta sessão: regressões de `ON CONFLICT DO NOTHING` no Npgsql foram endurecidas para validar explicitamente o token encontrado (`found '<token>'`) em continuações indevidas (`FROM`/`USING`/`SET`/`UPDATE`/`WHERE`/`EXTRA`), reduzindo risco de regressão silenciosa no diagnóstico.
- Incremento desta sessão: diagnósticos de `ON CONFLICT` foram refinados para incluir token encontrado também em `DO` ausente/inválido e `DO UPDATE` sem `SET` (incluindo `<end-of-statement>`), com regressões Npgsql explícitas para esses caminhos.
- Incremento desta sessão: `ON CONFLICT ON CONSTRAINT` sem nome da constraint passou a incluir token encontrado no erro (ex.: `DO` ou `<end-of-statement>`), com regressões Npgsql para ambos os cenários.
- Incremento desta sessão: cobertura de gate em dialetos não suportados foi estendida para `ON CONFLICT ON CONSTRAINT` sem nome da constraint (incluindo variantes no fim de statement), preservando precedência de erro de dialeto em MySQL/SQL Server.
- Incremento desta sessão: diagnóstico do alvo `ON CONFLICT (...)` foi refinado para incluir token encontrado em lista vazia, vírgula inicial/final e fechamento ausente, com regressões Npgsql explícitas desses casos.
- Incremento desta sessão: diagnósticos de `RETURNING` em DML foram refinados para incluir token encontrado em lista vazia, vírgula inicial/final e fim de statement (`<end-of-statement>`), com regressões Npgsql explícitas em `INSERT/UPDATE/DELETE` e cenários com `ON CONFLICT`.
- Incremento desta sessão: `RETURNING AS <alias>` sem expressão passou a gerar diagnóstico acionável com token encontrado (`found 'AS'`), com regressão dedicada no Npgsql para evitar regressão silenciosa desse caminho residual.
- Incremento desta sessão: dialetos sem suporte a `RETURNING` (MySQL/SQL Server) ganharam regressão explícita para `RETURNING AS <alias>` sem expressão, preservando precedência de `NotSupportedException` do gate de dialeto.
- Incremento desta sessão: cobertura de `RETURNING AS <alias>` sem expressão foi ampliada para as três mutações DML (`INSERT/UPDATE/DELETE`) no Npgsql e nos gates de MySQL/SQL Server, reduzindo risco de regressão por tipo de comando.
- Incremento desta sessão: `ON CONFLICT DO UPDATE SET` sem atribuições passou a incluir token encontrado no diagnóstico (`found '<token>'`), com regressões Npgsql para fim de statement e para `RETURNING` imediatamente após `SET` (com e sem `ON CONSTRAINT`).
- Incremento desta sessão: diagnósticos de lista de atribuições em `ON CONFLICT DO UPDATE SET` foram refinados para incluir token encontrado em vírgula inicial/final e `SET` repetido, com regressões Npgsql explícitas para os cenários com e sem `ON CONSTRAINT`.
- Incremento desta sessão: diagnósticos de lista de atribuições em `ON DUPLICATE KEY UPDATE` também foram refinados para incluir token encontrado em vírgula inicial/final e `SET` indevido, com regressões MySQL explícitas dos três cenários.
- Incremento desta sessão: diagnósticos de lista de atribuições em `UPDATE SET` foram refinados para incluir token encontrado em vírgula inicial/final e `SET` repetido, com regressões de parser no Npgsql/MySQL/SQL Server.
- Incremento desta sessão: `ON DUPLICATE KEY UPDATE` sem atribuições também passou a incluir token encontrado no diagnóstico (`found '<token>'`), com regressões MySQL para fim de statement e para casos iniciados por `RETURNING`.
- Incremento desta sessão: `UPDATE SET` sem atribuições passou a incluir token encontrado no diagnóstico (`found '<token>'`), com regressões de parser no Npgsql/MySQL/SQL Server para caminhos iniciados por `RETURNING`, `WHERE` e `;`.
- Incremento desta sessão: cobertura de regressão em MySQL para `ON DUPLICATE KEY UPDATE;` (sem atribuições + `;`) foi adicionada, validando diagnóstico com `found ';'`.
- Incremento desta sessão: `UPDATE/DELETE WHERE` sem predicado passaram a incluir token encontrado no diagnóstico (`found '<token>'`) para `EOF`/`;` em Npgsql/MySQL/SQL Server e para `WHERE RETURNING ...` no Npgsql.
- Incremento desta sessão: `ON CONFLICT target WHERE` e `ON CONFLICT DO UPDATE WHERE` sem predicado passaram a incluir token encontrado no diagnóstico (`found '<token>'`), com regressões Npgsql para caminhos com `DO`, `RETURNING` e `;`.
- Preservação da experiência de uso próxima ao fluxo SQL tradicional.

#### 1.2.3 Regras por dialeto e versão

- Implementação estimada: **76%**.
- Ativa/desativa construções sintáticas por provedor e versão.
- Trata incompatibilidades históricas entre bancos diferentes.
- Direciona comportamento esperado em testes de compatibilidade.
- Checklist de known gaps indica cobertura concluída para MERGE por dialeto, WITH RECURSIVE e normalização de paginação/quoting.

#### 1.2.4 Governança de evolução do parser

- Implementação estimada: **94%**.
- Backlog guiado por gaps observados em testes reais.
- Track global de normalização Parser/AST consolidado em ~90%, com foco atual em refinos finais por dialeto.
- Priorização por impacto em frameworks de acesso a dados.
- Expansão incremental para reduzir regressões.
- Backlog operacional segue cadência priorizada P0→P14 para reduzir dispersão de implementação entre parser/executor/docs.

#### 1.2.5 Funções SQL agregadoras e de composição de texto

- Implementação estimada: **100%**.
- Parser e AST agora suportam `WITHIN GROUP (ORDER BY ...)` para agregações textuais com gate explícito por dialeto/função.
- Cobertura atual inclui parsing de ordenação simples e composta, validação de cláusula malformada (`WITHIN GROUP requires ORDER BY`) e cenários negativos por função não nativa no dialeto.
- Hardening recente ampliou a validação de `ORDER BY` malformado dentro de `WITHIN GROUP` (lista vazia, vírgula inicial, vírgula final e ausência de vírgula entre expressões), com mensagens acionáveis por cenário.
- Runtime aplica a ordenação de `WITHIN GROUP` antes da agregação, incluindo combinações com `DISTINCT` e separador customizado.
- Trilha ordered-set para agregações textuais concluída para dialetos suportados (SQL Server, Npgsql, Oracle e DB2), com bloqueio explícito e testado para MySQL/SQLite.

#### 1.2.6 Funções de data/hora cross-dialect

- Implementação estimada: **93%**.
- Consolidar no `dialect` o catálogo de funções temporais sem argumento (data, hora e data/hora).
- Garantir suporte de avaliação tanto para função com parênteses quanto para tokens sem parênteses em `SELECT`, `WHERE`, `HAVING` e expressões de `INSERT/UPSERT`.
- Cobertura Dapper cross-provider adicionada para funções temporais sem argumento em projeção/filtro `WHERE`, em expressões de `INSERT VALUES` e em `UPDATE ... SET` (MySQL/SQL Server/Oracle/Npgsql/SQLite/DB2).
- Cobertura Dapper cross-provider expandida para `HAVING` e `ORDER BY` com função temporal sem argumento em consultas agrupadas (MySQL/SQL Server/Oracle/Npgsql/SQLite/DB2).
- Cobertura Dapper expandida para funções temporais adicionais por dialeto em `WHERE`, `HAVING` e `ORDER BY` (ex.: `CURRENT_DATE`/`CURRENT_TIME` em MySQL/Npgsql/SQLite/DB2; `GETDATE`/`SYSDATETIME` em SQL Server; `CURRENT_DATE`/`SYSTIMESTAMP` em Oracle).
- Cenário negativo por dialeto adicionado para função temporal de outro dialeto (ex.: `GETDATE()`/`NOW()`) com validação de erro claro por provider.
- Catálogo temporal por dialeto agora distingue tokens sem parênteses e funções invocáveis com parênteses, com cobertura negativa para chamadas inválidas de token (`CURRENT_TIMESTAMP()`) em MySQL/Npgsql/SQL Server/SQLite/Oracle/DB2.
- Cenário inverso (função call-only sem parênteses) validado com erro claro em SQL Server (`GETDATE`) e em MySQL/Npgsql (`NOW`).
- Cobertura positiva adicional para `NOW()` em consulta agrupada com `HAVING`/`ORDER BY` no MySQL, reforçando semântica call-style no dialeto.
- Cobertura positiva call-style expandida para `NOW()` no Npgsql (`WHERE` e `HAVING`/`ORDER BY`) e para `GETDATE()`/`SYSDATETIME()` em consulta agrupada no SQL Server.
- Oracle ganhou cobertura explícita de `SYSDATE` e `SYSTIMESTAMP` em `HAVING` e `ORDER BY`, além de cenários negativos úteis para uso inválido com parênteses (`SYSDATE()`/`SYSTIMESTAMP()`).
- DB2, SQLite, MySQL e Npgsql reforçaram contrato token-only para temporais ANSI com cenários negativos adicionais (`CURRENT_DATE()` em DB2/SQLite/MySQL/Npgsql e `CURRENT_TIME()` em DB2/SQLite).
- Novos testes de consistência por contexto para `CURRENT_TIMESTAMP` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET) em DB2 e SQLite, reduzindo risco de regressão cross-contexto.
- DB2 e SQLite também passaram a validar consistência por contexto para `CURRENT_DATE` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), ampliando cobertura token-style além de `CURRENT_TIMESTAMP`.
- DB2 e SQLite agora cobrem também consistência por contexto para `CURRENT_TIME` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), completando a tríade temporal ANSI (`CURRENT_DATE`/`CURRENT_TIME`/`CURRENT_TIMESTAMP`).
- MySQL e Npgsql agora também possuem testes de consistência por contexto para `NOW()` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), alinhando cobertura call-style com DB2/SQLite no cenário token-style.
- MySQL e Npgsql também passaram a validar consistência por contexto para `CURRENT_DATE` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), equilibrando cobertura entre contratos token-style e call-style nesses provedores.
- MySQL e Npgsql agora cobrem também consistência por contexto para `CURRENT_TIME` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), fechando a tríade temporal ANSI junto de `CURRENT_DATE` e `CURRENT_TIMESTAMP`.
- MySQL e Npgsql passaram a validar explicitamente consistência por contexto também para `CURRENT_TIMESTAMP` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), completando matriz de consistência para temporais ANSI nesses provedores.
- SQL Server ganhou teste de consistência por contexto para `GETDATE()` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), reduzindo gap de semântica call-style em cenários reais de uso.
- SQL Server também ganhou teste de consistência por contexto para `SYSDATETIME()` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), cobrindo a segunda função call-style principal do dialeto.
- Oracle passou a ter teste de consistência por contexto para `SYSDATE` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), consolidando cobertura token-style em fluxo fim a fim.
- Oracle também passou a ter teste de consistência por contexto para `SYSTIMESTAMP` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), fechando paridade de consistência entre os principais temporais token-style do dialeto.
- Oracle agora inclui consistência por contexto para `CURRENT_DATE` e cenário negativo explícito para `CURRENT_DATE()` (token chamado como função), fortalecendo o contrato token-only no dialeto.
- Oracle passou a validar consistência por contexto também para `CURRENT_TIMESTAMP` (SELECT, WHERE, HAVING, ORDER BY, INSERT VALUES e UPDATE SET), fechando cobertura dos principais temporais token-style do dialeto.
- MySQL e Npgsql ganharam cenário negativo adicional para `CURRENT_TIME()` (token chamado como função), alinhando o contrato token-only com DB2/SQLite para a tríade ANSI.
- SQL Server ganhou cenário negativo adicional para função call-only usada sem parênteses em `SYSDATETIME`, reforçando simetria com a validação já existente de `GETDATE`.
- Cobrir equivalências por provedor (exemplos):
  - Oracle: `SYSDATE`, `SYSTIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`.
  - SQL Server: `GETDATE`, `SYSDATETIME`, `CURRENT_TIMESTAMP`.
  - MySQL/PostgreSQL/SQLite/DB2: `NOW`, `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP` (quando aplicável ao dialeto).
- Introduzir serviço compartilhado para avaliação temporal e reutilização no executor AST, estratégias de insert/update e helpers de valor.
- Incluir cobertura explícita para funções de agregação textual por dialeto.
- Priorizar equivalências entre funções para reduzir divergência em testes multi-provedor.
- Exemplos prioritários de backlog:
  - `LISTAGG` (comum em Oracle e cenários DB2 modernos).
  - `STRING_AGG` (comum em SQL Server e PostgreSQL).
  - `GROUP_CONCAT` (comum em MySQL e SQLite).
- Definir comportamento esperado para:
  - ordenação interna da agregação (`WITHIN GROUP`/`ORDER BY` equivalente),
  - separador customizado,
  - tratamento de `NULL`,
  - compatibilidade com `GROUP BY` e filtros.

### 1.3 Executor SQL

#### 1.3.1 Pipeline de execução

- Implementação estimada: **69%**.
- Fluxo macro: parse → validação → execução no estado em memória → materialização de resultado.
- Track global de alinhamento de runtime estimado em ~55%, com evolução incremental por contracts de dialeto.
- Recalibrado por evidências de código: executor AST, estratégias de mutação por dialeto e ampla suíte `*StrategyTests`/`*GapTests` por provider.
- Tratamento de execução orientado por semântica do dialeto escolhido.
- Retorno previsível para facilitar asserts em testes.

#### 1.3.2 Operações comuns suportadas

- Implementação estimada: **86%**.
- Fluxos DDL/DML de uso frequente em aplicações corporativas .NET.
- Cenários com múltiplos comandos por contexto de teste.
- Execução orientada a simulação funcional (não benchmark de banco real).
- Tracker de concorrência transacional aponta evolução atual em ~35%, com base compartilhada de testes em 100% e próximas fases focadas em isolamento/visibilidade/savepoint/stress.
- Known gaps concluídos reforçam UPDATE/DELETE com JOIN multi-tabela e evolução de JSON por provider com bloqueio padronizado quando não suportado.
- Roadmap operacional cobre SQL Core, composição de consulta, SQL avançado, DML avançado e paginação por versão.
- Plano executável P7–P14 aponta trilhas ativas para UPSERT/UPDATE/DELETE avançados (P7), paginação/ordenação (P8) e JSON por provider (P9).
- **Fidelidade de rowcount por dialeto (FOUND_ROWS / ROW_COUNT / ROWCOUNT / @@ROWCOUNT / CHANGES): implementação estimada em 100%.**
  - Estado atual: tracking por conexão consolidado e cobertura funcional para MySQL, SQL Server, PostgreSQL, Oracle, DB2 e SQLite.
  - Incrementos concluídos:
    - suporte de rowcount em batches multi-statement com controle transacional (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO`, `RELEASE`) no `ExecuteReader`;
    - cobertura de regressão por dialeto para cenários `BEGIN ...; SELECT <função-rowcount>` e `UPDATE ...; COMMIT; SELECT <função-rowcount>`;
    - alinhamento de leitura por variável/função equivalente (`FOUND_ROWS()`, `ROW_COUNT()`, `ROWCOUNT()`, `@@ROWCOUNT`, `CHANGES()`);
    - correção de batches iniciados por `CALL` para preservar execução de statements subsequentes (ex.: `CALL ...; SELECT <rowcount>`);
    - cobertura de regressão de `CALL` + função de rowcount expandida para todos os dialetos suportados;
    - cobertura explícita para `ROLLBACK TO SAVEPOINT` e `RELEASE SAVEPOINT` em batches com leitura posterior de rowcount equivalente (todos os dialetos suportados).
    - cobertura de precedência em batch misto (`SELECT` seguido de `DML`) validando que a função de rowcount reflete o último statement executado.
    - cobertura de cenários combinados `CALL + DML + COMMIT + função de rowcount` para validar reset após comando transacional final.
    - cobertura de precedência inversa em batch (`DML` seguido de `SELECT`) validando que a função de rowcount passa a refletir o último `SELECT`.
  - Próximos passos (manutenção contínua):
    - monitorar regressões em novos cenários de procedure quando houver suporte a corpo multi-statement;
    - manter suíte de rowcount por dialeto atualizada conforme expansão de parser/executor.

#### 1.3.3 Resultados e consistência

- Implementação estimada: **90%**.
- Entrega de resultados em formatos esperados por consumidores ADO.NET.
- Coerência entre operação executada e estado final da base simulada.
- Comportamento determinístico para repetição do mesmo script.
- Hardening recente reforçou previsibilidade de regressão com foco em mensagens de erro não suportado e consistência de diagnóstico.
- Checklist operacional confirma padronização de `SqlUnsupported.ForDialect(...)` no runtime para fluxos não suportados.
- Hardening recente também consolidou semântica ordered-set para agregações textuais com cobertura de ordenação `ASC/DESC`, ordenação composta, `DISTINCT + WITHIN GROUP` e `LISTAGG` sem separador explícito nos dialetos suportados.

#### 1.3.4 Particionamento de tabelas (avaliação)

- Implementação estimada: **8%**.
- **Recomendação:** sim, vale incluir partição de tabelas como feature incremental para cenários de teste com alto volume e consultas por faixa (ex.: data, tenant, shard lógico).
- **Ganho esperado:**
  - redução de custo em varreduras quando filtros batem na chave de partição (partition pruning);
  - cenários de retenção/arquivamento mais realistas (drop/truncate por partição);
  - maior fidelidade para workloads multi-tenant e time-series;
  - testes de regressão de plano/estratégia com comportamento mais próximo de bancos reais.
- **Escopo mínimo sugerido no mock:**
  - metadado de partição por tabela (`RANGE`/`LIST` simplificado);
  - roteamento de `INSERT` para partição-alvo;
  - pruning básico em `SELECT/UPDATE/DELETE` quando filtro contém chave de partição;
  - fallback explícito de não suportado para DDL avançado fora do subset.
- **Risco/observação:** manter subset pequeno para não aumentar complexidade do executor antes de fechar gaps críticos já priorizados.

### 1.4 API fluente

#### 1.4.1 Definição de schema por código

- Implementação estimada: **86%**.
- Criação declarativa/programática de estruturas.
- Reduz dependência de scripts SQL longos para setup inicial.
- Facilita reuso de cenários entre suítes.

#### 1.4.2 Seed de dados

- Implementação estimada: **84%**.
- Carga inicial de registros para cenários controlados.
- Apoia testes de leitura, paginação e filtros complexos.
- Permite criar massas pequenas e objetivas por caso de teste.

#### 1.4.3 Composição de cenários

- Implementação estimada: **82%**.
- Encadeamento de passos de inicialização.
- Uso de builders/factories de contexto de teste.
- Legibilidade maior para times de aplicação.

### 1.5 Diagnóstico e observabilidade da execução

#### 1.5.1 Plano de execução mock

- Implementação estimada: **42%**.
- Geração de plano sintético para análise de comportamento da query.
- Visibilidade de entradas da execução e custo estimado.
- Suporte a testes que verificam diagnóstico e não só resultado.

#### 1.5.2 Métricas de runtime

- Implementação estimada: **72%**.
- Métricas disponíveis: `EstimatedCost`, `InputTables`, `EstimatedRowsRead`, `ActualRows`, `SelectivityPct`, `RowsPerMs`, `ElapsedMs`.
- Recalibrado com base na presença efetiva das métricas e nos testes de plano/formatter existentes no código.
- Permite validar cenários de seletividade e custo relativo.
- Facilita comparação entre estratégias de consulta em testes.

#### 1.5.3 Histórico por conexão

- Implementação estimada: **85%**.
- `LastExecutionPlan`: referência ao último plano executado.
- `LastExecutionPlans`: trilha dos planos da sessão de conexão.
- Útil para auditoria de execução em cenários multi-etapa.

#### 1.5.4 Uso prático no backlog

- Implementação estimada: **70%**.
- Ajuda a mapear comandos mais custosos no ambiente de testes.
- Apoia priorização de melhorias no parser/executor.
- Oferece material para diagnósticos reprodutíveis em issues.

### 1.6 Riscos técnicos e mitigação no núcleo

#### 1.6.1 Risco: divergência entre mock e banco real

- Implementação estimada: **60%**.
- Mitigar com smoke tests cross-dialect para consultas críticas.
- Catalogar explicitamente as diferenças conhecidas em documentação de compatibilidade.

#### 1.6.2 Risco: regressão em evolução do parser

- Implementação estimada: **70%**.
- Exigir cenários de regressão para cada correção de sintaxe.
- Priorizar suíte incremental por dialeto para reduzir efeito colateral.

#### 1.6.3 Risco: falsa percepção de performance

- Implementação estimada: **35%**.
- Reforçar que métricas do mock são diagnósticas e relativas.
- Evitar decisões de tuning de produção baseadas apenas em execução em memória.

## 2) Integração ADO.NET e experiência de uso

### 2.0 Objetivos de integração

- Maximizar reaproveitamento do código de acesso já existente em aplicações .NET.
- Reduzir custo de adoção em times que usam `DbConnection`, `DbCommand` e Dapper.
- Diminuir esforço de manutenção de doubles artesanais em testes de repositório.

### 2.1 Mocks ADO.NET por provedor

#### 2.1.1 Conexão mock por banco

- Implementação estimada: **90%**.
- Implementações específicas para cada provedor suportado.
- Interface familiar para quem já usa `DbConnection`/`DbCommand`.
- Foco em reduzir atrito de migração de teste real → teste mock.

#### 2.1.2 Integração com fluxo de testes

- Implementação estimada: **85%**.
- Injeção de conexão mock em serviços, repositórios e UoW.
- Evita dependência de infraestrutura externa em testes rápidos.
- Facilita execução local e em pipeline compartilhado.

#### 2.1.3 Benefícios de arquitetura

- Implementação estimada: **70%**.
- Camada de acesso mais desacoplada de banco físico.
- Melhor separação entre teste de regra e teste de infraestrutura.
- Menor custo de manutenção de ambientes dedicados.

### 2.2 Compatibilidade com Dapper

#### 2.2.1 Fluxo amigável para micro-ORM

- Implementação estimada: **82%**.
- Execução de queries e comandos com padrão próximo do uso em produção.
- Reaproveitamento de código de acesso a dados em ambiente de teste.
- Menor necessidade de doubles manuais de repositório.
- Fluxo validado para `Execute`/`Query` parametrizados e procedures (`CommandType.StoredProcedure`) com parâmetros `Input/Output/InputOutput/ReturnValue`.
- P10/P14 reforçam cobertura de procedures, parâmetros OUT e cenários Dapper avançados (multi-mapping, QueryMultiple) para uso real de aplicação.

#### 2.2.2 Cenários prioritários

- Implementação estimada: **70%**.
- Testes de SQL embarcado em métodos de repositório.
- Validação de mapeamento simples e comportamento de filtros.
- Ensaios de regressão de query sem banco real.

### 2.3 Factory de provedor em runtime

#### 2.3.1 Seleção dinâmica por chave

- Implementação estimada: **90%**.
- Escolha de provedor por string/configuração (`mysql`, `sqlserver`, `sqlazure`/`azure-sql`, `oracle`, `postgresql`, `sqlite`, `db2`).
- Suporte a testes parametrizados por dialeto.
- Base para suíte cross-provider.

#### 2.3.2 Estratégias de uso

- Implementação estimada: **84%**.
- Executar o mesmo caso de teste em múltiplos bancos simulados.
- Identificar dependências acidentais de sintaxe específica.
- Planejar portabilidade de consultas.

### 2.4 Critérios de qualidade para integração

#### 2.4.1 Confiabilidade de API

- Implementação estimada: **88%**.
- Chamadas mais comuns devem manter semântica previsível para testes de aplicação.
- Mensagens de erro precisam apontar de forma clara comando, dialeto e contexto.
- Capabilities comuns entre providers cobrem `WHERE`, `GROUP BY/HAVING`, `CREATE VIEW`, `CREATE TEMP TABLE` e integração ORM, reduzindo diferenças de uso em testes.
- Contrato de mensagens para SQL não suportado foi padronizado e coberto por regressão em múltiplos providers.

#### 2.4.2 Legibilidade dos testes consumidores

- Implementação estimada: **83%**.
- Priorizar exemplos com setup curto e intenção explícita.
- Evitar camadas de abstração que escondam a query que está sendo validada.

---

## 3) Provedores SQL suportados

### 3.0 Direcionadores do backlog por provedor

- Cobrir primeiro os provedores com maior base instalada no produto consumidor.
- Tratar diferenças de sintaxe como requisito funcional, não detalhe cosmético.
- Manter rastreabilidade entre gap reportado, teste criado e item de roadmap.

### 3.1 MySQL (`DbSqlLikeMem.MySql`)

#### 3.1.1 Versões simuladas

- Implementação estimada: **100%**.
- 3, 4, 5, 8.

#### 3.1.2 Recursos relevantes

- Implementação estimada: **85%**.
- Parser/executor para DDL/DML comuns.
- Suporte a `INSERT ... ON DUPLICATE KEY UPDATE`.
- Cobertura de `GROUP_CONCAT` ampliada com regressão para `DISTINCT` e tratamento de `NULL` em agregação textual; pendente evoluir ordenação interna da agregação.
- P7 consolidado: UPSERT por família (`ON DUPLICATE`/`ON CONFLICT`/`MERGE subset`) e mutações avançadas com contracts por strategy tests.
- Funções-chave do banco: `GROUP_CONCAT`, `IFNULL`, `DATE_ADD` e `JSON_EXTRACT` (subset no mock).

#### 3.1.3 Aplicações típicas

- Implementação estimada: **90%**.
- Legados com SQL histórico do ecossistema MySQL.
- Validação de comportamento de upsert no fluxo de escrita.

### 3.2 SQL Server (`DbSqlLikeMem.SqlServer`)

#### 3.2.1 Versões simuladas

- Implementação estimada: **100%**.
- 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022.

#### 3.2.2 Recursos relevantes

- Implementação estimada: **88%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `STRING_AGG` ampliada para `DISTINCT`, tratamento de `NULL` e ordenação interna via `WITHIN GROUP`, incluindo cenários de erro malformado com diagnóstico acionável.
- P8 consolidado: paginação por versão (`OFFSET/FETCH`, `TOP`) com gates explícitos de dialeto.
- Funções-chave do banco: `STRING_AGG`, `ISNULL`, `DATEADD`, `JSON_VALUE`/`OPENJSON` (subset no mock).
- `DbSqlLikeMem.SqlAzure` compartilha a base do dialeto SQL Server no ciclo atual, com níveis de compatibilidade 100/110/120/130/140/150/160/170.

#### 3.2.3 Aplicações típicas

- Implementação estimada: **90%**.
- Sistemas .NET com forte dependência de SQL Server.
- Testes de compatibilidade evolutiva por geração da plataforma.

### 3.3 Oracle (`DbSqlLikeMem.Oracle`)

#### 3.3.1 Versões simuladas

- Implementação estimada: **100%**.
- 7, 8, 9, 10, 11, 12, 18, 19, 21, 23.

#### 3.3.2 Recursos relevantes

- Implementação estimada: **88%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `LISTAGG` ampliada com separador customizado, comportamento padrão sem delimitador quando omitido e ordenação interna via `WITHIN GROUP` (incluindo combinações com `DISTINCT`).
- P8 consolidado: suporte a `FETCH FIRST/NEXT` por versão e contratos de ordenação por dialeto.
- Funções-chave do banco: `LISTAGG`, `NVL`, `JSON_VALUE` (subset escalar) e operações de data por versão.

#### 3.3.3 Aplicações típicas

- Implementação estimada: **90%**.
- Ambientes com legado Oracle e migração gradual de versões.
- Validação de SQL de camada de integração sem depender do ambiente corporativo.

### 3.4 PostgreSQL / Npgsql (`DbSqlLikeMem.Npgsql`)

#### 3.4.1 Versões simuladas

- Implementação estimada: **100%**.
- 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17.

#### 3.4.2 Recursos relevantes

- Implementação estimada: **88%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `STRING_AGG` ampliada para agregação textual com `DISTINCT`, `NULL` e ordenação por grupo via `WITHIN GROUP`, com gate por função/dialeto e mensagens acionáveis em sintaxe malformada.
- P7/P10 consolidado: `RETURNING` sintático mínimo em caminhos suportados e fluxo de procedures no contrato Dapper.
- Funções-chave do banco: `STRING_AGG`, operadores JSON (`->`, `->>`, `#>`, `#>>`) e expressões de data por intervalo.

#### 3.4.3 Aplicações típicas

- Implementação estimada: **90%**.
- Projetos modernos com Npgsql em APIs/serviços.
- Ensaios de portabilidade SQL entre PostgreSQL e outros bancos.

### 3.5 SQLite (`DbSqlLikeMem.Sqlite`)

#### 3.5.1 Versões simuladas

- Implementação estimada: **100%**.
- 3.

#### 3.5.2 Recursos relevantes

- Implementação estimada: **84%**.
- `WITH`/CTE disponível.
- Operadores JSON `->` e `->>` disponíveis no parser do dialeto.
- Cobertura de `GROUP_CONCAT` ampliada com separador customizado, `DISTINCT` e tratamento de `NULL`; ordenação interna da agregação segue como próximo passo.
- P8 consolidado: `LIMIT/OFFSET` e ordenação com regras de compatibilidade por versão simulada.
- Funções-chave do banco: `GROUP_CONCAT`, `IFNULL`, funções de data (`date`, `datetime`, `strftime`) e `JSON_EXTRACT` (subset).

#### 3.5.3 Restrições relevantes

- Implementação estimada: **100%**.
- `ON DUPLICATE KEY UPDATE` não suportado (usa `ON CONFLICT`).
- Operador null-safe `<=>` não suportado.

#### 3.5.4 Aplicações típicas

- Implementação estimada: **90%**.
- Testes leves com dependência mínima de infraestrutura.
- Simulação de cenários embarcados/offline.

### 3.6 DB2 (`DbSqlLikeMem.Db2`)

#### 3.6.1 Versões simuladas

- Implementação estimada: **100%**.
- 8, 9, 10, 11.

#### 3.6.2 Recursos relevantes

- Implementação estimada: **87%**.
- `WITH`/CTE disponível.
- `MERGE` disponível (>= 9).
- `FETCH FIRST` suportado.
- Cobertura de `LISTAGG` ampliada com separador customizado, `DISTINCT`, tratamento de `NULL` e ordenação ordered-set via `WITHIN GROUP`, incluindo validações sintáticas malformadas.
- P9 consolidado: fallback explícito de não suportado para JSON avançado e cobertura de `FETCH FIRST` no dialeto DB2.
- Funções-chave do banco: `LISTAGG` (por versão), `COALESCE`, `TIMESTAMPADD` e `FETCH FIRST` no fluxo de paginação.

#### 3.6.3 Restrições relevantes

- Implementação estimada: **100%**.
- `LIMIT/OFFSET` não suportado no dialeto DB2.
- `ON DUPLICATE KEY UPDATE` não suportado.
- Operador null-safe `<=>` não suportado.
- Operadores JSON `->` e `->>` não suportados.

#### 3.6.4 Aplicações típicas

- Implementação estimada: **90%**.
- Cenários corporativos com DB2 legado.
- Testes de SQL portado de outros dialetos para DB2.

### 3.7 Estratégia multi-provedor

#### 3.7.1 Matriz de cobertura

- Implementação estimada: **95%**.
- Executar casos críticos em todos os provedores prioritários do produto.
- Definir perfil mínimo de compatibilidade por módulo.
- Execução matricial por provider já iniciada em CI (`provider-test-matrix.yml`), com publicação de artefatos de resultado por projeto e etapas dedicadas de smoke e agregação cross-dialect, com publicação de snapshot por perfil em artefatos de CI.
- Cobertura de regressão inclui suíte cross-dialeto com snapshots por perfil (smoke/aggregation), operacionalizada no script `scripts/run_cross_dialect_equivalence.sh`; atualização em lote suportada por `scripts/refresh_cross_dialect_snapshots.sh` e baseline documental semântico (`manual-placeholder`) para evitar snapshot desatualizado no repositório.
- Matriz consolidada de providers/versões e capacidades comuns agora está refletida diretamente neste índice como fonte principal de backlog.

#### 3.7.2 Priorização de gaps

- Implementação estimada: **70%**.
- Gaps que quebram fluxo de negócio entram no topo do backlog.
- Priorização prática usa ondas inspiradas no pipeline P0..P14 (baseline, core, composição, avançado, hardening).
- Diferenças cosméticas/documentais podem ficar em ondas posteriores.

### 3.8 Modelo de evolução por ondas

#### 3.8.1 Onda 1 (crítica)

- Implementação estimada: **78%**.
- Comandos que bloqueiam operações essenciais de CRUD e autenticação/autorização da aplicação.

#### 3.8.2 Onda 2 (alta)

- Implementação estimada: **78%**.
- Diferenças que impactam relatórios, filtros avançados e paginação em módulos centrais.
- Inclui execução do plano P11/P12 para confiabilidade transacional, concorrência e diagnóstico de erro com contexto.
- Status detalhado de transações concorrentes: fase de hardening base concluída (100%), governança em progresso (~10%) e cenários críticos (fases 2–5) priorizados para fechamento.

#### 3.8.3 Onda 3 (média/baixa)

- Implementação estimada: **76%**.
- Cobertura de sintaxes menos frequentes e melhorias de ergonomia para debug.
- Inclui trilhas P13/P14 para performance (hot paths/caching) e conformidade de ecossistema (.NET/ORM/tooling).
- Inclui avaliação de partição de tabelas em subset (metadado + pruning básico) após estabilização dos gaps críticos de parser/executor.

---

## 4) Recursos comportamentais adicionais

### 4.0 Objetivo dos recursos comportamentais

- Simular efeitos colaterais de banco que impactam regra de negócio além do resultado da query.
- Tornar explícito no teste quando um comportamento depende de trigger ou semântica de dialeto.

### 4.1 Triggers

#### 4.1.1 Tabelas não temporárias

- Implementação estimada: **88%**.
- Suporte a triggers em `TableMock`.
- Percentual revisado com base em validações por dialeto (`SupportsTriggers`) e suites dedicadas por provider.
- Eventos: before/after insert, update e delete.
- Permite simular regras reativas de domínio persistido.

#### 4.1.2 Tabelas temporárias

- Implementação estimada: **100%**.
- Triggers não executadas em tabelas temporárias (connection/global).
- Comportamento explícito para evitar ambiguidade em testes.

#### 4.1.3 Diretrizes de uso

- Implementação estimada: **72%**.
- Preferir assertions claras sobre efeitos da trigger.
- Isolar cenários de trigger dos cenários de query pura.

### 4.2 Compatibilidade por dialeto (governança de gaps)

#### 4.2.1 Matriz de compatibilidade SQL

- Implementação estimada: **94%**.
- Registro do que já está suportado por banco/versão.
- Visão de lacunas e riscos por área funcional.
- Matriz feature x dialeto já publicada e usada como referência de hardening/regressão.
- Matriz versionada (`vCurrent`/`vNext`) e rastreável para testes corresponde ao fechamento do checklist de documentação.

#### 4.2.2 Roadmaps de parser/executor

- Implementação estimada: **88%**.
- Planejamento incremental por marcos.
- Track global de regressão cross-dialect está em ~70%, com ampliação contínua da cobertura em matriz de smoke/regressão.
- Conexão entre backlog técnico e testes de regressão.
- Known gaps aponta 14/14 itens tratados em código/documentação, com validação contínua dependente da suíte local/CI.

#### 4.2.3 Critérios de aceitação

- Implementação estimada: **96%**.
- Cada novo recurso deve incluir cenário positivo e negativo.
- O modelo TDD-first já está amplamente adotado: Red → Green → Refactor → Harden → Document em cada fatia de feature.
- Deve existir evidência de não regressão em dialetos correlatos.
- Para concorrência transacional, o aceite inclui ausência de flaky, cobertura por versão (`MemberData*Version`) e preservação de suites de transaction reliability.
- Regressões de mensagens `NotSupportedException` no parser já estão cobertas para MySQL/SQL Server/Oracle/Npgsql/DB2/SQLite.
- Cada fatia de entrega deve apresentar critérios de aceite, validação e escopo explícito no padrão dos prompts de implementação.

### 4.3 Observabilidade de comportamento em testes

#### 4.3.1 Evidências mínimas por cenário

- Implementação estimada: **90%**.
- SQL de entrada utilizado no teste.
- Estado esperado antes/depois quando houver efeito de trigger.
- Registro do dialeto e versão simulada para facilitar reprodução.
- Incluir no hardening evidência de mensagem padronizada para não suportado e referência ao teste de regressão associado.
- CI deve publicar relatório por provider e resultado da smoke cross-dialeto como evidência mínima de fechamento.

---

## 5) Ferramentas de produtividade (extensões)

### 5.0 Objetivo de produtividade

- Reduzir tarefas repetitivas de scaffolding em times de aplicação e teste.
- Padronizar artefatos para diminuir divergências entre equipes e projetos.

### 5.1 Fluxos de geração de artefatos

#### 5.1.1 Geração de classes de teste

- Implementação estimada: **88%**.
- Fluxo principal para acelerar criação de testes automatizados.
- Apoia padronização da base de testes.

#### 5.1.2 Geração de classes de modelos

- Implementação estimada: **76%**.
- Geração de artefatos de aplicação além de testes.
- Útil para bootstrap inicial de camadas de domínio/dados.

#### 5.1.3 Geração de classes de repositório

- Implementação estimada: **74%**.
- Auxilia criação consistente de componentes de acesso a dados.
- Reduz repetição em soluções com múltiplos módulos.

#### 5.1.4 Ganhos operacionais

- Implementação estimada: **78%**.
- Menor tempo de setup de projeto.
- Maior consistência estrutural entre times e repositórios.

### 5.2 Templates e consistência

#### 5.2.1 Configuração de templates

- Implementação estimada: **82%**.
- Suporte a templates textuais com tokens:
  - `{{ClassName}}`
  - `{{ObjectName}}`
  - `{{Schema}}`
  - `{{ObjectType}}`
  - `{{DatabaseType}}`
  - `{{DatabaseName}}`
- Permite adaptar saída para padrões internos de cada equipe.

#### 5.2.2 Check visual de consistência

- Implementação estimada: **80%**.
- Indicação de ausência, divergência ou sincronização de artefatos.
- Apoia revisão rápida antes de commit/publicação.

#### 5.2.3 Estratégia de governança

- Implementação estimada: **74%**.
- Versionar templates junto ao repositório quando possível.
- Definir baseline de geração por tipo de projeto.

### 5.3 Padrões recomendados para adoção em equipe

#### 5.3.1 Template baseline por tipo de solução

- Implementação estimada: **70%**.
- API: foco em repositórios e testes de integração leve.
- Worker/Batch: foco em comandos DML e validação de consistência.

#### 5.3.2 Revisão periódica de templates

- Implementação estimada: **70%**.
- Revisão trimestral para refletir novas convenções arquiteturais.
- Checklist de compatibilidade antes de atualizar templates compartilhados.

---

## 6) Distribuição e ciclo de vida

### 6.0 Objetivo de ciclo de vida

- Assegurar distribuição estável para consumidores legados e modernos.
- Garantir alinhamento entre versão de pacote, documentação e ferramentas associadas.

### 6.1 Targets e compatibilidade .NET

#### 6.1.1 Bibliotecas de provedores

- Implementação estimada: **100%**.
- Alvos: .NET Framework 4.8, .NET 6.0 e .NET 8.0.
- Cobertura de cenários legados e modernos.

#### 6.1.2 Núcleo DbSqlLikeMem

- Implementação estimada: **100%**.
- Alvos: .NET Standard 2.0 + .NET Framework 4.8 + .NET 6.0 + .NET 8.0.
- Estratégia para maximizar reuso em diferentes ambientes de execução.

#### 6.1.3 Implicações para consumidores

- Implementação estimada: **88%**.
- Projetos antigos e novos podem adotar a biblioteca com fricção reduzida.
- Planejamento de upgrade pode ser progressivo.

### 6.2 Publicação

#### 6.2.1 NuGet

- Implementação estimada: **85%**.
- Fluxo de empacotamento e distribuição de pacotes.
- Controle de versão semântica para evolução previsível.

#### 6.2.2 Extensões IDE

- Implementação estimada: **72%**.
- Publicação VSIX (Visual Studio).
- Publicação de extensão VS Code.
- Expande adoção em diferentes perfis de desenvolvedor.

#### 6.2.3 Operação contínua

- Implementação estimada: **86%**.
- Checklist de release para validação de artefatos.
- Sincronização entre documentação, pacote e extensões.
- Workflow CI matricial por provider e smoke cross-dialeto inicial já suportam auditoria contínua de regressão.
- Evolução de concorrência deve separar rotinas CI em smoke vs completo, com traits por categoria (isolamento, savepoint, conflito de escrita, stress).
- Próximos ciclos incluem trilhas de observabilidade, performance, concorrência e ecossistema (.NET/ORM/tooling) já descritas no pipeline de prompts e no plano executável P7–P14.

### 6.3 Organização da solução e ritmo de desenvolvimento

#### 6.3.1 Arquivo de solução (`.slnx`) e cobertura de projetos

- Implementação estimada: **96%**.
- Solução `DbSqlLikeMem.slnx` já estruturada por domínio/provedor e pronta para uso no Visual Studio 2026.
- Validação operacional indica cobertura completa dos projetos `*.csproj` do repositório na solução.
- Verificação automatizada já adicionada ao CI via `scripts/check_slnx_project_coverage.py` e com alternativa local Windows em `scripts/check_slnx_project_coverage.ps1` para detectar drift entre árvore `src` e conteúdo da solução.

#### 6.3.2 Matriz compartilhada de testes por capability

- Implementação estimada: **92%**.
- Priorizar base compartilhada para cenários repetitivos cross-dialect (ex.: agregação textual, `DISTINCT`, `NULL`, ordered-set).
- Reduzir duplicação de testes específicos por provider movendo contratos comuns para fixtures parametrizadas.
- Facilita evolução coordenada do parser/executor sem espalhar ajustes em múltiplos projetos de teste.
- Entregas recentes na trilha:
  - suíte compartilhada de agregação/having/ordinal já consolidada e reutilizada por MySQL, SQL Server, Oracle, Npgsql, SQLite e DB2;
  - normalização de nomenclatura dos testes cross-provider para reduzir variação entre cenários equivalentes;
  - alinhamento da base de smoke para manter mesma ordem de validação entre providers e simplificar diagnóstico de regressão.
  - camada compartilhada `SqlNotSupportedAssert` + helper base `AssertWithinGroupNotSupported(...)` adotados nos testes de agregação para padronizar validação de erro `NotSupported` com token da feature em SQL Server, Oracle, Npgsql, DB2, MySQL e SQLite.
  - contratos compartilhados para agregação textual com separador e `DISTINCT` + `NULL` extraídos para a base comum `AggregationHavingOrdinalTestsBase` e reutilizados por MySQL/SQL Server/Oracle/Npgsql/SQLite/DB2.
  - bloco comum de projeção mista (`agregação textual + NULL literal`) implementado na base compartilhada e validado nos seis providers Dapper principais, reduzindo risco de regressão em mapeamentos dinâmicos de resultado.
  - cobertura compartilhada expandida para projeção `CASE ... THEN NULL` combinada com agregação textual agrupada nos seis providers, reforçando previsibilidade para cenários de relatório com colunas calculadas nulas.
  - cobertura compartilhada ampliada para `CASE` com ramos mistos (`texto`/`NULL`) sobre agregação textual, validando estabilidade de ordem e coercão básica de saída por provider.
  - cobertura avançou para `CASE` de múltiplos ramos (`primary`/`secondary`/`NULL`) com agregação textual e ordenação estável, reduzindo risco de divergência em relatórios agrupados cross-provider.
  - cobertura evoluiu para `CASE` numérico multibranch (`100`/`200`/`0`) junto de agregação textual, validando estabilidade de coerção e leitura de tipos numéricos por provider.
- Próximos incrementos da capability matrix:
  - ampliar contratos compartilhados para cenários de ordenação dentro da agregação textual quando habilitados por dialeto;
  - expandir bloco comum para cenários de `CASE` com literais textuais e numéricos mistos no mesmo campo (coerção implícita cross-dialect);
  - consolidar assertions de mensagens de erro para `NotSupported` em uma camada única reutilizável.

#### 6.3.3 Entrada única de execução (build/test)

- Implementação estimada: **88%**.
- Script padronizado já existe para smoke cross-provider (`run_cross_dialect_equivalence.sh`); próximo passo é consolidar trilhas adicionais (core/parser/dapper completos) e evoluir continuamente os filtros de agregação conforme expansão de contratos textuais cross-dialect.
- Perfis de execução já explícitos no runner (`smoke`/`aggregation`) para acelerar feedback local e CI; modo `--continue-on-error` permite varredura completa com resumo de falhas por execução e snapshots com quadro-resumo por perfil; `--dry-run` permite inspecionar a matriz planejada sem execução de testes.
- CI inclui job dedicado de validação de automações (sintaxe shell, `py_compile`, `--help`, check `.slnx` e validação estrutural dos snapshots markdown) antes da matriz de testes por provider.
- Vincular categorias/traits para habilitar execução seletiva por domínio de regressão.

#### 6.3.4 Governança do backlog de documentação

- Implementação estimada: **72%**.
- Separar visão arquitetural estável e status operacional de sprint para reduzir conflito de merge em percentuais.
- Padronizar update de progresso com checklist de evidência mínima (teste, provider afetado, limitação conhecida).
- Alinhar PR template para exigir vínculo entre mudança de código, teste e atualização de backlog.
- Convenção operacional adotada para os próximos ciclos:
  - toda atualização de percentual deve registrar evidência objetiva (arquivo de teste, comando executado e resultado);
  - itens com escopo multi-provider devem indicar explicitamente onde houve cobertura total e onde permanece gap;
  - quando houver apenas atualização documental, incluir seção de risco de descompasso com o código e ação de mitigação planejada.

### 6.4 Política sugerida de versionamento

#### 6.4.1 SemVer para consumidores

- Implementação estimada: **84%**.
- Incremento major para quebras comportamentais/documentadas.
- Incremento minor para novos recursos compatíveis.
- Incremento patch para correções sem alteração contratual.

#### 6.4.2 Comunicação de mudanças

- Implementação estimada: **80%**.
- Changelog orientado a impacto por provedor/dialeto.
- Destaque para gaps fechados e limitações ainda abertas.

---

## 7) Mapa de aprofundamento sugerido

### 7.0 Como usar este índice no dia a dia

- Planejamento de sprint: usar as seções 1–4 para quebrar itens técnicos.
- Definição de padrões internos: usar seção 5 para operacionalizar templates e geração.
- Preparação de release: usar seção 6 como checklist de governança.

### 7.1 Primeiro nível (macro)

- Entender proposta do engine em memória.
- Mapear provedores usados no contexto do produto.
- Definir fronteira entre teste unitário e integração.

### 7.2 Segundo nível (funcional)

- Explorar parser/executor e API fluente.
- Consolidar padrões de seed e setup.
- Validar cenários críticos com Dapper/ADO.NET.

### 7.3 Terceiro nível (especialização)

- Monitorar métricas e planos de execução mock.
- Trabalhar gaps por dialeto com regressão automatizada.
- Refinar matriz de compatibilidade por domínio de negócio.

### 7.4 Quarto nível (ecossistema)

- Incorporar fluxos de extensão e templates no dia a dia.
- Padronizar publicação e governança documental.
- Manter backlog evolutivo com trilhas por prioridade.

### 7.5 Quinto nível (estratégia de produto)

- Definir roadmap anual de compatibilidade SQL.
- Balancear manutenção de legado e inovação de recursos.
- Criar indicadores de adoção e qualidade para direcionar próximos ciclos.
