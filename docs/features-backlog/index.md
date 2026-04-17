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

- Implementação estimada: **100%**.
- Estruturas para representar tabelas, colunas, linhas e metadados sem dependência de servidor externo.
- Armazenamento volátil por instância de banco mock, permitindo reset completo entre testes.
- Modelo ideal para testes unitários que exigem alta repetibilidade.
- Incremento desta sessão: suporte funcional de `sequence` consolidado no estado em memória do core, incluindo registro por schema, resolução SQL por provider e helpers para setup determinístico de `identity`.
- Incremento desta sessão: snapshots transacionais passam a incluir tabelas temporárias no escopo da conexão, garantindo rollback/rollback-to-savepoint determinístico também para estado temporário em memória (com regressão automatizada).
- Incremento desta sessão: cobertura de regressão expandida para MySQL e SQL Server com cenários dedicados de rollback e rollback-to-savepoint em tabelas temporárias de conexão.
- Incremento desta sessão: API explícita de reset volátil em memória adicionada no banco/conexão (`ResetVolatileData` e `ResetAllVolatileData`) para facilitar setup/teardown determinístico entre testes, com regressões dedicadas em SQLite para limpeza de dados temporários/permanentes e reset de identidade.
- Incremento desta sessão: cobertura de regressão do reset volátil expandida também para MySQL e SQL Server, garantindo paridade de comportamento entre providers principais.
- Incremento desta sessão: cobertura de reset volátil unificada também nos testes de estratégia de Db2, Oracle e Npgsql, garantindo aplicação da melhoria em todos os bancos principais suportados.
- Incremento desta sessão: comportamento seletivo de reset (`includeGlobalTemporaryTables`) coberto nos providers principais, garantindo preservação/limpeza determinística de tabelas temporárias globais conforme configuração.
- Incremento desta sessão: cobertura de rollback e rollback-to-savepoint para tabelas temporárias de conexão adicionada também em Db2, Oracle e Npgsql, fechando paridade entre todos os bancos principais no escopo de persistência temporária em memória.
- Incremento desta sessão: camada Dapper também passou a cobrir rollback e rollback-to-savepoint para tabelas temporárias de conexão em SQLite, MySQL, SQL Server, Oracle, Npgsql e Db2, mantendo paridade de comportamento entre APIs de estratégia e extensão Dapper.
- Incremento desta sessão: camada Dapper também passou a cobrir `ResetAllVolatileData` e `ResetVolatileData(includeGlobalTemporaryTables)` em SQLite, MySQL, SQL Server, Oracle, Npgsql e Db2, consolidando reset volátil determinístico e seletivo em todos os bancos principais.
- Incremento desta sessão: validação de `ResetVolatileData` preservando definições de tabela (schema/colunas) foi unificada em todos os bancos, tanto na camada Strategy quanto na camada Dapper, fechando paridade de contrato do reset em memória.
- Incremento desta sessão: `ResetAllVolatileData` passou a ter regressão dedicada para limpeza de linhas em tabelas temporárias globais (com preservação de definição) em todos os bancos, nas camadas Strategy e Dapper.
- Incremento desta sessão: `ResetAllVolatileData` passou a validar explicitamente invalidação de savepoints/estado transacional ativo em todos os bancos principais na camada Strategy, garantindo teardown determinístico sem reaproveitamento de snapshot transacional após reset.
- Incremento desta sessão: invalidação de savepoints após `ResetAllVolatileData` também foi coberta na camada Dapper em todos os bancos principais, mantendo simetria de contrato entre superfícies de uso.
- Incremento desta sessão: `Db.ResetVolatileData(...)` passou a ter regressão explícita de não interferência em tabelas temporárias de conexão (escopo de sessão) em todos os bancos, nas camadas Strategy e Dapper.

#### 1.1.2 Isolamento para testes unitários

- Implementação estimada: **100%**.
- Execução sem I/O de rede obrigatório.
- Cenários independentes de disponibilidade de banco real.
- Redução de flakiness em pipelines de CI.
- Incremento desta sessão: extensões de DI receberam registro `Transient` no núcleo e em todos os providers principais (`Sqlite`, `MySql`, `SqlServer`, `Oracle`, `Npgsql`, `Db2`, `SqlAzure`), permitindo isolamento explícito por resolução de serviço em cenários de teste.
- Incremento desta sessão: regressão de DI adicionada para `Transient` no contrato genérico (`AddDbMockTransient<T>`) e nos providers principais, garantindo criação de nova instância por resolução com aplicação determinística de setup (`acRegister`).
- Incremento desta sessão: cobertura da `DbMockConnectionFactory` expandida para todos os bancos principais com validação de tipo/provider, aplicação de `tableMappers` e isolamento entre chamadas consecutivas da fábrica (sem vazamento de estado).
- Incremento desta sessão: canonicalização/aliases de provider na `DbMockConnectionFactory` passou a ter regressão dedicada em todos os bancos principais (incluindo aliases PostgreSQL), reforçando resolução determinística de tipo de mock/conexão e reduzindo flakiness por variação de input.
- Incremento desta sessão: camada Strategy dos bancos principais passou a cobrir explicitamente exposição/reset de `CurrentIsolationLevel` (begin com nível explícito + reset para `Unspecified` em commit/rollback), reforçando isolamento transacional determinístico por conexão.
- Incremento desta sessão: isolamento de tabelas temporárias de conexão entre múltiplas conexões simultâneas do mesmo `DbMock` passou a ter regressão dedicada em todos os bancos principais na camada Strategy, evitando vazamento de estado por escopo de sessão.
- Incremento desta sessão: isolamento de tabelas temporárias de conexão entre múltiplas conexões também passou a ter regressão dedicada na camada Dapper em todos os bancos principais, concluindo paridade de isolamento entre superfícies de uso.
- Incremento desta sessão: wrappers Dapper de `TransactionReliabilityTests` dos seis bancos principais passaram a reutilizar a base genérica `ProviderDapperTransactionReliabilityTestsBase<TDb,TConnection>`, consolidando criação isolada de `DbMock`/conexão por cenário e reduzindo boilerplate sujeito a drift entre providers.
- Incremento desta sessão: `FluentTest` dos seis bancos principais passou a reutilizar a base compartilhada `DapperFluentTestsBase<TDb,TConnection>`, uniformizando setup/seed fluente com criação isolada de conexão por provider e reduzindo variação estrutural entre suites equivalentes.
- Incremento desta sessão: `Extended*MockTests` dos seis bancos principais também passaram a reutilizar a base compartilhada `ExtendedDapperProviderTestsBase<TDb,TConnection,TException>`, padronizando setup isolado de conexão/tabela para filtros, paginação, FK e inserts em lote, com manutenção local apenas dos casos realmente específicos.

- Incremento desta sessão: `JoinTests` e `TransactionTests` de SQLite, SQL Server, Oracle e Db2 também passaram a reutilizar as bases compartilhadas `DapperJoinTestsBase<TDb,TConnection>` e `DapperTransactionTestsBase<TDb,TConnection>`, removendo criação local repetida de `DbMock`/conexão e alinhando isolamento estrutural com MySQL e Npgsql.
- Incremento desta sessão: `QueryExecutorExtrasTests` de SQLite, SQL Server, Oracle e Db2 também passaram a reutilizar `QueryExecutorExtrasTestsBase`, fechando a padronização cross-provider desse bloco de agregação, paginação multi-result e tradução LINQ.
- Incremento desta sessão: `AdditionalBehaviorCoverageTests` dos seis bancos principais passaram a reutilizar a base compartilhada `AdditionalBehaviorCoverageTestsBase<TDb,TConnection>`, centralizando seed de `users/orders`, variações mínimas de SQL e o mesmo contrato de comportamento para `NULL`, `JOIN`, agregação, `IN`, insert fora de ordem, delete e update com expressão.

#### 1.1.3 Estado e ciclo de vida

- Implementação estimada: **100%**.
- Estado de dados acoplado ao objeto de contexto/conexão mock.
- Facilita setup/teardown por teste, fixture ou suíte.
- Permite compor ambientes mínimos para validação de regra de negócio.
- Incremento desta sessão: fechamento de conexão (`Close`) passou a limpar estado de sessão em memória (transação ativa, savepoints, isolamento corrente e tabelas temporárias de conexão) no core, reduzindo vazamento de estado entre ciclos de vida de conexão.
- Incremento desta sessão: regressão de ciclo de vida adicionada nos bancos principais (SQLite, MySQL, SQL Server, Oracle, Npgsql e Db2) para validar que `Close` encerra sessão de forma determinística e bloqueia rollback para savepoint antigo sem transação ativa.
- Incremento desta sessão: regressão adicional de ciclo de vida garante que `Close` preserve estado compartilhado do banco (tabelas permanentes e temporárias globais), limpando apenas estado da sessão da conexão que foi encerrada.
- Incremento desta sessão: camada Dapper dos bancos principais também passou a cobrir o contrato de `Close` (limpeza de estado de sessão + preservação de estado compartilhado), garantindo paridade de ciclo de vida entre Strategy e Dapper.
- Incremento desta sessão: camada Strategy dos bancos principais passou a cobrir explicitamente reabertura de conexão (`Close` → `Open`) com sessão limpa/reutilizável e preservação de estado compartilhado do banco, reforçando previsibilidade de ciclo de vida entre testes.
- Incremento desta sessão: camada Dapper dos bancos principais passou a cobrir reabertura de conexão (`Close` → `Open`) com sessão limpa/reutilizável e preservação de estado compartilhado, concluindo paridade de ciclo de vida entre Strategy e Dapper.
- Incremento desta sessão: a infraestrutura compartilhada de `TransactionReliabilityTests` na camada Dapper passou a centralizar criação/abertura de conexões por provider, reduzindo risco de divergência acidental no ciclo de vida transacional (`Open`/transação/savepoint) entre SQLite, MySQL, SQL Server, Oracle, Npgsql e Db2.
- Incremento desta sessão: `FluentTest` dos seis bancos principais também passou a centralizar o padrão de abertura/configuração de conexão na base `DapperFluentTestsBase`, reduzindo drift no ciclo de vida de sessão usado para setup rápido de testes consumidores.
- Incremento desta sessão: `Extended*MockTests` dos seis bancos principais passaram a compartilhar o mesmo ciclo de criação/abertura de conexão na base `ExtendedDapperProviderTestsBase`, diminuindo divergência acidental de lifecycle em suítes consumidoras que exercitam inserts, filtros e integridade referencial.

- Incremento desta sessão: `JoinTests` e `TransactionTests` de SQLite, SQL Server, Oracle e Db2 passaram a compartilhar o mesmo ciclo de criação/abertura de conexão nas bases `DapperJoinTestsBase` e `DapperTransactionTestsBase`, reduzindo divergência acidental de lifecycle entre os seis bancos principais nessa trilha Dapper.
- Incremento desta sessão: `QueryExecutorExtrasTests` de SQLite, SQL Server, Oracle e Db2 passaram a compartilhar o mesmo padrão de criação/abertura de conexão e seed na base `QueryExecutorExtrasTestsBase`, reduzindo drift de lifecycle nos cenários Dapper de leitura avançada.
- Incremento desta sessão: `AdditionalBehaviorCoverageTests` dos seis bancos principais também passaram a compartilhar ciclo de criação/abertura/descarta de conexão na base `AdditionalBehaviorCoverageTestsBase`, reduzindo drift de lifecycle nas suites Dapper de comportamento adicional.
- Incremento desta sessão: `SqlAzure` ganhou suíte dedicada de estratégia para transação/ciclo de vida (`commit`, `rollback`, isolamento explícito, `Close`/`Open`, savepoint e invalidação após `ResetAllVolatileData`), fechando a malha explícita de lifecycle transacional também no provider Azure.

### 1.2 Parser SQL

#### 1.2.1 Interpretação de comandos DDL

- Implementação estimada: **95%**.
- Leitura e processamento de comandos de definição de schema.
- Suporte a operações estruturais comuns (criação e alteração de entidades).
- Aplicação de regras específicas por dialeto e versão simulada.
- Incremento desta sessão: parser/executor passaram a suportar `CREATE/DROP SEQUENCE` com AST dedicada, execução non-query e registro/remoção real da sequence no estado em memória.
- Incremento desta sessão: o suporte a `SEQUENCE` passou a obedecer gate explícito do dialeto e da versão simulada, cobrindo `SQL Server/SqlAzure` apenas quando a versão efetivamente suporta a feature e mantendo rejeição acionável em dialetos sem sequence DDL como MySQL.
- Incremento desta sessão: parser DDL passou a rejeitar explicitamente `CREATE OR REPLACE` fora de `VIEW`, evitando aceitação ambígua em `CREATE ... TABLE ...`.
- Incremento desta sessão: `DROP VIEW` passou a validar fim de statement e rejeitar continuação inesperada (`DROP VIEW ... EXTRA`), com regressões de parser adicionadas para SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `CREATE VIEW ... AS` e `CREATE TEMPORARY TABLE ... AS` passaram a rejeitar statement adicional após `;` no corpo (ex.: `... AS SELECT ...; SELECT ...`), reduzindo risco de parse parcial silencioso com regressões unificadas para SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: parser DDL passou a validar corpo obrigatório após `AS` em `CREATE VIEW` e `CREATE TEMPORARY TABLE`, gerando erro acionável para casos como `AS ;`/corpo vazio, com regressões unificadas em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `DROP VIEW` passou a validar explicitamente nome obrigatório (incluindo variantes `DROP VIEW ;` e `DROP VIEW IF EXISTS ;`) com regressões unificadas em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `CREATE VIEW` passou a endurecer validação da lista de colunas (lista vazia e vírgula final agora geram erro acionável), com regressões unificadas em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `CREATE TEMPORARY TABLE` também passou a endurecer validação da lista de colunas (lista vazia, vírgula inicial/final e fechamento ausente), com regressões unificadas em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão foi ampliada para vírgula inicial em listas de colunas de `CREATE VIEW` e `CREATE TEMPORARY TABLE`, mantendo contrato de erro consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão foi ampliada para listas de colunas não fechadas em `CREATE VIEW` e `CREATE TEMPORARY TABLE`, mantendo diagnóstico determinístico e consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `CREATE TEMPORARY TABLE` passou a rejeitar explicitamente ausência de vírgula entre definições de coluna (ex.: `id INT name VARCHAR(...)`) com regressões unificadas em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão foi ampliada para ausência de vírgula entre nomes na lista de colunas de `CREATE VIEW` (ex.: `(id name)`), mantendo contrato de erro consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão foi ampliada para `DROP VIEW` seguido de segundo statement no parse unitário (`DROP VIEW ...; SELECT ...`), reforçando boundary de statement em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de boundary de `DROP VIEW` foi estendida para a variante `IF EXISTS` seguida de segundo statement (`DROP VIEW IF EXISTS ...; SELECT ...`) em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de `CREATE TEMPORARY TABLE` foi ampliada para ausência de vírgula após tipo com parênteses (ex.: `VARCHAR(50) age INT`), mantendo diagnóstico consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de listas de colunas foi ampliada para vírgula duplicada (`id,,name`) em `CREATE VIEW` e `CREATE TEMPORARY TABLE`, mantendo contrato de erro consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de `CREATE VIEW` foi ampliada para lista de colunas não fechada antes de `AS SELECT` (`CREATE VIEW ... (id AS SELECT ...`) em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de `CREATE TEMPORARY TABLE` foi ampliada para variante inválida `IF EXISTS` (aceito apenas `IF NOT EXISTS`) em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: cobertura de regressão de `CREATE ... TABLE` foi ampliada para variante inválida `CREATE GLOBAL TABLE ...` sem `TEMPORARY/TEMP`, reforçando erro explícito e consistente em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: parser DDL passou a suportar `DROP TABLE` (incluindo `IF EXISTS` e variantes `TEMP/TEMPORARY/GLOBAL TEMPORARY`) com validação de nome obrigatório e boundary de statement.
- Incremento desta sessão: cobertura de regressão de `DROP TABLE` foi adicionada de forma unificada em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2, incluindo casos válidos (`IF EXISTS`, `GLOBAL TEMPORARY`) e inválidos (`DROP TABLE IF EXISTS ;`, `DROP GLOBAL TABLE ...`, segundo statement indevido).
- Incremento desta sessão: corpus de parser por provedor foi alinhado para remover `DROP TABLE` da lista de comandos explicitamente inválidos, refletindo o novo contrato de interpretação DDL.
- Incremento desta sessão: parser/executor passaram a suportar o subset pragmático de `CREATE INDEX` e `DROP INDEX`, incluindo `UNIQUE`, lista simples de colunas, `IF EXISTS` em `DROP INDEX` e a variante `DROP INDEX ... ON <table>` nos dialetos que a expõem (`MySQL` e `SQL Server`), com busca única por índice no schema atual quando o `DROP` não informa tabela.
- Incremento desta sessão: parser/runtime passaram a suportar o subset pragmático de `ALTER TABLE ... ADD [COLUMN] ...` com tipo simples, `NULL/NOT NULL` e `DEFAULT` literal, incluindo backfill determinístico de linhas já existentes e regressões dedicadas em Auto/SQLite/MySQL, além do alinhamento do corpus por provedor para retirar `ALTER TABLE` da trilha de comandos explicitamente inválidos.
- Incremento desta sessão: o subset de `ALTER TABLE ... ADD [COLUMN] ...` recebeu hardening adicional para bloquear coluna duplicada com erro consistente de metadata e para rejeitar `NOT NULL` sem `DEFAULT` em tabela já populada sem deixar mutação parcial, com regressões direcionadas em MySQL e SQLite/Auto.
- Incremento desta sessão: `ALTER TABLE ... ADD [COLUMN] ...` passou a validar explicitamente referência de tabela sem alias na gramática compartilhada, emitindo erro acionável para variantes como `ALTER TABLE users u ADD COLUMN ...` no modo `Auto` e no dialeto SQL Server.
- Incremento desta sessão: `ALTER TABLE ... ADD [COLUMN] ...` passou a rejeitar explicitamente `NOT NULL DEFAULT NULL` na gramática compartilhada, evitando que `DEFAULT NULL` seja tratado como ausência silenciosa de default no caminho de execução.
- Incremento desta sessão: `ALTER TABLE ... ADD [COLUMN] ...` passou a exigir nome de tabela concreto também contra fontes derivadas como `ALTER TABLE (SELECT ...) u ADD COLUMN ...`, mantendo o subset pragmático alinhado ao contrato de DDL estrutural exposto pelo mock.
- Incremento desta sessão: o parser de `ALTER TABLE ... ADD [COLUMN] ...` deixou de normalizar argumentos de tipo inválidos para defaults silenciosos e agora rejeita explicitamente variantes malformadas como `VARCHAR(foo)` e `DECIMAL(10, foo)`, com regressões em Auto/SQL Server e caminho end-to-end MySQL.
- Incremento desta sessão: `ALTER TABLE ... ADD [COLUMN] ...` passou a preservar também a precisão de colunas `DECIMAL(p,s)` nos metadados compartilhados (além da escala), alinhando AST e runtime ao contrato de schema esperado no mock.
- Incremento desta sessão: `CREATE INDEX` passou a bloquear colunas-chave duplicadas (`(Name, Name)`) com erro explícito antes de registrar metadata parcial, com regressão dedicada no pipeline MySQL.
- Incremento desta sessão: o runtime de `CREATE INDEX` passou a validar colunas-chave referenciadas antes de registrar metadata, rejeitando índice sobre coluna inexistente mesmo em tabela vazia e evitando aceitação silenciosa que antes só explodia quando surgissem linhas.
- Incremento desta sessão: a API de core de `CREATE INDEX` passou a validar também `include columns`, rejeitando duplicatas e sobreposição redundante com as colunas-chave antes de registrar metadata parcial em tabela vazia.
- Incremento desta sessão: o hardening de `include columns` em `CREATE INDEX` passou a comparar sobreposição com `key columns` de forma case-insensitive e a persistir os nomes `include` já normalizados na metadata do índice, evitando drift de casing/wrappers no core.
- Incremento desta sessão: o parser de `CREATE INDEX` também passou a rejeitar lista de colunas-chave duplicadas já na construção da AST compartilhada, evitando aceitar DDL inválido no modo `Auto` e reduzindo divergência entre parse e runtime.
- Incremento desta sessão: `CREATE INDEX` passou a exigir referência de tabela concreta sem alias na gramática compartilhada, rejeitando tanto `ON users u (...)` quanto fontes derivadas como `ON (SELECT ...) u (...)` antes de cair em erros genéricos do runtime.
- Incremento desta sessão: `DROP INDEX ... ON <table>` passou a validar explicitamente nome de tabela obrigatório na gramática compartilhada, emitindo erro acionável para casos como `DROP INDEX ix_users_name ON ;` no modo `Auto` e no dialeto SQL Server.
- Incremento desta sessão: `DROP INDEX ... ON <table>` passou a exigir referência de tabela concreta sem alias na gramática compartilhada, evitando aceitar `DROP INDEX ... ON users u` fora do contrato pragmático exposto por Auto/SQL Server.
- Incremento desta sessão: `DROP INDEX ... ON <table>` deixou de aceitar `table sources` genéricos e agora exige nome qualificado concreto também contra fontes derivadas como `ON (SELECT ...) u`, mantendo o subset pragmático alinhado ao contrato real exposto pelo mock.
- Incremento desta sessão: a cobertura de runtime de `DROP INDEX` foi ampliada para rejeitar busca ambígua por nome sem tabela explícita quando mais de uma tabela do schema atual expõe o mesmo índice, preservando a metadata intacta no caminho MySQL.
- Incremento desta sessão: parser/runtime passaram a suportar um primeiro subset pragmático de `FUNCTION` escalar (`CREATE FUNCTION ... RETURNS <type> AS BEGIN RETURN <expr> END` + `DROP FUNCTION`) no estilo `SQL Server/SqlAzure`, com execução end-to-end de chamadas zero-arg e gate explícito de `NotSupportedException` nos demais dialetos enquanto as variantes reais de cada provider continuam fora do subset.
- Incremento desta sessão: o suporte de FUNCTION evoluiu para um subset pragmático e provider-real também em MySQL, PostgreSQL, Oracle e Db2, com parâmetros escalares simples, corpo mínimo compatível por dialeto (RETURN <expr>, AS 'SELECT <expr>' LANGUAGE SQL, RETURN ... IS BEGIN ... END) e cobertura versionada/end-to-end nos providers que aceitam DDL de função; SQLite permanece fora do escopo com gate explícito de NotSupportedException.
- Incremento desta sessão: `CREATE OR REPLACE FUNCTION` passou a ser suportado no subset provider-real de PostgreSQL e Oracle, com substituição end-to-end do corpo existente e regressão versionada explícita de rejeição nos providers que aceitam `FUNCTION` mas não expõem a variante `OR REPLACE`.
- TODO: revisar a trilha restante de objetos programáveis (`PROCEDURE`/`TRIGGER` DDL e variantes avançadas de `FUNCTION`, como sobrecarga por assinatura, defaults de parâmetros, funções tabulares e corpos procedurais mais ricos) para deixar explícito no backlog o que será suportado de forma real e o que continuará bloqueado por `NotSupportedException`.

#### 1.2.2 Interpretação de comandos DML

- Implementação estimada: **97%**.
- Processamento de comandos de escrita e leitura.
- Tradução da consulta para operações no estado em memória.
- Incremento desta sessão: `LIKE ... ESCAPE ...` deixou de ser apenas tolerado no parse e passou a ser materializado na AST e respeitado no executor, com política de escape padrão agora centralizada no dialeto e regressão cobrindo parser/roundtrip e execução DB2 end-to-end.
- Incremento desta sessão: `LIKE ... ESCAPE ...` passou também a rejeitar valores com mais de um caractere tanto no parse literal quanto na avaliação parametrizada, mantendo o contrato real de cardinalidade do escape sob regra do dialeto.
- Incremento desta sessão: `JSON_VALUE(... RETURNING <tipo>)` passou a obedecer gate explícito de dialeto no parser e a aplicar coerção do payload no executor, fechando o contrato Oracle sem vazar a sintaxe para SQL Server.
- Incremento desta sessão: `REGEXP` do executor passou a obedecer política explícita de case-sensitivity do dialeto, fechando a semântica esperada do MySQL em cenários com pattern minúsculo.
- Incremento desta sessão: parser/runtime passaram a cobrir a trilha principal de sequences por provider, incluindo `NEXT VALUE FOR` (SQL Server/DB2), `nextval/currval/setval/lastval` (Npgsql), `seq.NEXTVAL/CURRVAL` (Oracle) e variantes qualificadas por schema.
- Hardening recente reforça parsing de DML com `RETURNING` (itens vazios, vírgula inicial e vírgula final) com mensagens acionáveis no dialeto suportado e gate explícito nos não suportados.
- Incremento desta sessão: suporte a `MATCH(...) AGAINST(...)` no fluxo MySQL (parser + evaluator) com validação de modos (`IN BOOLEAN MODE`, `IN NATURAL LANGUAGE MODE`, variantes com `WITH QUERY EXPANSION`), gate explícito para dialetos não-MySQL e regressão cobrindo também query parametrizada de candidatos léxicos (`@QueryText`/`@CandidateLimit`) com `ORDER BY lexical_score DESC`.
- Incremento desta sessão: `INSERT ... VALUES` passou a resolver corretamente `CAST(@param AS JSON)` no caminho de persistência (incluindo `ON DUPLICATE KEY UPDATE` com `VALUES(col)`), evitando gravar texto bruto iniciando por `CAST(` e mantendo payload JSON íntegro no mock MySQL.
- Incremento desta sessão: splitter de `INSERT ... VALUES` foi endurecido para respeitar strings quoted (single/double) ao separar por vírgula, evitando quebrar literais JSON/texto com vírgulas internas e aproximando o comportamento do MySQL real.
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
- TODO: revisar cobertura equivalente de sintaxes nativas de `sequence` nos demais providers que exponham formas próprias alem de `SQL Server`, `Npgsql`, `Oracle` e `DB2`.
- TODO: avaliar variantes adicionais de `sequence` por dialeto somente quando houver demanda real e validacao contra o comportamento do banco/provedor real.
- TODO: levar a trilha de `sequence` para exemplos/documentacao canonica end-to-end assim que a matriz cross-provider dessa feature estiver fechada.
- TODO: manter este item abaixo de `100%` até fechar as famílias reais de DML/query ainda fora do fluxo principal do parser/runtime (`FOR JSON`, `CROSS APPLY/OUTER APPLY`, `DISTINCT ON`, `LATERAL`, `json_each/json_tree` e demais formas tabulares correlatas por provider).
- TODO: revisar materialização/execução de DML avançado por provider para que o item só volte a `100%` quando as diferenças remanescentes estiverem reduzidas a subset documentado e intencional.

#### 1.2.3 Regras por dialeto e versão

- Implementação estimada: **92%**.
- Ativa/desativa construções sintáticas por provedor e versão.
- Trata incompatibilidades históricas entre bancos diferentes.
- Direciona comportamento esperado em testes de compatibilidade.
- Checklist de known gaps indica cobertura concluída para MERGE por dialeto, WITH RECURSIVE e normalização de paginação/quoting.
- Incremento desta sessão: `STRING_AGG` passou a obedecer gate explícito de dialeto/versão no núcleo, com `SQL Server` habilitando a função apenas a partir de 2017 e `SqlAzure` herdando o mesmo contrato a partir do compatibility level 140.
- Incremento desta sessão: `OPENJSON` passou a obedecer gate explícito de dialeto/versão já no parser, alinhando `SQL Server/SqlAzure` ao suporte de 2016+ e evitando aceite prematuro antes da semântica compatível.
- Incremento desta sessão: `JSON_EXTRACT` passou a obedecer gate explícito de dialeto/versão já no parser do MySQL, alinhando o aceite ao contrato `5+` já declarado no dialeto e ao gate que antes existia apenas no executor.
- Incremento desta sessão: a família `DATEADD/DATE_ADD/TIMESTAMPADD` passou a obedecer gate explícito do dialeto já no parser, impedindo aceite cruzado indevido entre sintaxes de SQL Server e MySQL antes do executor.
- Incremento desta sessão: `NEXT VALUE FOR`/`PREVIOUS VALUE FOR` passaram a obedecer capabilities distintas no dialeto e no executor, mantendo `SQL Server/SqlAzure` com suporte apenas a `NEXT VALUE FOR` a partir de 2012/compatibility level 110 e preservando `PREVIOUS VALUE FOR` como sintaxe específica do DB2.
- Incremento desta sessão: `seq.NEXTVAL/CURRVAL` passou a obedecer capability explícita do dialeto no parser e no executor, preservando a forma pontuada como sintaxe Oracle e rejeitando esse formato nos demais providers, como Npgsql.
- Incremento desta sessão: `nextval/currval/setval/lastval` passou a obedecer capability explícita do dialeto no parser e no executor, preservando essa família como sintaxe PostgreSQL/Npgsql e rejeitando o formato em dialetos como SQL Server.
- Incremento desta sessão: `ILIKE` passou a obedecer capability explícita do dialeto no parser e no executor, preservando a semântica case-insensitive apenas no Npgsql e rejeitando o operador em dialetos como SQL Server.
- Incremento desta sessão: `JSON_TABLE` passou a obedecer gate explícito do dialeto já no parser e no runtime, mantendo a mesma fonte de verdade de capability por provider.
- Incremento desta sessão: `MATCH ... AGAINST` passou a sair de capability explícita do dialeto também no runtime, removendo o acoplamento ao nome hardcoded `mysql` e alinhando parser/executor à mesma fonte de verdade.
- Incremento desta sessão: o executor deixou de usar switches por `dialect.Name` para `FOUND_ROWS/ROW_COUNT/CHANGES/ROWCOUNT/@@ROWCOUNT`; esses aliases de row-count agora saem de capabilities explícitas do dialeto, incluindo herança automática do caminho `SqlAzure -> SqlServer`.
- Incremento desta sessão: o parser passou a obedecer a mesma capability de row-count do dialeto para `FOUND_ROWS()/ROW_COUNT()/CHANGES()/ROWCOUNT()`, evitando aceitar no parse chamadas que o executor já não considerava válidas para aquele banco.
- Incremento desta sessão: o tokenizer do parser deixou de hardcodear `sqlserver` para `@@ROWCOUNT`; a sintaxe `@@ident` agora também é capability explícita do dialeto, herdada automaticamente por `SqlAzure` e rejeitada nos demais providers.
- Incremento desta sessão: as sintaxes de mutação multi-tabela (`UPDATE ... JOIN/FROM` e `DELETE ... FROM/USING`), o rowcount de UPSERT e o modificador MySQL `SQL_CALC_FOUND_ROWS` passaram a obedecer capabilities explícitas do dialeto em parser, strategies e executor; o fallback legado de frame clause do DB2 também foi removido, deixando a regra "o dialeto manda" sem branch comportamental residual por nome de provider nessa trilha.
- Incremento desta sessão: a primeira fatia de `SqlDialect.Auto` entrou no parser com `AutoSqlDialect`, `SqlSyntaxDetector` e `DialectNormalizer`, normalizando `TOP`, `LIMIT`, `FETCH FIRST`, `OFFSET/FETCH` e o subset seguro de `ROWNUM` para a mesma AST de paginação, sem novos branches no executor.
- Incremento desta sessão: o hot path de `SqlQueryParser.Parse` deixou de retokenizar o mesmo SQL no parse uncached, reduzindo custo linear redundante justamente na nova trilha de detecção automática.
- Incremento desta sessão: o pipeline de parsing agora também expõe entradas explícitas dedicadas ao modo `Auto` (`ParseAuto`, `ParseMultiAuto`, `SplitStatementsAuto`, `ParseUnionChainAuto`, `ParseScalarAuto` e `ParseWhereAuto`), centralizando a criação do dialeto automático sem espalhar construção manual no código chamador.
- TODO: evoluir `SqlDialect.Auto` com heuristicas adicionais realmente necessarias (`identidade`, `concatenacao` e demais diferencas compartilhadas), mantendo parser e executor agnosticos a provider.
- TODO: expor `SqlDialect.Auto` como entrada explícita também no runtime de execução, sem quebrar o modelo atual baseado em provider/`DbMock`.
- TODO: garantir que a expansao de familias continue baseada em heranca/refatoracao de dialeto compartilhado (`MariaDb` na familia MySQL e `DuckDb` na familia PostgreSQL), sem reintroduzir switches centrais por nome de provider.

#### 1.2.4 Governança de evolução do parser

- Implementação estimada: **85%**.
- Backlog guiado por gaps observados em testes reais.
- Track global de normalização Parser/AST consolidado em ~90%, com foco atual em refinos finais por dialeto.
- Priorização por impacto em frameworks de acesso a dados.
- Expansão incremental para reduzir regressões.
- Backlog operacional segue cadência priorizada P0→P14 para reduzir dispersão de implementação entre parser/executor/docs.
- TODO: exigir que cada novo gap do parser registre explicitamente AST afetada, capability do dialeto, suites positivas/negativas e impacto documental antes de subir percentual.
- TODO: consolidar um inventário executável de gaps ainda abertos por sintaxe/família SQL para reduzir drift entre backlog, código e testes cross-dialect.

#### 1.2.5 Funções SQL agregadoras e de composição de texto

- Implementação estimada: **100%**.
- Parser e AST agora suportam `WITHIN GROUP (ORDER BY ...)` para agregações textuais com gate explícito por dialeto/função.
- Cobertura atual inclui parsing de ordenação simples e composta, validação de cláusula malformada (`WITHIN GROUP requires ORDER BY`) e cenários negativos por função não nativa no dialeto.
- Hardening recente ampliou a validação de `ORDER BY` malformado dentro de `WITHIN GROUP` (lista vazia, vírgula inicial, vírgula final e ausência de vírgula entre expressões), com mensagens acionáveis por cenário.
- Runtime aplica a ordenação de `WITHIN GROUP` antes da agregação, incluindo combinações com `DISTINCT` e separador customizado.
- Incremento desta sessão: parser/runtime passaram a aceitar a sintaxe nativa do SQLite para ordenação interna em `GROUP_CONCAT(... ORDER BY ...)`, reutilizando a mesma trilha lógica de ordenação da agregação textual e cobrindo também `DISTINCT` + erro acionável para vírgula final malformada.
- Incremento desta sessão: parser/runtime passaram a aceitar a sintaxe nativa do MySQL para `GROUP_CONCAT(expr ORDER BY ... SEPARATOR ...)`, reaproveitando a mesma trilha de ordenação da agregação textual, com cobertura para `DISTINCT` e erro acionável quando `SEPARATOR` não recebe expressão.
- Trilha ordered-set para agregações textuais concluída para dialetos suportados (SQL Server, Npgsql, Oracle e DB2), com bloqueio explícito e testado para MySQL e manutenção do `WITHIN GROUP` como não suportado no SQLite, onde o equivalente nativo `GROUP_CONCAT(... ORDER BY ...)` agora está coberto.
- Incremento desta sessão: o parser e a suíte regressiva passaram a rejeitar `DISTINCT` nas combinações não oficiais por dialeto/agregador, bloqueando `STRING_AGG(DISTINCT ...)` em SQL Server/SqlAzure e `GROUP_CONCAT(DISTINCT ..., separador)` no SQLite, enquanto as formas oficiais com `ORDER BY` interno e `SEPARATOR` seguem cobertas nos providers que as expõem.

#### 1.2.6 Funções de data/hora cross-dialect

- Implementação estimada: **94%**.
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
- TODO: fechar a família temporal além de "current time" com equivalências guiadas pelo dialeto para `DATE_TRUNC`/`DATETRUNC`, `DATEDIFF`/`TIMESTAMPDIFF` e aritmética de intervalo por provider/versão.
- TODO: centralizar a avaliação temporal compartilhada para que parser, executor AST e estratégias de mutação usem a mesma fonte de verdade também nas famílias de diferença/truncamento de data.

#### 1.2.7 Detecção automática de dialeto

- Implementação estimada: **97%**.
- Objetivo: aceitar múltiplas sintaxes SQL equivalentes sem exigir seleção manual prévia do dialeto.
- O parser deve continuar agnóstico; a detecção e a normalização devem ficar concentradas em componentes próprios de dialeto.
- Incremento desta sessão: a primeira entrega de `Auto` já detecta marcadores de paginação/`ROWNUM` em varredura linear (`SqlSyntaxDetector`) e normaliza `TOP`, `LIMIT`, `FETCH FIRST`, `OFFSET/FETCH` e `ROWNUM` seguro para `SqlLimitOffset` (`DialectNormalizer`).
- Incremento desta sessão: o parser já possui entradas explícitas dedicadas para o modo `Auto` em queries e expressões (`ParseAuto`, `ParseMultiAuto`, `SplitStatementsAuto`, `ParseUnionChainAuto`, `ParseScalarAuto` e `ParseWhereAuto`), reduzindo acoplamento de criação manual do dialeto.
- Incremento desta sessão: a cobertura TDD inicial já valida shape canônico de AST para `TOP`, `LIMIT`, `FETCH FIRST`, `OFFSET/FETCH`, `ROWNUM` simples, `ROWNUM` parametrizado, combinação com `AND`, combinação com `TOP`, helpers dedicados de parse de query/expressão e o caso inseguro com `OR`.
- Incremento desta sessão: o pipeline compartilhado de execução agora resolve um dialeto efetivo por conexão (`UseAutoSqlDialect`) e já aceita em runtime sintaxes equivalentes de paginação como `TOP` e `FETCH FIRST` em providers concretos, sem introduzir branches sintáticos no executor.
- Incremento desta sessão: a cobertura de runtime do modo `Auto` agora inclui também mutação suportada (`INSERT ... SELECT TOP 1`) no pipeline compartilhado de non-query, além de leitura com `TOP` e `FETCH FIRST` em `SqliteConnectionMock`.
- Incremento desta sessão: o modo `Auto` agora também possui regressao de batch multi-statement em runtime cobrindo `TOP`, `FETCH FIRST` e `LIMIT` no mesmo `ExecuteReader`, reforçando a equivalencia operacional das sintaxes de paginação já normalizadas.
- Incremento desta sessão: a propagacao do dialeto efetivo no runtime foi estendida aos `CommandMock` restantes (`MySql`, `Db2`, `Npgsql`, `Oracle` e `SqlServer`), reduzindo risco de comportamento inconsistente do modo `Auto` entre providers.
- Incremento desta sessão: a trilha TDD de runtime do modo `Auto` agora prova equivalencia de resultado para `TOP`, `LIMIT`, `FETCH FIRST` e o subset seguro de `ROWNUM`, todos convergindo para a mesma leitura em `SqliteConnectionMock`.
- Incremento desta sessão: `SqlSyntaxDetector` passou a cobrir tambem heuristicas lineares de `identidade` (`IDENTITY`, `AUTO_INCREMENT`, `SERIAL`, `BIGSERIAL`) e `concatenacao` (`CONCAT`, `CONCAT_WS`, `||`), com regressao para evitar falso positivo quando esses marcadores aparecem apenas dentro de strings.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem a familia compartilhada de `sequence` (`CREATE/DROP SEQUENCE`, `NEXT VALUE FOR`, `PREVIOUS VALUE FOR`, `NEXTVAL/CURRVAL/LASTVAL` e `seq.NEXTVAL/CURRVAL`), reaproveitando o evaluator e o runtime ja existentes sem adicionar branch especial por provider.
- Incremento desta sessão: `SqlSyntaxDetector` passou a reconhecer tambem marcadores baratos da familia `sequence` (`SEQUENCE`, `NEXT/PREVIOUS VALUE FOR`, `NEXTVAL`, `CURRVAL`, `SETVAL`, `LASTVAL`), e a trilha TDD do runtime agora cobre estado de sessao com `PREVIOUS VALUE FOR` e `DROP SEQUENCE IF EXISTS` no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem operadores JSON compartilhados (`->`, `->>`, `#>`, `#>>`), reaproveitando `JsonAccessExpr` e a avaliacao compartilhada do executor; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime de `->>` no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem as funcoes JSON compartilhadas `JSON_EXTRACT` e `JSON_VALUE`, reaproveitando gates ja existentes no parser/executor; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime dessas chamadas no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem aliases temporais compartilhados (`CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `SYSTEMDATE`, `SYSDATE`, `NOW()`, `GETDATE()`, `SYSDATETIME()`, `SYSTIMESTAMP()`), reaproveitando `SqlTemporalFunctionEvaluator`; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime desses aliases no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `JSON_VALUE ... RETURNING`, reaproveitando a coerção ja existente no executor compartilhado; a trilha TDD cobre parsing do payload `RETURNING` e execução com coerção numérica no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a explicitar tambem a familia compartilhada de adição temporal (`DATE_ADD`, `DATEADD`, `TIMESTAMPADD`), reaproveitando o evaluator temporal ja existente; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime das três variantes no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem a familia compartilhada de agregação textual (`GROUP_CONCAT`, `STRING_AGG`, `LISTAGG`), incluindo `WITHIN GROUP`, `ORDER BY` interno e `SEPARATOR` no subset comum; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime dessas variantes no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem a familia compartilhada de rowcount (`FOUND_ROWS`, `ROW_COUNT`, `CHANGES`, `ROWCOUNT` e `@@ROWCOUNT`), reaproveitando o estado de last-found-rows ja existente na conexao/executor; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime dessas variantes no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem o modificador `SQL_CALC_FOUND_ROWS`, reaproveitando o suporte ja existente do parser e do executor para popular `FOUND_ROWS()`; o detector barato agora tambem marca esse sinal e a trilha TDD cobre parsing e runtime do fluxo `SELECT SQL_CALC_FOUND_ROWS ...; SELECT FOUND_ROWS();`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem o operador de igualdade null-safe `<=>`, reaproveitando o `SqlBinaryOp.NullSafeEq` e a avaliação já existente no executor; o detector barato agora tambem marca esse operador e a trilha TDD cobre parsing e runtime no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `ILIKE`, reaproveitando o `LikeExpr` com `CaseInsensitive = true` e a avaliação já existente no executor; o detector barato agora tambem marca esse operador e a trilha TDD cobre parsing e runtime no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `MATCH ... AGAINST`, reaproveitando o parser para `MATCH_AGAINST` e o evaluator compartilhado de score/boolean mode; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a explicitar tambem `IF`/`IIF` e a familia compartilhada de null-substitute (`IFNULL`, `ISNULL`, `NVL`, `COALESCE`, `NULLIF`); o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime escalar no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `OPENJSON` no subset escalar já suportado pelo parser/evaluator compartilhados; o detector barato passou a incluir essa chamada na familia de funcoes JSON e a trilha TDD cobre parsing e runtime basico no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a assumir explicitamente tambem a superficie compartilhada de window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`) no subset já suportado pelo parser/evaluator; o detector barato agora tambem marca essa familia e a trilha TDD cobre parsing e runtime de `ROW_NUMBER`/`LAG` no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `PIVOT` no subset compartilhado já implementado pelo parser/executor (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`); o detector barato agora tambem marca essa clausula e a trilha TDD cobre parsing e runtime com `COUNT(...) FOR ... IN (...)` no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `WITH/CTE` no fluxo compartilhado já suportado pelo parser/executor; o detector barato agora tambem marca esse cabeçalho e a trilha TDD cobre parsing e runtime de CTE não-recursiva no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `RETURNING` no fluxo DML já suportado por parser/runtime; o detector barato agora tambem marca essa clausula e a trilha TDD cobre parsing e runtime de `INSERT`/`UPDATE`/`DELETE ... RETURNING` no modo `Auto`.
- Incremento desta sessão: `SqlDialect.Auto` passou a expor tambem `ORDER BY ... NULLS FIRST/LAST` no fluxo compartilhado já suportado pelo parser/executor; o detector barato agora tambem marca esse modificador e a trilha TDD cobre parsing e runtime da ordenação explícita de `NULL` no modo `Auto`.
- TODO: expandir `SqlSyntaxDetector` além da fatia atual de paginação/`ROWNUM`/marcadores compartilhados ja cobertos (`identidade`, `concatenacao`, `sequence`, JSON, temporal, agregacao textual, rowcount, comparadores e helpers condicionais/nulos), cobrindo apenas equivalências cross-dialect de alto retorno realmente consumidas.
- TODO: expandir `DialectNormalizer` além da primeira AST canônica de paginação para novos nós compartilhados somente quando houver contrato claro de execução comum.
- TODO: estender a mesma prova de equivalência TDD já fechada para paginação (`TOP`, `LIMIT`, `FETCH FIRST`, `ROWNUM`) às próximas famílias compartilhadas que entrarem no modo `Auto`.
- TODO: impedir que `SqlDialect.Auto` introduza branches sintáticos no executor; qualquer diferença nova deve ser resolvida antes da execução.

### 1.3 Executor SQL

#### 1.3.1 Pipeline de execução

- Implementação estimada: **69%**.
- Fluxo macro: parse → validação → execução no estado em memória → materialização de resultado.
- Track global de alinhamento de runtime estimado em ~55%, com evolução incremental por contracts de dialeto.
- Recalibrado por evidências de código: executor AST, estratégias de mutação por dialeto e ampla suíte `*StrategyTests`/`*GapTests` por provider.
- Tratamento de execução orientado por semântica do dialeto escolhido.
- Retorno previsível para facilitar asserts em testes.
- TODO: consolidar os pontos restantes de dispatch/estratégia que ainda escapam do pipeline shared, reduzindo branches residuais fora do contrato dirigido por capability do dialeto.
- TODO: ampliar o pipeline comum para cobrir também lacunas de execução avançada por family (`PIVOT` avançado, JSON tabular, mutações multi-tabela), sem reintroduzir atalhos por provider.

#### 1.3.2 Operações comuns suportadas

- Implementação estimada: **93%**.
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
- Incremento desta sessão: decisões de `UPDATE/DELETE ... JOIN/FROM/USING` e a semântica de rowcount de `INSERT ... ON DUPLICATE KEY UPDATE` passaram a sair do contrato explícito do dialeto, em vez de depender de branches centrais por nome de provider.
- Incremento desta sessão: o executor compartilhado de `PIVOT` passou a reutilizar a mesma trilha de agregação comum para `SUM`, `MIN`, `MAX` e `AVG`, corrigindo também a semântica de `COUNT(expr)` para ignorar `NULL` e removendo o retorno artificial de `0` para `SUM` em bucket vazio.
- TODO: completar no executor a matriz de agregadores avançados de `PIVOT` para os dialetos que já declaram a cláusula (`SQL Server`, `SqlAzure`, `Oracle`), cobrindo funções além do conjunto comum `COUNT/SUM/MIN/MAX/AVG` quando houver necessidade real por banco.
- TODO: expandir a trilha shared de `UNPIVOT` para além de `SQL Server/SqlAzure`, mantendo gate por capability do dialeto nos bancos que suportam essa família de forma nativa.

#### 1.3.3 Resultados e consistência

- Implementação estimada: **90%**.
- Entrega de resultados em formatos esperados por consumidores ADO.NET.
- Coerência entre operação executada e estado final da base simulada.
- Comportamento determinístico para repetição do mesmo script.
- Hardening recente reforçou previsibilidade de regressão com foco em mensagens de erro não suportado e consistência de diagnóstico.
- Checklist operacional confirma padronização de `SqlUnsupported.ForDialect(...)` no runtime para fluxos não suportados.
- Hardening recente também consolidou semântica ordered-set para agregações textuais com cobertura de ordenação `ASC/DESC`, ordenação composta, `DISTINCT + WITHIN GROUP` e `LISTAGG` sem separador explícito nos dialetos suportados.
- TODO: ampliar a malha de consistência para batches mistos com `RETURNING`/`OUTPUT`/rowcount/trigger, garantindo que resultado materializado e estado final permaneçam coerentes no mesmo script.
- TODO: registrar no backlog diferenças conhecidas de materialização por provider quando o mock optar por subset explícito em vez de reproduzir todo o contrato do banco real.

#### 1.3.4 Particionamento de tabelas (avaliação)

- Implementação estimada: **35%**.
- **Já implementado:**
  - metadata de partição em memória para o subset do MySQL;
  - `PARTITION BY RANGE` e `PARTITION BY LIST` por ano;
  - `INSERT ... PARTITION (...)`;
  - roteamento automático de `INSERT` para partição conhecida;
  - leitura explícita com `FROM ... PARTITION (...)`;
  - pruning seguro por igualdade, `IN (...)`, `BETWEEN` e `OR`;
  - pruning por `YEAR(col)` e `EXTRACT(YEAR FROM col)`;
  - round-trip de snapshot preservando a metadata de partição.
- **A implementar:**
  - pruning mais amplo fora do subset seguro de ano;
  - suporte a chaves e expressões de partição além de `YEAR` e `EXTRACT`;
  - roteamento/pruning para outros providers;
  - DDL avançado de partições;
  - manutenção de partições em cenários de retenção/arquivamento.
- **Recomendação:** sim, vale manter o item no backlog como feature incremental para cenários de teste com alto volume e consultas por faixa.
- **Ganho esperado:**
  - redução de custo em varreduras quando filtros batem na chave de partição;
  - cenários de retenção e arquivamento mais realistas;
  - maior fidelidade para workloads multi-tenant e time-series;
  - testes de regressão de plano e estratégia mais próximos de bancos reais.
- **Risco/observação:** manter o subset pequeno para não aumentar a complexidade do executor antes de fechar gaps críticos já priorizados.

### 1.4 API fluente

#### 1.4.1 Definição de schema por código

- Implementação estimada: **86%**.
- Criação declarativa/programática de estruturas.
- Reduz dependência de scripts SQL longos para setup inicial.
- Facilita reuso de cenários entre suítes.
- TODO: expandir a API fluente para cobrir também `View`, `Sequence`, `Index` e metadados de trigger sem obrigar fallback para SQL textual em setups frequentes.

#### 1.4.2 Seed de dados

- Implementação estimada: **84%**.
- Carga inicial de registros para cenários controlados.
- Apoia testes de leitura, paginação e filtros complexos.
- Permite criar massas pequenas e objetivas por caso de teste.
- TODO: adicionar helpers de seed guiados por dialeto para identidade/sequence, JSON, valores temporais e defaults calculados, reduzindo setup manual repetitivo.

#### 1.4.3 Composição de cenários

- Implementação estimada: **82%**.
- Encadeamento de passos de inicialização.
- Uso de builders/factories de contexto de teste.
- Legibilidade maior para times de aplicação.
- TODO: materializar cenários reutilizáveis de transação/savepoint/tabela temporária/trigger em builders compartilhados, reduzindo boilerplate cross-provider nas suites consumidoras.

#### 1.4.4 Snapshot e replay de schema

- Implementação estimada: **100%**.
- Objetivo: capturar schema real de uma conexao ADO.NET e reproduzi-lo no mock sem reescrita manual de setup.
- Deve servir tanto para bootstrap de suite quanto para congelar fixtures versionaveis em JSON.
- Incremento desta sessão: a primeira fatia de `SchemaSnapshot` já expõe `Export(connection|db)`, `ToJson()`, `Load(json)` e `ApplyTo(DbMock)`, cobrindo exportação e replay estrutural do subset básico de `tables` e `columns`.
- Incremento desta sessão: o snapshot já preserva metadados essenciais de coluna (`DbType`, `nullable`, `size`, `decimalPlaces`, `identity`, `defaultValue`, `enumValues`) e `NextIdentity` por tabela, com round-trip JSON em TDD.
- Incremento desta sessão: o replay atual substitui o estado estrutural anterior do `DbMock` de forma determinística antes de recriar o schema exportado, evitando drift residual entre fixtures.
- Incremento desta sessão: `SchemaSnapshot` passou a cobrir tambem `views` e `sequences`, persistindo `RawSql` da view e estado estrutural/corrente da sequence (`start`, `increment`, `currentValue`) para replay determinístico.
- Incremento desta sessão: `SchemaSnapshot` passou a cobrir tambem `primary key`, `indexes` e `foreign keys`, reaplicando a estrutura na ordem correta (tabelas -> PK/indices -> FKs -> views -> sequences) para evitar referências quebradas no replay.
- Incremento desta sessão: a conexão agora expõe atalhos públicos (`ExportSchemaSnapshot`, `ExportSchemaSnapshotJson`, `ImportSchemaSnapshot`) para consumir o snapshot sem acoplamento direto ao `DbMock`, com round-trip coberto em TDD.
- Incremento desta sessão: `SchemaSnapshot` tambem já suporta persistência versionável em arquivo (`SaveToFile`, `LoadFromFile`) e replay direto por caminho para bootstrap de fixture sem passar manualmente por string JSON.
- Incremento desta sessão: a conexão agora expõe atalhos file-based (`ExportSchemaSnapshotToFile`, `ImportSchemaSnapshotFromFile`), com round-trip em arquivo coberto em TDD no provider SQLite.
- Incremento desta sessão: `SchemaSnapshot` passou a preservar tambem assinaturas de `procedure` (`required in`, `optional in`, `out` e `return`, incluindo valores default), com replay estrutural coberto em TDD.
- Incremento desta sessão: o replay agora tem cobertura TDD para multi-schema (`tables`, `views`, `sequences` e `procedures` em schemas distintos), reduzindo risco de fixture parcial quando o banco simulado usa mais de um schema.
- Incremento desta sessão: `foreign keys` passaram a preservar tambem o schema da tabela referenciada, com replay cross-schema coberto em TDD para evitar perda silenciosa de relacionamento ao exportar fixtures multi-schema.
- Incremento desta sessão: a importação via conexão passou a realinhar `Database` quando o schema anteriormente selecionado deixa de existir após o replay, evitando que a conexão fique apontando para um schema removido.
- Incremento desta sessão: `SchemaSnapshot` agora expõe gate explícito de compatibilidade por `dialect/version` (`IsCompatibleWith` e `EnsureCompatibleWith`), e a conexão ganhou import estrito opcional para bloquear replay em destino incompatível antes de alterar o estado.
- Incremento desta sessão: a API orientada a snapshot ficou simétrica com a da conexão, com `ApplyTo(DbConnectionMockBase)` e loaders estáticos para conexão (`Load(..., connection)` / `LoadFromFile(..., connection)`), evitando reserialização desnecessária no bootstrap de fixture.
- Incremento desta sessão: o mesmo gate estrito de compatibilidade agora tambem cobre o caminho `DbMock` puro (`ApplyTo(db, ensureCompatibility)` / `Load(..., db, ensureCompatibility)`), mantendo consistencia entre as superfícies de replay.
- Incremento desta sessão: `SchemaSnapshot` agora expõe fingerprint estável e comparação direta contra `snapshot`/`DbMock`/`connection`, permitindo detectar drift estrutural objetivo de fixture sem inspeção manual do JSON.
- Incremento desta sessão: a comparação agora também retorna relatório estruturado de drift (`CompareTo(...)` + `SchemaSnapshotComparison.ToText()`), tornando divergências de schema anexáveis em log/issue sem diff manual do arquivo JSON.
- Incremento desta sessão: o subset suportado do `SchemaSnapshot` ficou explicitado em código/documentação via `SchemaSnapshotSupportProfile` e [schema-snapshot.md](/c:/Projects/DbSqlLikeMem/docs/features-backlog/schema-snapshot.md), fechando o escopo funcional do item com gate explícito do que entra e do que fica fora.
- Incremento desta sessão: a mesma descrição do subset suportado também ficou acessível direto pela conexão (`GetSchemaSnapshotSupportProfile()` / `GetSchemaSnapshotSupportProfileText()`), mantendo a superfície pública simétrica com os helpers de export/import.
- Incremento desta sessão: a regressão end-to-end do subset suportado agora valida export -> replay -> reexport sem drift estrutural, usando `CompareTo(...)` e fingerprint para confirmar equivalência canônica.

#### 1.4.5 Expansão de metadata avançada de snapshot

- Implementação estimada: **0%**.
- Objetivo: cobrir metadata e objetos executáveis intencionalmente fora do subset estrutural concluído em `1.4.4`.
- Escopo futuro: `check constraints`, defaults computados por expressão, geradores de coluna computada, corpos de `trigger`, corpos de `procedure` e demais objetos programáveis não-estruturais.

### 1.5 Diagnóstico e observabilidade da execução

#### 1.5.1 Plano de execução mock

- Implementação estimada: **48%**.
- Geração de plano sintético para análise de comportamento da query.
- Visibilidade de entradas da execução e custo estimado.
- Suporte a testes que verificam diagnóstico e não só resultado.
- Incremento desta sessão: o execution plan passou a cobrir também a primeira fatia de DML (`INSERT`, `UPDATE` e `DELETE`) no fluxo non-query, reutilizando a mesma superfície pública de `LastExecutionPlan` sem custo no parser/runtime fora da própria mutação.
- Incremento desta sessão: a suíte SQLite agora valida emissão de plano para `INSERT`, `UPDATE` e `DELETE`, incluindo alvo, filtro/SET básicos, linhas afetadas e disclaimer de performance.
- TODO: expandir execution plan além de `SELECT`/`UNION` para DML, batches e pontos de trigger, com warnings e contexto operacional suficientes para diagnóstico de regressão.

#### 1.5.2 Métricas de runtime

- Implementação estimada: **77%**.
- Métricas disponíveis: `EstimatedCost`, `InputTables`, `EstimatedRowsRead`, `ActualRows`, `SelectivityPct`, `RowsPerMs`, `ElapsedMs`.
- Recalibrado com base na presença efetiva das métricas e nos testes de plano/formatter existentes no código.
- Permite validar cenários de seletividade e custo relativo.
- Facilita comparação entre estratégias de consulta em testes.
- TODO: consolidar contrato estável para métricas de mutação, batch, trigger e transação, mantendo separação explícita entre telemetria diagnóstica e semântica funcional do executor.

#### 1.5.3 Histórico por conexão

- Implementação estimada: **87%**.
- `LastExecutionPlan`: referência ao último plano executado.
- `LastExecutionPlans`: trilha dos planos da sessão de conexão.
- Útil para auditoria de execução em cenários multi-etapa.
- TODO: adicionar política configurável de retenção/limpeza e ampliar o histórico para mutações e batches, não só planos textuais de leitura.

#### 1.5.4 Uso prático no backlog

- Implementação estimada: **72%**.
- Ajuda a mapear comandos mais custosos no ambiente de testes.
- Apoia priorização de melhorias no parser/executor.
- Oferece material para diagnósticos reprodutíveis em issues.
- TODO: ligar snapshots/telemetria do plano de execução diretamente aos itens do backlog e às issues de regressão, para transformar observabilidade em critério objetivo de priorização.

#### 1.5.5 Debug trace de execução

- Implementação estimada: **90%**.
- Diferente do execution plan textual/JSON: deve mostrar o rastro real dos operadores executados no runtime do mock.
- Precisa expor volume de linhas de entrada/saída, tempo relativo e detalhes suficientes para diagnosticar interpretação incorreta da query.
- Incremento desta sessão: a primeira fatia da feature já expõe `DebugSql(string sql)` na conexão e os contratos públicos `QueryDebugTrace`/`QueryDebugTraceStep`, sem conflitar com `LastExecutionPlan`.
- Incremento desta sessão: o executor já registra um trace mínimo sob demanda para `SELECT`/`UNION`, cobrindo etapas básicas como `TableScan`, `Join`, `Filter`, `Group`, `Having`, `Project`, `Sort`, `Limit`, `UnionInputs` e `UnionCombine`.
- Incremento desta sessão: a cobertura inicial de regressão foi ligada à suíte SQLite para validar a API `DebugSql` e a presença dos operadores básicos de leitura.
- Incremento desta sessão: `DebugSql` agora preserva o último trace mesmo quando precisa abrir/fechar a conexão automaticamente, evitando perda do artefato logo após a chamada.
- Incremento desta sessão: `UNION` passou a publicar apenas o trace consolidado da operação em vez de vazar traces internos de cada `SELECT`, reduzindo ruído sem custo extra no caminho normal.
- Incremento desta sessão: a conexão agora também expõe `DebugSqlBatch(string sql)` para multi-statements, reaproveitando a captura existente e devolvendo todos os traces da execução reader em ordem.
- Incremento desta sessão: a malha TDD inicial do debugger cobre agora `SELECT`, retenção após auto-close, `UNION` consolidado e batch com múltiplos statements.
- Incremento desta sessão: cada `QueryDebugTrace` retornado pelo batch agora carrega também contexto do statement (`StatementIndex` e `SqlText`), deixando o resultado autoexplicativo em execuções multi-statement.
- Incremento desta sessão: a feature agora também possui formatter textual dedicado (`QueryDebugTraceFormatter`) e atalhos na conexão (`DebugSqlText`/`DebugSqlBatchText`) para inspeção direta sem montagem manual de saída.
- Incremento desta sessão: o formatter do debugger agora também expõe JSON estruturado (`FormatJson`/`FormatBatchJson`) e atalhos na conexão (`DebugSqlJson`/`DebugSqlBatchJson`), preparando consumo automatizado futuro sem serialização ad-hoc.
- Incremento desta sessão: o contrato do trace passou a expor agregados prontos de observabilidade (`TotalExecutionTime`, `MaxInputRows`, `MaxOutputRows`, `OperatorSignature`), e os formatters textual/JSON agora refletem esse resumo sem recomputacao no chamador.
- Incremento desta sessão: o formatter de batch passou a expor tambem resumo consolidado do lote (`TotalStepCount`, tempo total, maiores volumes e assinatura agregada de operadores), facilitando leitura e automacao sem reprocessamento externo.
- Incremento desta sessão: o resumo de batch agora inclui tambem contagens agregadas por tipo de query e por operador (`QueryTypes`/`OperatorCounts`), reduzindo trabalho manual para diagnostico e futura integracao com CI.
- Incremento desta sessão: o resumo de batch agora identifica tambem o statement mais caro e o de maior volume (`SlowestStatementIndex`/`WidestStatementIndex`), com desempate estavel por indice para consumo automatizado.
- Incremento desta sessão: o resumo de batch passou a expor tambem o SQL associado ao statement mais caro e ao de maior volume (`SlowestStatementSql`/`WidestStatementSql`), eliminando lookup manual adicional no cliente.
- Incremento desta sessão: a visualizacao consolidada do batch agora entrega diretamente indice e SQL dos statements criticos, reduzindo o passo manual de correlacionar resumo agregado com a lista detalhada de traces.
- Incremento desta sessão: o trace individual passou a expor tambem `OperatorCounts`, e os formatters textual/JSON agora entregam a distribuicao por operador sem recontagem no chamador.
- Incremento desta sessão: o trace individual agora tambem aponta o operador mais caro e o de maior volume (`SlowestOperator`/`WidestOperator`), facilitando diagnostico rapido sem inspecionar todos os passos manualmente.
- Incremento desta sessão: a leitura rapida do trace individual agora fica simetrica ao resumo de batch, destacando tanto distribuicao (`OperatorCounts`) quanto hotspots principais do fluxo executado.
- Incremento desta sessão: o trace individual agora tambem explicita o primeiro e o ultimo operador do fluxo (`FirstOperator`/`LastOperator`), deixando o inicio/fim da pipeline visivel sem depender apenas da assinatura completa.
- Incremento desta sessão: o trace individual passou a expor tambem os indices dos hotspots (`SlowestStepIndex`/`WidestStepIndex`), permitindo localizar o passo critico diretamente sem percorrer a lista inteira.
- Incremento desta sessão: o resumo individual do trace agora combina operador e indice do hotspot, deixando a navegacao ate o passo critico direta nas saidas textual e JSON.
- Incremento desta sessão: o trace individual agora tambem expõe os `Details` do passo mais lento e do mais largo (`SlowestStepDetails`/`WidestStepDetails`), reduzindo a necessidade de abrir manualmente a lista detalhada.
- Incremento desta sessão: o trace individual agora tambem cobre os extremos minimos (`Fastest*` e `Narrowest*`), fechando a leitura rapida dos extremos de custo e volume sem depender de analise manual.
- Incremento desta sessão: o resumo individual do trace agora cobre os dois extremos completos do fluxo (mais caro/mais barato e mais largo/mais estreito), com operador, indice e detalhes prontos nas saidas textual e JSON.
- Incremento desta sessão: o resumo consolidado de batch agora expõe tambem os statements mais rapido e mais estreito (`FastestStatement*`/`NarrowestStatement*`), fechando a visao dos quatro extremos do lote com selecao estavel e sem ordenacoes LINQ extras.
- Incremento desta sessão: a integracao publica do debugger em batch agora tambem possui regressao dedicada para `DebugSqlBatchText` e `DebugSqlBatchJson`, cobrindo os agregados novos diretamente na API da conexao.
- Incremento desta sessão: os `Details` dos operadores de leitura ficaram mais ricos para diagnostico (`Project`, `Sort`, `Group` e `Join` agora carregam itens/chaves/predicado relevantes), sem alterar o caminho normal fora do modo debug.
- Incremento desta sessão: `UNION` e `DISTINCT` agora tambem expõem detalhes operacionais mais explicitos (`parts`, segmentos `ALL`/`DISTINCT`, modo consolidado e contagem de colunas projetadas), e a suíte SQLite passou a cobrir um fluxo agrupado com `GROUP`/`HAVING`/`DISTINCT`.
- Incremento desta sessão: a materializacao de `QueryDebugTrace` foi reescrita para agregacao em passagem unica, reduzindo ordenacoes e enumeracoes LINQ repetidas sem mudar o contrato observavel de desempate dos hotspots/extremos.
- Incremento desta sessão: o formatter de batch agora consolida todos os agregados em um resumo interno unico, evitando multiplas passagens sobre os traces e preservando o mesmo desempate estavel para os extremos do lote.
- Incremento desta sessão: a conexao agora tem politica simples de retencao e limpeza para traces (`DebugTraceRetentionLimit` e `ClearDebugTraces()`), mantendo por padrao a compatibilidade e permitindo limitar memoria em cenarios de batch/debug intensivo.
- Incremento desta sessão: cada nova captura externa de debug agora reinicia o historico anterior antes da execucao, alinhando `LastDebugTrace`/`LastDebugTraces` com a semantica documentada de “ultima execucao” e evitando acumulacao indevida entre chamadas.
- Incremento desta sessão: a conexao agora pode exportar o snapshot atual do debugger sem reexecutar SQL (`GetDebugTraceSnapshot`, `GetDebugTraceSnapshotText`, `GetDebugTraceSnapshotJson`), facilitando anexar artefatos a regressões e issues.
- Incremento desta sessão: a API publica do debugger ficou simetrica tambem para o ultimo trace retido (`GetLastDebugTraceSnapshot`, `GetLastDebugTraceSnapshotText`, `GetLastDebugTraceSnapshotJson`), cobrindo tanto inspeção pontual quanto exportacao do lote inteiro sem nova execucao.
- Incremento desta sessão: a camada publica de snapshot agora tem comportamento vazio coberto por regressao, incluindo lote vazio formatavel e erro explicito para exportacao textual/JSON do ultimo trace quando nada foi retido.
- Incremento desta sessão: a superficie publica do debugger agora tambem oferece leitura nao-excepcional do ultimo trace (`TryGetLastDebugTraceSnapshot`), fechando a ergonomia de consumo para cenarios interativos e automacao defensiva.
- TODO: aprofundar a instrumentação do executor para registrar detalhes mais ricos por operador e reduzir passos ainda genéricos/agrupados.
- TODO: manter o trace em memória por conexão/comando, com política clara de retenção e limpeza.
- TODO: conectar o trace aos cenários de regressão para que debug de parser/executor não dependa apenas do plano sintético final.

### 1.6 Riscos técnicos e mitigação no núcleo

#### 1.6.1 Risco: divergência entre mock e banco real

- Implementação estimada: **60%**.
- Mitigar com smoke tests cross-dialect para consultas críticas.
- Catalogar explicitamente as diferenças conhecidas em documentação de compatibilidade.
- TODO: manter um catálogo vivo de diferenças conhecidas por provider/versão e conectá-lo à matriz de compatibilidade e aos snapshots cross-dialect.

#### 1.6.2 Risco: regressão em evolução do parser

- Implementação estimada: **70%**.
- Exigir cenários de regressão para cada correção de sintaxe.
- Priorizar suíte incremental por dialeto para reduzir efeito colateral.
- TODO: fechar o contrato operacional de regressão exigindo sempre teste positivo, teste negativo e prova de não regressão em dialetos correlatos antes de marcar um gap como concluído.

#### 1.6.3 Risco: falsa percepção de performance

- Implementação estimada: **74%**.
- Reforçar que métricas do mock são diagnósticas e relativas.
- Evitar decisões de tuning de produção baseadas apenas em execução em memória.
- Incremento desta sessão: plano de execução textual/JSON passou a emitir `PerformanceDisclaimer` explícito informando que métricas do mock são relativas e não devem orientar benchmark/tuning de produção.
- Incremento desta sessão: regressões de execution plan foram atualizadas em todos os bancos principais (SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2) para exigir presença do disclaimer no output.
- Incremento desta sessão: disclaimer de performance foi migrado para camada de recursos (`SqlExecutionPlanMessages` + `.resx` multilíngue), removendo texto hardcoded e mantendo alinhamento de i18n entre plano textual e payload JSON.
- Incremento desta sessão: regressão dedicada foi adicionada no formatter para garantir que planos `UNION` também emitam o disclaimer de performance localizado, evitando lacunas entre tipos de plano textual.
- Incremento desta sessão: documentação do pacote core (`src/DbSqlLikeMem/README.md`) foi reforçada com guidance explícito para não usar métricas/tempos do mock como benchmark de produção.
- Incremento desta sessão: regressões de execution plan nos bancos principais passaram a validar não só a presença do campo de disclaimer, mas também a mensagem localizada emitida por recursos.
- Incremento desta sessão: guia de compatibilidade (`docs/wiki/pages/Providers-and-Compatibility.md`) passou a explicitar em EN/PT-BR que métricas de execution plan no mock são diagnósticas/relativas e não substituem benchmark de produção.
- Incremento desta sessão: execution plan textual/JSON passou a incluir `mockRuntimeContext` com `simulatedLatencyMs`, `dropProbability`, `threadSafe` e flag explícita de métricas relativas, reduzindo interpretação ambígua de `elapsed`/`rowsPerMs` como throughput real.
- Incremento desta sessão: execution plan também passou a sinalizar `mockRuntimePerturbationActive` quando há latência/falha simulada configurada, deixando explícito que comparações diretas de tempo entre cenários estão contaminadas por perturbação artificial.
- Incremento desta sessão: a trilha comparativa de benchmark foi materializada em `benchmark/DbSqlLikeMem.Benchmarks`, comparando o runtime do `DbSqlLikeMem` com bancos reais/nativos a partir do mesmo catálogo de cenários.
- Incremento desta sessão: a infraestrutura comparativa foi padronizada em `Testcontainers` para `MySQL`, `SQL Server`, `PostgreSQL/Npgsql`, `Oracle` e `DB2`, com modo `preprovisioned` para execuções controladas sem custo de boot a cada rodada e `Sqlite` usando baseline nativa embedded.
- Incremento desta sessão: os artefatos objetivos dessa trilha passaram a ser publicados na wiki espelhada em `docs/Wiki/performance-matrix.md`, `docs/Wiki/performance-matrix-app-specific.md` e `docs/Wiki/BenchmarkResults`, mantendo histórico versionado por rodada de medição.
- Incremento desta sessão: a frente de performance dos testes foi enxugada no caminho quente e passou a usar flags específicas por suíte para manter o benchmark rápido como padrão, evitando que comparação com container entre por acidente nas execuções normais.
- TODO: propagar o disclaimer de performance para todos os pontos de consumo de telemetria/planos e manter a documentação de entrada alinhada sempre que novas métricas forem expostas.
- TODO: enriquecer os artefatos comparativos da wiki com `footprint`, notas de limitação operacional e demais metadados de custo que ainda não aparecem de forma consolidada em todas as matrizes.

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
- TODO: fechar paridade remanescente de comportamento entre command/batch/async/cancelamento/lifecycle nos providers que ainda dependem de diferenças estruturais fora do núcleo compartilhado.

#### 2.1.2 Integração com fluxo de testes

- Implementação estimada: **85%**.
- Injeção de conexão mock em serviços, repositórios e UoW.
- Evita dependência de infraestrutura externa em testes rápidos.
- Facilita execução local e em pipeline compartilhado.
- TODO: publicar e manter exemplos mínimos de integração com DI/UoW/transação por provider, reduzindo variação de setup entre projetos consumidores.

#### 2.1.3 Benefícios de arquitetura

- Implementação estimada: **100%**.
- Camada de acesso mais desacoplada de banco físico.
- Melhor separação entre teste de regra e teste de infraestrutura.
- Menor custo de manutenção de ambientes dedicados.
- Incremento desta sessão: pipeline ADO.NET de execução passou a suportar `DROP TABLE` via AST dedicado (`SqlDropTableQuery`) no núcleo do parser, reduzindo dependência de parsing manual por string.
- Incremento desta sessão: estratégia compartilhada de execução (`DbSelectIntoAndInsertSelectStrategies`) ganhou caminho unificado para `DROP TABLE`, centralizando regra de negócio e reduzindo duplicação entre providers.
- Incremento desta sessão: infraestrutura de banco/conexão recebeu operações explícitas de remoção de tabela permanente, temporária de conexão e temporária global, melhorando organização do ciclo de vida dos artefatos DDL.
- Incremento desta sessão: command mocks de SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2 passaram a despachar `DROP TABLE` no mesmo fluxo arquitetural de comandos AST (NonQuery/DataReader), reduzindo branches especiais e melhorando previsibilidade/performance de manutenção.
- Incremento desta sessão: `ExecuteNonQuery` dos seis providers principais foi alinhado para usar o dispatcher compartilhado `ExecuteParsedNonQuery(...)`, removendo `switch` duplicado por provider e consolidando regras de merge/union por opção de dialeto.
- Incremento desta sessão: contrato `ICommandExecutionPipeline` e implementação base `CommandExecutionPipeline` foram introduzidos no núcleo para concentrar o fluxo template de `ExecuteNonQuery` (split de statements, tx-control, hooks especiais e dispatch AST), com adoção em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: pipeline de non-query evoluiu para cadeia explícita de handlers (`TransactionControlNonQueryCommandHandler`, `SpecialNonQueryCommandHandler` e cadeia AST dedicada) com contexto compartilhado, iniciando separação formal por responsabilidades.
- Incremento desta sessão: cadeia AST foi decomposta em handlers especializados (`AstDmlNonQueryCommandHandler`, `AstDdlNonQueryCommandHandler`, `AstReadGuardNonQueryCommandHandler`, `AstUnsupportedNonQueryCommandHandler`) com parse compartilhado por contexto, reduzindo acoplamento e custo de evolução por tipo de comando.
- Incremento desta sessão: handlers comuns de `CALL` e `CREATE TABLE` foram extraídos para o pipeline base (`CallNonQueryCommandHandler` e `CreateTableAsSelectNonQueryCommandHandler`), removendo duplicação entre providers e reduzindo branches específicos por comando.
- Incremento desta sessão: pipeline passou a reutilizar cadeia padrão estática de handlers e a validar SQL uma única vez por statement antes do parse compartilhado em contexto, reduzindo overhead de execução e melhorando previsibilidade de performance.
- Incremento desta sessão: `Sqlite`, `MySql` e `Db2` removeram atalhos DDL redundantes de `ExecuteNonQuery` (create temp/view/drop view), passando a depender do mesmo caminho AST/pipeline compartilhado dos demais providers; `SpecialCommand` ficou focado em exceções reais de dialeto (como `RETURNING INTO` no Oracle).
- Incremento desta sessão: telemetria de pipeline foi adicionada em `DbMetrics` para `ExecuteNonQuery` (contagem de statements processados, hits por handler e parse cache hit/miss por statement), criando base objetiva para avaliar custo de pipeline e risco de falsa percepção de performance.
- Incremento desta sessão: telemetria foi ampliada com latência acumulada por handler (`NonQueryHandlerElapsedTicks`) no caminho efetivamente tratado do pipeline, permitindo leitura objetiva de custo por estágio e fechamento do item arquitetural.
- Incremento desta sessão: pipeline de non-query passou a telemetrar também falhas por handler (`NonQueryHandlerFailures`), exceções totais de fluxo (`NonQueryExceptions`) e statements não tratados (`NonQueryUnhandledStatements`), aumentando rastreabilidade de contrato e reduzindo diagnóstico subjetivo de gargalo/erro.
- Incremento desta sessão: criação do runner compartilhado `ExecuteNonQueryWithPipeline(...)` no núcleo, removendo duplicação de inicialização de pipeline/opções em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: `ExecuteReader` também recebeu prelude compartilhado (`TryHandleExecuteReaderPrelude`) para stored procedure, split de statements e caso único de `CALL`, com adoção nos seis providers principais e preservação de diferenças de dialeto (ex.: normalização de SQL no MySQL).
- Incremento desta sessão: parsing/execução de comandos transacionais comuns foi centralizado no helper `TryExecuteStandardTransactionControl(...)`, com wrappers nos seis providers e preservação de comportamento específico do SQL Server para `RELEASE SAVEPOINT` (no-op).
- Incremento desta sessão: loop interno de `ExecuteReader` passou a delegar o tratamento comum de `tx-control` + `CALL` ao helper compartilhado `TryHandleReaderControlCommand(...)`, reduzindo duplicação estrutural em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: finalização comum de `ExecuteReader` (erro sem `SELECT` + atualização de `Metrics.Selects`) foi centralizada no helper `FinalizeReaderExecution(...)`, removendo repetição cross-provider e reduzindo risco de divergência de contrato.
- Incremento desta sessão: telemetria compartilhada de `ExecuteReader` foi adicionada em `DbMetrics` (statements processados, controles transacionais, `CALL`, procedures, quantidade de result tables, linhas retornadas e ocorrência de `ExecuteReader` sem `SELECT`), elevando observabilidade arquitetural cross-provider sem duplicação por comando mock.
- Incremento desta sessão: despacho AST de `ExecuteReader` foi unificado no helper compartilhado `DispatchParsedReaderQuery(...)`, removendo `switch` duplicado em SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2, com preservação dos comportamentos específicos (`RETURNING`, `OUTPUT`, `MERGE` e estratégias de `UPDATE/DELETE` por dialeto).
- Incremento desta sessão: dispatcher compartilhado de reader passou a telemetrar `ReaderQueryTypeHits` por tipo AST no `DbMetrics`, permitindo comparar distribuição real de comandos por provider sem instrumentação duplicada em cada command mock.
- Incremento desta sessão: coleta de result sets de `DbDataReader` em batches foi unificada no helper `BatchReaderResultCollector.CollectAllResultSets(...)`, removendo duplicação de hidratação tabular em SQLite, MySQL, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: execução resiliente de comandos batch (`ExecuteReader` com fallback para `ExecuteNonQuery` em ausência de `SELECT`) foi centralizada no helper `BatchCommandExecutionRunner.ExecuteIntoTables(...)`, consolidando contrato cross-provider e reduzindo divergência de tratamento de erro em todos os bancos.
- Incremento desta sessão: `DbMetrics` passou a expor telemetria de batch (`BatchNonQueryCommands`, `BatchReaderCommands`, `BatchReaderFallbackToNonQuery`) com instrumentação no runner compartilhado de batch e nos fluxos de `ExecuteNonQuery` de SQLite, MySQL, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: execução `ExecuteNonQuery` de batch foi consolidada no helper `BatchNonQueryExecutionRunner` (sync+async), padronizando telemetria por modo/tipo (`BatchCommandTypeHits`) e removendo duplicação de contadores/dispatch em todos os providers.
- Incremento desta sessão: runners compartilhados de batch passaram a telemetrar tempo acumulado por fase (`BatchPhaseElapsedTicks` para `reader`, `nonquery` e `fallback-nonquery`) e o batch MySQL foi alinhado para respeitar `CommandBehavior` na execução de reader, reduzindo divergência de contrato entre providers.
- Incremento desta sessão: materialização de `DbCommand` a partir de `DbBatchCommand` (`CommandText`, `CommandType`, `Timeout` e parâmetros) foi centralizada no helper `BatchCommandMaterializer.Apply(...)`, reduzindo duplicação estrutural em SQLite, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: execução de `ExecuteScalar` em batch foi unificada no helper `BatchScalarExecutionRunner.ExecuteFirstScalar(...)`, removendo repetição de seleção do primeiro comando e criação de comando executável em SQLite, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: telemetria de scalar em batch foi centralizada no runner compartilhado (`BatchScalarCommands`, `BatchCommandTypeHits` com prefixo `scalar:` e tempo em `BatchPhaseElapsedTicks["scalar"]`), incluindo o caminho assíncrono do MySQL via `ExecuteFirstScalarAsync(...)`.
- Incremento desta sessão: caminhos assíncronos de batch (`ExecuteNonQueryAsync`, `ExecuteDbDataReaderAsync` e `ExecuteScalarAsync`) foram alinhados para execução realmente assíncrona com runners compartilhados e cancelamento propagado em SQLite, MySQL, SQL Server, Npgsql, Oracle, Db2 e SQL Azure, removendo wrappers `Task.FromResult` e reduzindo divergência de contrato entre bancos.
- Incremento desta sessão: loops assíncronos repetidos de batch foram extraídos para `BatchAsyncExecutionRunner` (`ExecuteNonQueryCommandsAsync` e `ExecuteReaderCommandsAsync`) e adotados por todos os providers, reduzindo duplicação estrutural e consolidando um único template de execução cross-provider.
- Incremento desta sessão: criação/materialização de comandos batch por provider foi encapsulada em `CreateExecutableCommand(...)` nos mocks de SQLite, SQL Server, Npgsql, Oracle, Db2 e SQL Azure, eliminando repetição em caminhos sync/async/scalar e reduzindo pontos de divergência de manutenção.
- Incremento desta sessão: loops síncronos de batch também foram extraídos para `BatchSyncExecutionRunner` (`ExecuteNonQueryCommands` e `ExecuteReaderCommands`) e adotados em SQLite, SQL Server, Npgsql, Oracle, Db2 e SQL Azure, fechando a simetria arquitetural sync/async no núcleo.
- Incremento desta sessão: validação de conexão obrigatória foi centralizada em `BatchExecutionGuards.RequireConnection(...)` e o wrapping de `DbDataReader` final passou a ser provido pelos runners sync/async via factory, reduzindo boilerplate repetido e padronizando contrato de erro entre providers.
- Incremento desta sessão: criação de comandos materializados em batch foi generalizada no helper `BatchCommandFactory.Create(...)`, simplificando os factories por provider (`CreateExecutableCommand`) e removendo duplicação de wiring em SQLite, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: `MySqlBatchMock` passou a adotar `BatchCommandFactory.Create(...)` com estratégia de materialização customizável para preservar clone tipado de `MySqlParameter`, reduzindo divergência de implementação sem perder compatibilidade semântica do provider.
- Incremento desta sessão: `BatchCommandFactory` passou a instrumentar materialização de comandos com `BatchMaterializations`, `BatchCommandTypeHits` (`materialize:*`) e latência em `BatchPhaseElapsedTicks["materialization"]`, fornecendo telemetria objetiva de overhead dessa fase em todos os providers.
- Incremento desta sessão: coleta de resultados de reader em batch passou a retornar estatísticas (`BatchReaderCollectionStats`) e alimentar métricas de cardinalidade (`BatchResultTables`, `BatchRowsReturned`) diretamente no runner compartilhado, aumentando precisão de diagnóstico de throughput lógico entre providers.
- Incremento desta sessão: contrato de erro para batch sem conexão foi centralizado em `SqlExceptionMessages.BatchConnectionRequired()` (com recursos EN/PT) e aplicado no guard compartilhado (`BatchExecutionGuards`) e no fluxo de validação do MySQL, reduzindo risco de divergência de mensagem entre providers.
- Incremento desta sessão: runners/factories de batch passaram a telemetrar falhas por fase (`BatchPhaseFailures`) e exceções totais (`BatchExceptions`) nos caminhos `materialization`, `reader`, `fallback-nonquery`, `nonquery` e `scalar`, elevando capacidade de diagnóstico cross-provider sem instrumentação específica por banco.
- Incremento desta sessão: chaves de métrica/fase de batch foram centralizadas em `BatchMetricKeys` (prefixos de tipo e fases), eliminando strings literais duplicadas nos runners/factory e reduzindo risco de drift de instrumentação entre providers.
- Incremento desta sessão: recursos de localização para `BatchConnectionRequired` foram completados em todos os arquivos de idioma existentes (`de`, `es`, `fr`, `it`, além de `en`/`pt`), reduzindo fallback implícito de mensagem e aumentando consistência de contrato internacionalizado.
- Incremento desta sessão: runners de batch passaram a telemetrar execuções vazias por modo (`BatchEmptyNonQueryExecutions`, `BatchEmptyReaderExecutions`, `BatchEmptyScalarExecutions`), melhorando leitura de cenários “sem trabalho” que podem inflar percepção de performance em benchmarks locais.
- Incremento desta sessão: métricas de batch passaram a separar cancelamentos de exceções gerais (`BatchCancellations`, `BatchPhaseCancellations`) nos caminhos compartilhados de `materialization`, `reader`, `fallback-nonquery`, `nonquery` e `scalar`, refinando análise de resiliência e timeout/cancel cross-provider.
- Incremento desta sessão: contrato de validação “batch deve conter ao menos um comando” foi centralizado em `SqlExceptionMessages.BatchCommandsMustContainCommand()` (recursos EN/PT/DE/ES/FR/IT) e aplicado no fluxo MySQL, reduzindo string literal duplicada e fallback de localização.
- Incremento desta sessão: validação de estado do `MySqlBatchMock` (`IsValid`/`NeedsPrepare`) foi consolidada no método único `ValidateBatchState(...)`, reaproveitando `BatchExecutionGuards.RequireAtLeastOneCommand(...)` para reduzir duplicação de regras e risco de drift no contrato de pré-condição.
- Incremento desta sessão: validações restantes de comandos inválidos no `MySqlBatchMock` (`BatchCommandsMustNotContainNull` e `BatchCommandTextRequired`) foram migradas para `SqlExceptionMessages` com recursos multilíngues, removendo os últimos literais de erro no fluxo batch do provider.
- Incremento desta sessão: mensagens de estado de conexão (`BatchConnectionMustBeOpenCurrentState`) e limitação de `Prepare` no MySQL (`MySqlBatchPrepareOnlyTextSupported`) também foram centralizadas em `SqlExceptionMessages` com recursos para todos os idiomas do projeto, eliminando literais restantes no ciclo de vida de batch do provider.
- Incremento desta sessão: traduções de `de/es/fr/it` para as novas chaves de contrato batch foram efetivamente aplicadas (substituindo textos em inglês), reforçando consistência semântica da experiência internacionalizada em diagnósticos de erro.
- Incremento desta sessão: validação de estado de conexão aberta em batch foi consolidada em `BatchExecutionGuards.RequireOpenConnectionState(...)` e aplicada nos runners compartilhados (`BatchSyncExecutionRunner`, `BatchAsyncExecutionRunner`, `BatchScalarExecutionRunner`), garantindo contrato homogêneo em SQLite, MySQL, SQL Server, Npgsql, Oracle, Db2 e SQL Azure.
- Incremento desta sessão: telemetria/captura de falha-cancelamento por fase de batch foi extraída para helper compartilhado `BatchPhaseExecutionTelemetry` e adotada em `BatchCommandFactory`, `BatchNonQueryExecutionRunner` e `BatchScalarExecutionRunner`, reduzindo duplicação de `try/catch` e risco de divergência de instrumentação cross-provider.
- Incremento desta sessão: runners compartilhados de leitura batch (`BatchSyncExecutionRunner` e `BatchAsyncExecutionRunner`) passaram a pré-alocar capacidade de `List<TableResultMock>` com base no total de comandos, reduzindo realocações em cenários de múltiplos statements por lote.
- Fechamento desta sessão: após consolidação final do contrato cross-provider de batch e da telemetria arquitetural compartilhada, o item `2.1.3` e o roteiro A-E foram concluídos em `100%`.
- Diretrizes arquiteturais para evolução contínua:
  - `S` (Single Responsibility): separar claramente parsing, despacho de comando, execução e acesso a estado.
  - `O` (Open/Closed): novas capacidades SQL devem entrar por extensão (novas estratégias/handlers), sem aumentar `if/switch` centrais.
  - `L` (Liskov): contratos comuns entre providers devem manter semântica equivalente para o mesmo SQL suportado.
  - `I` (Interface Segregation): expor interfaces menores por papel (parser, dispatcher, executor, storage ops), evitando contratos monolíticos.
  - `D` (Dependency Inversion): alto nível (comando mock) deve depender de abstrações de despacho/execução, não de detalhes de provider.
- DDD (onde aplicável):
  - Tratar `DbMock`/`SchemaMock`/`TableMock` como núcleo de domínio técnico de persistência simulada.
  - Isolar regras de lifecycle transacional e DDL em serviços de domínio técnico (`application services`) para reduzir acoplamento com infraestrutura ADO.NET.
  - Delimitar bounded contexts em torno de `Parser`, `Execution`, `Provider Integration` e `Diagnostics`.
- Design Patterns (GoF e correlatos) aplicáveis ao item:
  - `Strategy`: seleção de execução por dialeto/comando sem branch excessivo.
  - `Command`: encapsular operações SQL parseadas em objetos executáveis (AST + executor).
  - `Factory Method/Abstract Factory`: criação de executores/dispatchers por provedor.
  - `Template Method`: fluxo padrão de `ExecuteNonQuery/ExecuteReader` com hooks por provider.
  - `Adapter`: compatibilização entre superfície ADO.NET/Dapper e motor interno.
  - `Chain of Responsibility` (opcional): pipeline de handlers DDL/DML para substituir sequência fixa de `if`.
  - `Facade`: ponto único simplificado para orchestration parser+executor+estado.
- Roteiro de melhorias do item (`2.1.3`) para seguirmos:
  - Etapa A - Dispatcher unificado por AST em todos os providers: **100%**.
  - Etapa B - Extração de contrato `ICommandExecutionPipeline` com Template Method base: **100%**.
  - Etapa C - Separação em handlers especializados (`DDL`, `DML`, `TxControl`, `ProcedureCall`): **100%**.
  - Etapa D - Telemetria arquitetural (contagem de branches, latência por handler, cache-hit de parse): **100%**.
  - Etapa E - Hardening cross-provider de contrato (regressões de semântica idêntica): **100%**.
- Andamento agregado do roteiro de implantação arquitetural (A-E): **100%**.

#### 2.1.4 Pipeline de interceptação ADO.NET

- Implementação estimada: **100%**.
- Nova trilha para compor comportamentos sobre `DbConnection`, `DbCommand` e transações sem substituir o engine em memória atual.
- Direção arquitetural: coexistência entre dois modos complementares:
  - interceptação de provider real para telemetria, inspeção de query, fault injection, simulação de latência e experimentos de resiliência;
  - uso do engine `DbSqlLikeMem` como provider/in-memory engine para testes determinísticos e validação SQL cross-dialect.
- Escopo inicial da abstração: wrapping composable sobre conexão/comando/transação com hooks explícitos de lifecycle e execução, preservando a superfície ADO.NET consumida por aplicações.
- Primeiros alvos de adoção planejados: `SqlClient`, `Npgsql`, `MySqlConnector`, `Sqlite` e também o próprio engine `DbSqlLikeMem` como destino opcional do pipeline.
- Benefícios esperados:
  - instrumentação extensível sem fork por provider;
  - menor custo para validar retries, timeouts e logging em cima de conexões reais;
  - trilha futura de integração com `DiagnosticListener`, `Activity` e OpenTelemetry;
  - possibilidade de reaproveitar a mesma cadeia composable tanto em testes de aplicação quanto em cenários híbridos com banco real.
- Incremento desta sessão: núcleo inicial do pipeline foi introduzido no core com `DbInterceptionPipeline.Wrap(...)`, contrato público `DbConnectionInterceptor`, wrappers `InterceptingDbConnection`/`InterceptingDbCommand`/`InterceptingDbTransaction`, hooks de `Open`/`Close`, início/commit/rollback de transação, criação de comando e interceptação sync/async de `ExecuteNonQuery`, `ExecuteScalar` e `ExecuteReader`, além de regressões contratuais no projeto base.
- Incremento desta sessão: o pacote base também passou a expor `DelegatingDbConnectionInterceptor` para composição leve por delegates e `RecordingDbConnectionInterceptor` para trilha diagnóstica em memória de eventos de conexão/comando/transação, ajudando a estabilizar o contrato inicial do pipeline antes dos adapters por provider.
- Incremento desta sessão: a adoção do pipeline foi conectada também às entradas já existentes do engine, com helper direto em `DbConnectionMockBase` (`Intercept(...)`) e sobrecargas da `DbMockConnectionFactory` para devolver conexão já encapsulada, reduzindo atrito para uso prático da nova trilha em testes atuais.
- Incremento desta sessão: a trilha também ganhou um interceptor concreto de resiliência (`FaultInjectionDbConnectionInterceptor`) para injeção determinística de latência/falha em conexão, comando e transação, validando um caso de uso central da proposta já no contrato inicial do core.
- Incremento desta sessão: o núcleo do pipeline também passou a oferecer `LoggingDbConnectionInterceptor` para emissão de eventos estruturados via `Action<string>`, cobrindo um caminho pragmático de observabilidade leve sem exigir integração imediata com frameworks externos.
- Incremento desta sessão: `LoggingDbConnectionInterceptor` e `RecordingDbConnectionInterceptor` passaram a convergir para um formatter público compartilhado (`DbInterceptionEventFormatter`), reduzindo acoplamento às strings internas e deixando a representação textual dos eventos estável para logging/diagnóstico leve.
- Incremento desta sessão: a trilha ganhou integração nativa com `DiagnosticListener` (`DiagnosticListenerDbConnectionInterceptor` + nomes públicos em `DbInterceptionDiagnosticNames`), abrindo caminho para observabilidade baseada em runtime sem adicionar dependências externas ao contrato inicial.
- Incremento desta sessão: em TFMs modernos o pipeline também passou a expor `ActivitySourceDbConnectionInterceptor` e `DbInterceptionActivityNames`, conectando a mesma trilha de eventos a spans/activities nativos do runtime para integração futura com OpenTelemetry.
- Incremento desta sessão: o pacote base passou a oferecer também `TextWriterDbConnectionInterceptor` como ponte direta para `Console.Out`, `StringWriter` e writers de arquivo, cobrindo um caminho operacional simples de captura textual sem amarrar o contrato a um framework de logging específico.
- Incremento desta sessão: a criação do pipeline também foi consolidada em `DbInterceptionOptions` + `WithInterception(...)`, permitindo compor recorder/logging/text-writer/fault injection/diagnostics em uma entrada única sem wiring manual repetitivo nos testes consumidores.
- Incremento desta sessão: a trilha ganhou helpers de DI (`AddDbInterception`, `AddDbConnectionInterceptor<T>` e `WithRegisteredInterceptors(IServiceProvider)`), reduzindo atrito de adoção em aplicações/testes que já constroem conexões a partir de `ServiceCollection`.
- Incremento desta sessão: o caminho por `DbInterceptionOptions`/DI passou a aceitar instâncias explícitas de recorder e a registrar interceptors também pelo tipo concreto, facilitando reutilização do mesmo `RecordingDbConnectionInterceptor` e inspeção posterior do histórico sem varrer apenas a interface base.
- Incremento desta sessão: a integração com DI ganhou atalhos mais altos para os casos operacionais mais comuns (`AddDbInterceptionRecording`, `AddDbInterceptionLogging` e `AddDbInterceptionTextWriter`), reduzindo ainda mais o boilerplate em hosts/test setups simples.
- Incremento desta sessão: a trilha também passou a oferecer ponte direta para o stack padrão de logging do .NET com `ILoggerDbConnectionInterceptor` e `AddDbInterceptionLogger(...)`, reaproveitando o formatter comum do pipeline sem criar um modelo paralelo de mensagem.
- Incremento desta sessão: o core ganhou também `IDbInterceptionConnectionFactory`/`DbInterceptionConnectionFactory` e os helpers `WithInterceptionFactory(...)`, aproximando a proposta do cenário de providers reais ao permitir encapsular qualquer `Func<DbConnection>` sem depender ainda de um provider específico do repositório.
- Incremento desta sessão: a primeira adoção provider-specific do pipeline foi aplicada nas factories `Sqlite` de `EF Core` e `LinqToDB`, que passaram a aceitar interceptors ou `DbInterceptionOptions` diretamente e a devolver conexões abertas já encapsuladas, validando o caminho fora do core puro.
- Incremento desta sessão: o mesmo padrão de adoção provider-specific também foi replicado nas factories `SqlServer` de `EF Core` e `LinqToDB`, reduzindo o risco de que a trilha de interceptação estivesse acoplada a particularidades do provider SQLite.
- Incremento desta sessão: a adoção provider-specific foi expandida também para `Npgsql` (`EF Core` e `LinqToDB`), consolidando o mesmo modelo em três providers distintos e reduzindo o risco de drift entre integrações ORM principais.
- Incremento desta sessão: o mesmo modelo foi expandido também para `MySql` (`EF Core` e `LinqToDB`), elevando para quatro providers a adoção direta do pipeline e tornando mais defensável a ideia de uma trilha comum de interceptação para integrações ORM do projeto.
- Incremento desta sessão: a mesma trilha provider-specific também passou a cobrir `Oracle` (`EF Core` e `LinqToDB`), levando a adoção direta do pipeline para cinco providers principais e reduzindo ainda mais o risco de especialização excessiva por dialeto.
- Incremento desta sessão: a expansão horizontal foi concluída também em `Db2` (`EF Core` e `LinqToDB`), deixando os seis providers ORM principais do repositório sob o mesmo padrão inicial de factories interceptáveis e reduzindo o trabalho restante para replicação estrutural.
- Incremento desta sessão: o contrato compartilhado da `DbMockConnectionFactory` também passou a cobrir explicitamente o caminho `CreateWithTablesIntercepted(...)` em todos os providers que reutilizam a base comum, estendendo a trilha de interceptação para o entry point runtime transversal inclusive onde não há factory ORM dedicada.
- Incremento desta sessão: a integração com DI foi estendida também para `IDbInterceptionConnectionFactory` (`AddDbInterceptionConnectionFactory(...)` com interceptors ou options), conectando a nova factory genérica ao mesmo fluxo de host/test setup já coberto pelos helpers de `ServiceCollection`.
- Incremento desta sessão: o caminho de DI para `IDbInterceptionConnectionFactory` também ganhou uma sobrecarga baseada em `IServiceProvider`, permitindo que a factory criada por delegate reutilize automaticamente a cadeia de `DbConnectionInterceptor` já registrada no container do host/teste.
- Incremento desta sessão: a mesma factory em DI também passou a aceitar composicao de `DbInterceptionOptions` a partir do `IServiceProvider`, fechando o caso em que conexao interna, recorder/logger e demais dependencias precisam ser resolvidos do mesmo container sem wiring manual fora do host.
- Incremento desta sessão: `AddDbInterception(...)` tambem passou a aceitar composicao de `DbInterceptionOptions` com acesso ao `IServiceProvider`, alinhando o helper principal de DI ao restante da trilha e permitindo montar interceptors nativos a partir de servicos ja registrados no host.
- Incremento desta sessão: a ergonomia da `DbMockConnectionFactory` foi alinhada ao restante da trilha de interceptação, com overloads `Create*WithTablesIntercepted(...)`/`CreateWithTablesIntercepted(...)` aceitando também `DbInterceptionOptions`, e o contrato compartilhado passou a validar esse caminho em todos os providers que reutilizam a base comum.
- Incremento desta sessão: `DbInterceptionOptions` ganhou helpers fluentes (`UseRecording`, `UseLogging`, `UseTextWriter`, `UseFaultInjection`, `UseDiagnosticListener`, `UseActivitySource`, `AddInterceptor`), reduzindo boilerplate de composição e deixando a superfície principal da feature mais coesa para hosts/test setups.
- Incremento desta sessão: a API estática `DbInterceptionPipeline.Wrap(...)` passou a aceitar configuração inline por `Action<DbInterceptionOptions>`, fechando a simetria entre entrada estática, extensions, factory genérica e factory runtime interceptada.
- Incremento desta sessão: a trilha passou a incluir exemplos mínimos de composicao no README e validacao consumidora direta com Dapper sobre conexao interceptada, fechando o escopo pratico da proposta original sem quebrar a superficie ADO.NET padrao usada por bibliotecas externas.
- Incremento desta sessão: `WithInterceptionFactory(...)` tambem passou a aceitar uma instancia pronta de `DbInterceptionOptions`, fechando a simetria ergonomica entre wrapping direto, factory generica, factory runtime e configuracao por DI.
- Incremento desta sessão: `SqlAzure` foi fechado explicitamente como provider sem pacote ORM dedicado, com validacao direta do `Intercept(...)`/`CreateSqlAzureWithTablesIntercepted(...)` e documentacao do caminho oficial de adocao pelo proprio provider package e pela `DbMockConnectionFactory`.
- Resultado consolidado do item:
  - core do pipeline entregue para conexao, comando e transacao;
  - interceptors concretos para recorder, logging, `TextWriter`, `ILogger`, fault injection, `DiagnosticListener` e `ActivitySource`;
  - adocao no engine `DbSqlLikeMem`, factories runtime, factories ORM por provider e fluxo por DI;
  - validacao de uso com EF Core/LinqToDB por factories, Dapper por testes consumidores e composicao documentada com MiniProfiler.
- Andamento agregado do item (`2.1.4`): **100%**.

### 2.2 Compatibilidade com Dapper

#### 2.2.1 Fluxo amigável para micro-ORM

- Implementação estimada: **100%**.
- Execução de queries e comandos com padrão próximo do uso em produção.
- Reaproveitamento de código de acesso a dados em ambiente de teste.
- Menor necessidade de doubles manuais de repositório.
- Fluxo validado para `Execute`/`Query` parametrizados e procedures (`CommandType.StoredProcedure`) com parâmetros `Input/Output/InputOutput/ReturnValue`.
- P10/P14 reforçam cobertura de procedures, parâmetros OUT e cenários Dapper avançados (multi-mapping, QueryMultiple) para uso real de aplicação.
- Incremento desta sessão: suíte contratual compartilhada `DapperSupportTestsBase` passou a cobrir `QueryMultiple` com múltiplos result sets ordenados e multi-mapping com `splitOn`, elevando cobertura cross-provider automática via `DapperSmokeTests` (SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2) sem duplicação de cenário.
- Incremento desta sessão: contratos consumidores Dapper de CRUD, `QueryMultiple`, multi-mapping e stored procedures foram consolidados em bases compartilhadas (`DapperCrudTestsBase`, `DapperUserTestsBase`, `StoredProcedureExecutionTestsBase`), reduzindo boilerplate entre providers e reforçando previsibilidade do fluxo micro-ORM sem esconder o SQL exercitado.
- Incremento desta sessão: `StoredProcedureExecutionTests` de SQL Server, Oracle e Db2 também passaram a reutilizar `StoredProcedureExecutionTestsBase`, estendendo a padronização do contrato de procedures para cinco providers (MySQL, Npgsql, SQL Server, Oracle e Db2), enquanto SQLite permanece isolado por semântica própria de `ParameterDirection`.
- Incremento desta sessão: `DapperTests`, `DapperUserTests` e `DapperUserTests2` de SQLite e SQL Server também passaram a reutilizar as bases compartilhadas (`DapperCrudTestsBase` e `DapperUserTestsBase`), e o DTO `UserObjectTest` foi promovido para `DbSqlLikeMem.Dapper.TestTools`, fechando a principal lacuna remanescente de duplicação cross-provider nos testes consumidores Dapper.
- Incremento desta sessão: a suíte específica de stored procedures do SQLite também foi extraída para a base dedicada `SqliteStoredProcedureExecutionTestsBase`, preservando a semântica própria de `ParameterDirection` do provider e fechando o último bloco relevante de boilerplate no fluxo micro-ORM.

#### 2.2.2 Cenários prioritários

- Implementação estimada: **100%**.
- Testes de SQL embarcado em métodos de repositório.
- Validação de mapeamento simples e comportamento de filtros.
- Ensaios de regressão de query sem banco real.
- Incremento desta sessão: cenários prioritários de consumo real de repositório foram reforçados com contratos compartilhados de leitura multi-result (`QueryMultiple`) e composição de agregado por join (`Query<TFirst,TSecond,...>` com `splitOn`), reduzindo risco de regressão em fluxos Dapper avançados.
- Incremento desta sessão: cenários de usuário/repositório e procedures em MySQL, Npgsql, Oracle e Db2 passaram a compartilhar contratos consumidores explícitos (CRUD, `QueryMultiple`, join com `splitOn`, `CommandType.StoredProcedure` e parâmetros `OUT/ReturnValue`), aumentando cobertura reutilizável de casos próximos ao uso real.
- Incremento desta sessão: o contrato compartilhado de procedures foi estendido também a SQL Server, Oracle e Db2, reduzindo divergência entre bancos principais em cenários de repositório com `CommandType.StoredProcedure` e validações de parâmetros obrigatórios/OUT/ReturnValue.
- Incremento desta sessão: SQLite e SQL Server foram alinhados às mesmas bases compartilhadas de CRUD/usuário Dapper, ampliando a cobertura reutilizável dos cenários de repositório para todos os principais providers já tratados no backlog.
- Incremento desta sessão: o caso específico de procedures no SQLite deixou de depender de suíte local monolítica e passou a usar base dedicada, reduzindo custo de manutenção sem perder a diferença comportamental relevante do provider.
- Incremento desta sessão: `QueryExecutorExtrasTests` de MySQL e Npgsql passaram a reutilizar a base compartilhada `QueryExecutorExtrasTestsBase`, cobrindo agregação agrupada, paginação multi-result e tradução LINQ com diferença explícita apenas na sintaxe de paginação por dialeto.
- Incremento desta sessão: `JoinTests` e `TransactionTests` de MySQL e Npgsql passaram a reutilizar bases compartilhadas (`DapperJoinTestsBase` e `DapperTransactionTestsBase`) com wrappers finos preservando `Trait`/categoria por provider, reduzindo duplicação de setup/seed sem esconder a intenção dos cenários de uso real.
- Incremento desta sessão: `FluentTest` e `Extended*MockTests` de MySQL/Npgsql passaram a reutilizar bases compartilhadas (`DapperFluentTestsBase` e `ExtendedDapperProviderTestsBase`), consolidando cenários consumidores de setup fluente, filtros, paginação e integridade referencial com diferenças explícitas só no SQL específico do provider.

### 2.3 Factory de provedor em runtime

#### 2.3.1 Seleção dinâmica por chave

- Implementação estimada: **100%**.
- Escolha de provedor por string/configuração (`mysql`, `sqlserver`, `sqlazure`/`azure-sql`, `oracle`, `postgresql`, `sqlite`, `db2`).
- Suporte a testes parametrizados por dialeto.
- Base para suíte cross-provider.
- Incremento desta sessão: `DbMockConnectionFactory` passou a usar plano de resolução cacheado por provider canônico (`ProviderResolutionPlan`), eliminando varredura/reflection completa em cada chamada e reduzindo overhead de seleção dinâmica em runtime.
- Incremento desta sessão: regressões de alias normalizado com hífen/sublinhado foram ampliadas para todos os bancos na suíte da factory (`sql_ite`, `my-sql`, `sql-server`, `or_acle`, `post_gres`/`post-gresql`, `db-2`), reforçando robustez da seleção dinâmica por configuração textual heterogênea.
- Incremento desta sessão: fábrica de `DbMock` passou a evitar tentativa de instanciação redundante durante detecção de construtor compatível e o resolver de conexão voltou a percorrer todos os membros candidatos (property/method) até achar `IDbConnection` não-nulo, preservando semântica de fallback com menor overhead.
- Incremento desta sessão: aliases pragmáticos adicionais de runtime (`mssql`, `sqlsrv`, `mariadb`, `sqlite3`, `ibmdb2`, `pg`, `ora`) foram canonizados na `DbMockConnectionFactory` e cobertos pelos testes contratuais dos providers, fechando a seleção dinâmica por chave com maior tolerância a convenções reais de configuração.

#### 2.3.2 Estratégias de uso

- Implementação estimada: **100%**.
- Executar o mesmo caso de teste em múltiplos bancos simulados.
- Identificar dependências acidentais de sintaxe específica.
- Planejar portabilidade de consultas.
- Incremento desta sessão: estratégia de criação e resolução de conexão por provider foi consolidada em delegates reutilizáveis (fábricas de `DbMock` + resolvers de `IDbConnection`), preservando isolamento entre chamadas e melhorando previsibilidade/performance para suítes parametrizadas cross-provider.

### 2.4 Critérios de qualidade para integração

#### 2.4.1 Confiabilidade de API

- Implementação estimada: **100%**.
- Chamadas mais comuns devem manter semântica previsível para testes de aplicação.
- Mensagens de erro precisam apontar de forma clara comando, dialeto e contexto.
- Capabilities comuns entre providers cobrem `WHERE`, `GROUP BY/HAVING`, `CREATE VIEW`, `CREATE TEMP TABLE` e integração ORM, reduzindo diferenças de uso em testes.
- Contrato de mensagens para SQL não suportado foi padronizado e coberto por regressão em múltiplos providers.
- Incremento desta sessão: mensagens de validação/limitação para projeções DML (`RETURNING`, `OUTPUT`, `RETURNING INTO`) foram centralizadas no helper compartilhado `SqlUnsupported`, removendo literais duplicados em `SqlServerCommandMock`, `NpgsqlCommandMock`, `SqliteCommandMock` e `OracleCommandMock` e reforçando consistência diagnóstica cross-provider.
- Incremento desta sessão: mensagens de runtime para tabela inexistente e ciclo de savepoint (savepoint não encontrado e ausência de transação ativa) foram centralizadas em `SqlUnsupported` e adotadas no núcleo (`DbConnectionMockBase`) e nas estratégias DML (`DbInsertStrategy`, `DbUpdateStrategy`, `DbDeleteStrategy`, `DbUpdateDeleteFromSelectStrategies`, `DbSelectIntoAndInsertSelectStrategies`), reduzindo duplicação e drift semântico de diagnóstico.
- Incremento desta sessão: mensagens de contrato para pipeline non-query e procedures (`NonQueryHandlerCouldNotProcessStatement`, `ProcedureNameNotProvided`, `InvalidCallStatement`) foram centralizadas em `SqlExceptionMessages` e aplicadas no núcleo compartilhado (`CommandExecutionPipeline`, `DbStoredProcedureStrategy`) com recursos multilíngues (`en`, `pt`, `de`, `es`, `fr`, `it`), melhorando consistência diagnóstica entre providers.
- Incremento desta sessão: mensagem de falha de extração de tabela no LINQ provider foi centralizada em `SqlExceptionMessages.LinqCouldNotExtractTableNameFromExpression(...)` e aplicada em todos os providers (`SqliteLinqProvider`, `MySqlLinqProvider`, `SqlServerLinqProvider`, `NpgsqlLinqProvider`, `OracleLinqProvider`, `Db2LinqProvider`) com suporte multilíngue, eliminando literal duplicado e padronizando diagnóstico.
- Incremento desta sessão: mensagens repetidas de criação/inserção em memória (`TableAlreadyExists`, `InvalidCreateTableStatement`, `InvalidInsertSelectStatement`, `ColumnCountDoesNotMatchSelectList`) foram centralizadas em `SqlExceptionMessages` e adotadas no core (`SchemaMock`, `DbInsertStrategy`, `DbSelectIntoAndInsertSelectStrategies`) com recursos multilíngues (`en`, `pt`, `de`, `es`, `fr`, `it`), reduzindo drift semântico em erros frequentes de setup e carga de dados.
- Incremento desta sessão: mensagens de contrato para `MERGE` e `UPDATE/DELETE ... JOIN` (`Merge*`, `UpdateJoin*`, `DeleteJoin*` e `JoinOnMustReferenceTargetAndSubqueryAliases`) foram centralizadas em `SqlExceptionMessages` e aplicadas nos strategies compartilhados (`DbMergeStrategy`, `DbUpdateDeleteFromSelectStrategies`) com recursos multilíngues (`en`, `pt`, `de`, `es`, `fr`, `it`), reforçando consistência diagnóstica em mutações avançadas cross-provider.
- Incremento desta sessão: diagnósticos de resolução dinâmica de conexão e materialização de batch (`ResolvedConnectionTypeNotCompatible`, `NoConcreteDbMockImplementationFound`, `NoCompatibleDbMockConstructorFound`, `CouldNotResolveConnectionFromDbMock`, `NoCompatibleConnectionConstructorFound`, `CannotMaterializeBatchCommandType`, `BatchCommandTypeHasIncompatibleMembers`) foram centralizados em `SqlExceptionMessages` e aplicados em `DbMockConnectionFactory` e `BatchCommandMaterializer`, com recursos multilíngues (`en`, `pt`, `de`, `es`, `fr`, `it`) para reduzir drift em falhas de infraestrutura cross-provider.
- Incremento desta sessão: mensagens repetidas de setup em `DbSeedExtensions` (`TableNotYetDefined`, `ColumnAlreadyExistsInTable`, `SeedRowHasMoreValuesThanColumns`) foram centralizadas em `SqlExceptionMessages` e aplicadas nas rotinas fluentes de definição/seed, reforçando consistência diagnóstica para cenários de inicialização de testes.
- Incremento desta sessão: `DbMock` passou a reutilizar contrato centralizado para `view` duplicada/inexistente (`ViewAlreadyExists`, `ViewDoesNotExist`) e alinhou os caminhos de `DROP TABLE/TEMP TABLE` ao helper compartilhado `SqlUnsupported.ForNormalizedTableDoesNotExist(...)`, reduzindo drift entre runtime base e operações de catálogo.
- Incremento desta sessão: seleções ambíguas de schema, catálogo base (`GetTable`/`GetView`), duplicidade de PK/índice e validações estruturais de `ColumnDef` foram padronizadas em `SqlExceptionMessages`, eliminando exceções genéricas/literais remanescentes do núcleo exposto ao consumidor e fechando o eixo principal de confiabilidade diagnóstica da API.

#### 2.4.2 Legibilidade dos testes consumidores

- Implementação estimada: **100%**.
- Priorizar exemplos com setup curto e intenção explícita.
- Evitar camadas de abstração que escondam a query que está sendo validada.
- Incremento desta sessão: testes de `DbMockConnectionFactory` dos sete providers passaram a usar contrato compartilhado em `DbSqlLikeMem.TestTools` (`DbMockConnectionFactoryContractTestsBase`), reduzindo duplicação de setup/assert, padronizando intenção dos cenários (shortcut, mapeamento, isolamento e aliases) e melhorando manutenção/leitura cross-provider.
- Incremento desta sessão: `DapperSmokeTests` dos seis providers passaram a herdar da base genérica compartilhada `DapperSmokeTestsBase<TConnection>`, removendo boilerplate repetido de abertura de conexão e mantendo comportamento contratual uniforme para SQLite, MySQL, SQL Server, Npgsql, Oracle e Db2.
- Incremento desta sessão: smoke tests de `EF Core`, `LinqToDB` e `NHibernate` passaram a reutilizar bases genéricas compartilhadas (`EfCoreSmokeTestsBase`, `LinqToDbSmokeTestsBase`, `NHibernateSmokeTestsBase`), reduzindo wrappers repetidos por provider e mantendo explícita apenas a configuração específica de dialeto/driver/factory.
- Incremento desta sessão: os `DapperTests` CRUD/multi-result de MySQL, Npgsql, Oracle e Db2 passaram a reutilizar a base genérica `DapperCrudTestsBase`, removendo duplicação estrutural entre providers e preservando explícitas apenas as factories de `DbMock`, conexão e comando.
- Incremento desta sessão: os `DapperUserTests` de MySQL, Npgsql, Oracle e Db2 passaram a reutilizar a base genérica `DapperUserTestsBase` com modelo contratual compartilhado (`DapperUserContractModel`), reduzindo boilerplate de setup/assert sem esconder a intenção dos cenários CRUD e `QueryMultiple`.
- Incremento desta sessão: os `DapperUserTests2` de MySQL, Npgsql, Oracle e Db2 também passaram a reutilizar `DapperUserTestsBase`, com parametrização mínima de SQL por provider para diferenças de quoting em `QueryMultiple`/`JOIN`, fechando o principal bloco remanescente de duplicação em testes consumidores Dapper.

---

## 3) Provedores SQL suportados

### 3.0 Direcionadores do backlog por provedor

- Cobrir primeiro os provedores com maior base instalada no produto consumidor.
- Tratar diferenças de sintaxe como requisito funcional, não detalhe cosmético.
- Manter rastreabilidade entre gap reportado, teste criado e item de roadmap.

#### 3.0.1 Expansão planejada de famílias SQL

- Implementação estimada: **80%**.
- A próxima expansão deve continuar por famílias de dialeto, reaproveitando parser/runtime existentes antes de isolar o que realmente merecer provider próprio.
- TODO: refatorar a família PostgreSQL para permitir um `DuckDbDialect` compartilhando o máximo possível do caminho `Npgsql/PostgreSQL`.
- TODO: cobrir no `DuckDbDialect` o subset inicial realmente priorizado (`STRUCT`, `LIST`, `UNNEST`) somente depois da base compartilhada estar pronta.
- TODO: planejar a fase posterior da família analytics com `ClickHouse` (`ARRAY JOIN`, `LIMIT BY`, `ENGINE MergeTree`) sem acoplar sintaxe analítica diretamente ao executor comum.
- TODO: planejar `Snowflake` como extensão posterior da trilha analytics, com matriz de compatibilidade e subset explícito antes da primeira implementação.

#### 3.0.2 Inventário funcional pendente por provider

- Implementação estimada: **100%**.
- Incremento desta sessão: o inventário pendente passou a registrar explicitamente a convenção documental de versões MySQL em formato humano (`3.0`, `4.0`, `5.5`, `5.6`, `5.7`, `8.0`, `8.4`) com equivalência para os inteiros usados na API (`30`, `40`, `55`, `56`, `57`, `80`, `84`), reduzindo drift entre backlog, código e exemplos.
- Incremento desta sessão: o pruning do MySQL passou a inferir o mesmo subset também quando o predicado usa a coluna de data diretamente em faixa alinhada ao ano (`CreatedAt >= ... AND CreatedAt < ...`).
- Incremento desta sessão: a mesma faixa direta agora atravessa mais de uma particao conhecida quando o intervalo cobre varios anos.
- Incremento desta sessão: o subset ganhou tambem `PARTITION BY LIST (YEAR(...))` com valores explicitos.
- `MySQL`: `LIMIT/OFFSET`, `ON DUPLICATE KEY UPDATE`, `MATCH ... AGAINST`, `SQL_CALC_FOUND_ROWS`/`FOUND_ROWS`, `USE/IGNORE/FORCE INDEX`, `<=>` e `GROUP_CONCAT` ja estao cobertos nas versoes simuladas atuais.
- `MySQL`: `JSON_EXTRACT`/`->`/`->>` entram em `5.0+`; `WITH RECURSIVE` e window functions entram em `8.0+`.
- `MySQL`: `JSON_TABLE` já está coberto no subset `8.0+`.
- `MySQL`: particionamento ja cobre:
  - metadata
  - roteamento de escrita por particao conhecida
  - leitura explicita com `PARTITION (...)`
  - pruning por igualdade, `IN (...)`, `BETWEEN`, `OR` seguro e faixa direta por data alinhada ao ano
  - pruning por `YEAR()` e `EXTRACT(YEAR FROM ...)`
  - `EXTRACT(YEAR FROM ...)` com `IN (...)`
  - `EXTRACT(YEAR FROM ...)` com `BETWEEN ... AND ...`
  - `EXTRACT(YEAR FROM ...)` com comparacao de faixa
  - faixa invertida com `EXTRACT(YEAR FROM ...)`
  - `EXTRACT(YEAR FROM ...)` com `OR` em faixas distintas
  - `EXTRACT(YEAR FROM ...)` com `OR` em `BETWEEN`
  - `LIST` por `YEAR()`
- `MySQL`: o pruning tambem cobre misturas de igualdade com `IN (...)` e faixas que atravessam mais de uma particao conhecida na mesma consulta.
- `SQL Server/SqlAzure`: `TOP`, `OFFSET/FETCH`, `OUTPUT`, `MERGE`, `@@ROWCOUNT`, table/query hints `WITH (...)`, `PIVOT/UNPIVOT`, `CROSS APPLY`/`OUTER APPLY`, `JSON_VALUE`/`OPENJSON`, `STRING_AGG`, `STRING_SPLIT` e `FOR JSON` ja estao mapeados com gate por versao simulada e `compatibility level`, ficando o backlog residual concentrado nas nuances avancadas dessas familias.
- `Oracle`: `ROWNUM`, `FETCH FIRST/NEXT`, `MERGE`, `seq.NEXTVAL/CURRVAL`, `LISTAGG`, `JSON_VALUE`, `JSON_TABLE` e o subset atual de `PIVOT` ja estao explicitados por versao simulada.
- `Oracle`: `CONNECT BY/START WITH`, `MATCH_RECOGNIZE` e `MODEL` seguem fora do subset explicito ou ainda parciais.
- `PostgreSQL/Npgsql`: `LIMIT/OFFSET`, `FETCH FIRST`, `ON CONFLICT`, `RETURNING`, `ILIKE`, `STRING_AGG`, operadores JSON `->`/`->>`/`#>`/`#>>` e `WITH [NOT] MATERIALIZED` ja estao mapeados na trilha atual; `DISTINCT ON`, `LATERAL` e `MERGE` seguem como backlog residual explicito.
- `SQLite`: `LIMIT/OFFSET`, `ON CONFLICT`, `RETURNING`, `GROUP_CONCAT` com `ORDER BY`, `JSON_EXTRACT`/`->`/`->>`, `WITH RECURSIVE`, `CHANGES()`, `NULLS FIRST/LAST` e a familia principal de window functions ja estao refletidos no subset atual; `MATERIALIZED/NOT MATERIALIZED` permanece parcial por cenario, e `json_each/json_tree` com detalhes avancados de frame continuam pendentes.
- `DB2`: `FETCH FIRST`, `MERGE` (`>= 9`), `NEXT VALUE FOR`/`PREVIOUS VALUE FOR`, `LISTAGG`, `JSON_QUERY`, `JSON_TABLE` e `CREATE OR REPLACE FUNCTION/PROCEDURE/TRIGGER` ja estao refletidos no inventario atual; `LIMIT/OFFSET` segue nao suportado no dialeto.

### 3.1 MySQL (`DbSqlLikeMem.MySql`)

- Conteúdo movido para [DBs/MySql.md](DBs/MySql.md).

#### 3.1.1 MariaDB (`DbSqlLikeMem.MariaDb`)

- Conteúdo movido para [DBs/MariaDb.md](DBs/MariaDb.md).

### 3.2 SQL Server (`DbSqlLikeMem.SqlServer`)

- Conteúdo movido para [DBs/SqlServer.md](DBs/SqlServer.md).

### 3.2.1 SqlAzure (`DbSqlLikeMem.SqlAzure`)

- Conteúdo movido para [DBs/SqlAzure.md](DBs/SqlAzure.md).

### 3.3 Oracle (`DbSqlLikeMem.Oracle`)

- Conteúdo movido para [DBs/Oracle.md](DBs/Oracle.md).

### 3.4 PostgreSQL / Npgsql (`DbSqlLikeMem.Npgsql`)

- Conteúdo movido para [DBs/Npgsql.md](DBs/Npgsql.md).

### 3.5 SQLite (`DbSqlLikeMem.Sqlite`)

- Conteúdo movido para [DBs/Sqlite.md](DBs/Sqlite.md).

### 3.6 DB2 (`DbSqlLikeMem.Db2`)

- Conteúdo movido para [DBs/Db2.md](DBs/Db2.md).

### 3.7 Estratégia multi-provedor

#### 3.7.1 Matriz de cobertura

- Implementação estimada: **94%**.
- Executar casos críticos em todos os provedores prioritários do produto.
- Definir perfil mínimo de compatibilidade por módulo.
- Execução matricial por provider já iniciada em CI (`provider-test-matrix.yml`), com publicação de artefatos de resultado por projeto e etapas dedicadas de smoke e agregação cross-dialect, com publicação de snapshot por perfil em artefatos de CI.
- Cobertura de regressão inclui suíte cross-dialeto com snapshots por perfil (smoke/aggregation/parser), operacionalizada no script `scripts/run_cross_dialect_equivalence.sh`; atualização em lote suportada por `scripts/refresh_cross_dialect_snapshots.sh` e baseline documental semântico (`manual-placeholder`) para evitar snapshot desatualizado no repositório.
- O profile `parser` agora inclui também `SqlAzure`, fechando a matriz principal de providers SQL suportados nessa trilha sem precisar duplicar runtime do dialeto.
- Matriz consolidada de providers/versões e capacidades comuns agora está refletida diretamente neste índice como fonte principal de backlog.
- Incremento desta sessão: a trilha dedicada de benchmark comparativo foi materializada em `benchmark/DbSqlLikeMem.Benchmarks`, cobrindo `MySQL`, `SQL Server`, `PostgreSQL/Npgsql`, `Oracle` e `DB2` contra runtimes reais via `Testcontainers`, `Sqlite` contra baseline nativa e `SqlAzure` como mock-only com proxy operacional em `SQL Server`.
- Incremento desta sessão: a mesma trilha foi padronizada em `Testcontainers`/modo `preprovisioned`, garantindo baseline reproduzível por provider sem depender de instalação manual local para os bancos reais viáveis no ambiente de testes.
- Incremento desta sessão: os resultados consolidados dessa trilha já são publicados na wiki espelhada em `docs/Wiki/performance-matrix.md`, `docs/Wiki/performance-matrix-app-specific.md` e artefatos versionados em `docs/Wiki/BenchmarkResults`.
- Incremento desta sessão: a trilha de performance agora consolida, no backlog, os hot paths do core já estabilizados em otimizações anteriores com chaves estruturadas para índices e PK, `AddBatch` incremental e paralelização best-effort em caminhos thread-safe, sem reabrir funcionalidades já fechadas no código.
- TODO: ampliar a matriz compartilhada para capacidades avançadas auditadas contra bancos reais (`FOR JSON`, `CROSS APPLY/OUTER APPLY`, `LATERAL`, `DISTINCT ON`, `json_each/json_tree`, `PIVOT/UNPIVOT`) com status explícito por provider.
- TODO: incluir `SqlDialect.Auto` na malha `parser`/`smoke` com snapshots dedicados para sintaxes equivalentes de paginação e demais heurísticas que entrarem no modo automático.
- TODO: expandir a matriz para os próximos providers/famílias planejados (`DuckDB` e, em fase posterior, `ClickHouse`/`Snowflake`) com status por etapa de implementação.
- TODO: conectar a futura API de validação cross-dialect aos artefatos publicados da matriz para transformar compatibilidade em evidência objetiva de CI.
- Providers do backlog com benchmark viável por container para expansão futura: `ClickHouse`.
- Fora desta trilha por enquanto: `DuckDB` e `SQLite` permanecem embedded/nativos, e `SqlAzure`/`Snowflake` seguem sem baseline local/container equivalente no ciclo atual.

#### 3.7.2 Priorização de gaps

- Implementação estimada: **70%**.
- Gaps que quebram fluxo de negócio entram no topo do backlog.
- Priorização prática usa ondas inspiradas no pipeline P0..P14 (baseline, core, composição, avançado, hardening).
- Diferenças cosméticas/documentais podem ficar em ondas posteriores.
- TODO: formalizar critério objetivo de severidade por gap combinando impacto de negócio, quantidade de providers afetados e distância para o comportamento do banco real.

### 3.8 Modelo de evolução por ondas

#### 3.8.1 Onda 1 (crítica)

- Implementação estimada: **84%**.
- Comandos que bloqueiam operações essenciais de CRUD e autenticação/autorização da aplicação.
- TODO: manter nesta onda os gaps que ainda quebram fluxo essencial do core, começando pela refatoração das famílias reutilizáveis de dialeto e pelo fechamento dos gaps pequenos/críticos do parser comum.
- TODO: manter também nesta onda os gaps que ainda quebram fluxo essencial do core, como `UPDATE/DELETE` multi-tabela dirigidos por dialeto, `PIVOT` subset incompleto e families JSON tabulares mais críticas por provider.

#### 3.8.2 Onda 2 (alta)

- Implementação estimada: **78%**.
- Diferenças que impactam relatórios, filtros avançados e paginação em módulos centrais.
- Inclui execução do plano P11/P12 para confiabilidade transacional, concorrência e diagnóstico de erro com contexto.
- Status detalhado de transações concorrentes: fase de hardening base concluída (100%), governança em progresso (~10%) e cenários críticos (fases 2–5) priorizados para fechamento.
- TODO: manter nesta onda recursos avançados de consulta com impacto funcional frequente (`FOR JSON`, `STRING_SPLIT`, `CROSS APPLY/OUTER APPLY`, `DISTINCT ON`, `LATERAL`, window frames avançados no SQLite).
- TODO: priorizar nesta onda `DuckDB` e `Cross Dialect Validator`, respeitando a ordem de dependências definida no roadmap, já que `Query Plan Debugger` e `Schema Snapshot` passaram a constar como trilhas já materializadas no índice.

#### 3.8.3 Onda 3 (média/baixa)

- Implementação estimada: **76%**.
- Cobertura de sintaxes menos frequentes e melhorias de ergonomia para debug.
- Inclui trilhas P13/P14 para performance (hot paths/caching) e conformidade de ecossistema (.NET/ORM/tooling).
- Inclui avaliação de partição de tabelas em subset (metadado + pruning básico) após estabilização dos gaps críticos de parser/executor.
- TODO: manter nesta onda recursos especializados e de menor recorrência operacional, como `MATCH_RECOGNIZE`, particionamento mais amplo fora do subset seguro e expansões de observabilidade/ergonomia do plano de execução.
- TODO: deixar nesta onda a família analytics (`ClickHouse`, `Snowflake`) e a trilha de fuzz/comparação multi-dialeto, salvo se algum consumidor real elevar a prioridade.

### 3.9 MariaDB (`DbSqlLikeMem.MariaDb`)

- Conteúdo movido para [DBs/MariaDb.md](DBs/MariaDb.md).

---

## 4) Recursos comportamentais adicionais

### 4.0 Objetivo dos recursos comportamentais

- Simular efeitos colaterais de banco que impactam regra de negócio além do resultado da query.
- Tornar explícito no teste quando um comportamento depende de trigger ou semântica de dialeto.

### 4.1 Triggers

#### 4.1.1 Tabelas não temporárias

- Implementação estimada: **89%**.
- Suporte a triggers em `TableMock`.
- Percentual revisado com base em validações por dialeto (`SupportsTriggers`) e suites dedicadas por provider.
- Eventos: before/after insert, update e delete.
- Permite simular regras reativas de domínio persistido.
- Incremento desta sessão: `SqlAzure` ganhou suíte dedicada de estratégia para triggers em tabelas não temporárias e temporárias, fechando o gap remanescente do provider que compartilha pipeline com SQL Server mas ainda não tinha regressão explícita.
- TODO: explicitar e validar no backlog as diferenças remanescentes de triggers por provider (ordenação, recursão, mutação encadeada e limitações intencionais do mock).

#### 4.1.2 Tabelas temporárias

- Implementação estimada: **100%**.
- Triggers não executadas em tabelas temporárias (connection/global).
- Comportamento explícito para evitar ambiguidade em testes.

#### 4.1.3 Diretrizes de uso

- Implementação estimada: **72%**.
- Preferir assertions claras sobre efeitos da trigger.
- Isolar cenários de trigger dos cenários de query pura.
- TODO: adicionar cookbook operacional de trigger com padrões de teste, anti-padrões e guidance de isolamento por provider/escopo de tabela.

### 4.2 Compatibilidade por dialeto (governança de gaps)

#### 4.2.1 Matriz de compatibilidade SQL

- Implementação estimada: **88%**.
- Registro do que já está suportado por banco/versão.
- Visão de lacunas e riscos por área funcional.
- Matriz feature x dialeto já publicada e usada como referência de hardening/regressão.
- Matriz versionada (`vCurrent`/`vNext`) e rastreável para testes corresponde ao fechamento do checklist de documentação.
- TODO: sincronizar a matriz de compatibilidade com a nova auditoria contra bancos reais, expondo explicitamente os recursos já listados como TODO nas seções por provider.
- TODO: publicar também o resultado do futuro `SqlCompatibilityCheck`/`ValidateAcrossDialects(query)` como evidência objetiva por recurso, provider e versão simulada.

#### 4.2.2 Roadmaps de parser/executor

- Implementação estimada: **84%**.
- Planejamento incremental por marcos.
- Track global de regressão cross-dialect está em ~70%, com ampliação contínua da cobertura em matriz de smoke/regressão.
- Conexão entre backlog técnico e testes de regressão.
- Known gaps do ciclo anterior foram tratados, mas o roadmap seguinte reabre trilhas estruturais de multi-dialeto, novos providers, snapshot de schema e validação de compatibilidade.
- Incremento desta sessão: a trilha imediata do core voltou a priorizar gaps pequenos, mas reais, de semântica compartilhada do parser/executor, começando por `LIKE ... ESCAPE ...` com regra dirigida pelo dialeto em vez de hardcode único no helper comum.
- Incremento desta sessão: a mesma trilha incremental do core passou a fechar também payloads já parseados, mas ainda subutilizados no runtime, começando por `JSON_VALUE ... RETURNING` com gate do dialeto e coerção efetiva no executor.
- Incremento desta sessão: a próxima lacuna pequena fechada no core foi DDL de `SEQUENCE`, reaproveitando a infraestrutura já existente de runtime e deixando parser/dispatcher/executor seguirem a capacidade declarada no dialeto.
- Incremento desta sessão: o parser comum de agregação textual foi endurecido para a forma nativa do MySQL (`GROUP_CONCAT(DISTINCT ... ORDER BY ... SEPARATOR ...)`), aceitando `SEPARATOR` como terminador válido do `ORDER BY` interno apenas quando o dialeto/função o suportam.
- Incremento desta sessão: a trilha auditada de regras por dialeto removeu os últimos branches comportamentais centrais por `dialect.Name` para mutações multi-tabela, rowcount de UPSERT e `SQL_CALC_FOUND_ROWS`, consolidando parser/executor/strategies sob o mesmo contrato de capability do provider.
- Incremento desta sessão: a próxima fatia funcional do executor fechou o subset principal de `PIVOT` com `SUM/MIN/MAX/AVG`, adicionou `UNPIVOT` e abriu o subset inicial de `FOR JSON` no caminho compartilhado de `SQL Server/SqlAzure`, deixando agregadores avançados, nuances tabulares por versão e arestas finas de serialização JSON como backlog residual explícito.
- TODO: executar o roadmap remanescente na ordem acordada: `DuckDB` -> `Cross Dialect Validator`, considerando `Query Plan Debugger` e `Schema Snapshot` como trilhas já materializadas no ciclo atual.
- TODO: extrair/refatorar bases compartilhadas por família antes de `DuckDB`, para evitar duplicação e preservar o parser/executor agnósticos.
- TODO: fechar a trilha auditada contra bancos reais com implementação incremental de `FOR JSON`/`STRING_SPLIT`/`CROSS APPLY`/`OUTER APPLY` (SQL Server/SqlAzure), `DISTINCT ON`/`LATERAL` (PostgreSQL), `json_each`/`json_tree` e frames avançados de window (SQLite).
- TODO: revisar cada nova feature acima com a regra "dialeto manda", garantindo gate no tokenizer/parser, contract no executor e suíte positiva/negativa por versão simulada antes de marcar o item como concluído.

#### 4.2.3 Critérios de aceitação

- Implementação estimada: **100%**.
- Cada novo recurso deve incluir cenário positivo e negativo.
- O modelo TDD-first já está amplamente adotado: Red → Green → Refactor → Harden → Document em cada fatia de feature.
- Deve existir evidência de não regressão em dialetos correlatos.
- Para concorrência transacional, o aceite inclui ausência de flaky, cobertura por versão (`MemberData*Version`) e preservação de suites de transaction reliability.
- Regressões de mensagens `NotSupportedException` no parser já estão cobertas para MySQL/SQL Server/SqlAzure/Oracle/Npgsql/DB2/SQLite.
- Incremento desta sessão: a trilha `LIKE ... ESCAPE ...` passou a ter aceite explícito positivo e negativo no core/DB2, cobrindo parse, roundtrip e avaliação parametrizada com erro acionável quando o escape não é unitário.
- Incremento desta sessão: a trilha `REGEXP` do MySQL passou a ter aceite explícito também para sensibilidade de caixa governada pelo dialeto, sem depender do comportamento padrão do runtime .NET.
- Incremento desta sessão: a suíte dedicada de parser do `SqlAzure` passou a registrar também cenários positivos e negativos do contrato compartilhado (`OFFSET/FETCH`, `JSON_VALUE`, `STRING_AGG ... WITHIN GROUP`), fechando o provider na malha de aceite cross-dialect.
- Incremento desta sessão: o `SqlAzure` passou a ter também suíte dedicada de estratégia para o contrato transacional compartilhado (`Close`/`Open`, savepoint, `ResetAllVolatileData` e isolamento), ampliando o aceite explícito fora da trilha apenas de parser.
- Cada fatia de entrega deve apresentar critérios de aceite, validação e escopo explícito no padrão dos prompts de implementação.

#### 4.2.4 Validador cross-dialect

- Implementação estimada: **0%**.
- Objetivo: informar se um SQL é compatível ou não com cada dialeto suportado, sem depender de tentativa manual provider a provider.
- O resultado precisa usar o banco/provedor real como fonte de verdade para heurísticas e gaps conhecidos, não apenas opinião do mock.
- TODO: expor `SqlCompatibilityCheck` / `ValidateAcrossDialects(query)` com saída mínima `Compatible` e `Not compatible` por dialeto.
- TODO: reutilizar capabilities reais do dialeto, regras por versão e baselines auditadas contra bancos reais para reduzir falso positivo/falso negativo.
- TODO: ligar o validador à matriz de compatibilidade, snapshots de CI e backlog de gaps para que cada divergência vire item rastreável.

### 4.3 Observabilidade de comportamento em testes

#### 4.3.1 Evidências mínimas por cenário

- Implementação estimada: **92%**.
- SQL de entrada utilizado no teste.
- Estado esperado antes/depois quando houver efeito de trigger.
- Registro do dialeto e versão simulada para facilitar reprodução.
- Incluir no hardening evidência de mensagem padronizada para não suportado e referência ao teste de regressão associado.
- CI deve publicar relatório por provider e resultado da smoke cross-dialeto como evidência mínima de fechamento.
- Incremento desta sessão: a malha CI passou a publicar também snapshot dedicado da camada `Strategy`, ampliando a trilha mínima de evidência objetiva para regressões transacionais/trigger além da smoke geral.
- TODO: anexar também o mapeamento entre evidência publicada, item do backlog e suites afetadas, para reduzir fechamento sem rastreabilidade objetiva.

#### 4.3.2 Fuzz testing e comparação multi-dialeto

- Implementação estimada: **0%**.
- Objetivo: executar a mesma query em múltiplos dialetos e produzir quadro objetivo de `OK`/`FAIL` por provider.
- Essa trilha deve complementar o validador de compatibilidade com execução comparativa e não apenas análise estática.
- TODO: adicionar um runner do tipo `TestAcrossDialects(query)` para comparar parse/execução/erro entre providers selecionados.
- TODO: registrar motivo da divergência por provider (`parse`, `gate de dialeto`, `semântica`, `resultado`) para acelerar triagem.
- TODO: usar essa trilha primeiro em regressões de compatibilidade e, depois, como base de futura expansão para `ClickHouse`/`Snowflake`.

---

## 5) Ferramentas de produtividade (extensões)

### 5.0 Objetivo de produtividade

- Reduzir tarefas repetitivas de scaffolding em times de aplicação e teste.
- Padronizar artefatos para diminuir divergências entre equipes e projetos.

### 5.1 Fluxos de geração de artefatos

#### 5.1.1 Geração de classes de teste

- Implementação estimada: **98%**.
- Fluxo principal para acelerar criação de testes automatizados.
- Apoia padronização da base de testes.
- Incremento desta sessão: a geração principal da VSIX passou a respeitar o `namespace` configurado por tipo de objeto também no conteúdo estruturado das classes geradas, reduzindo divergência entre o mapeamento salvo e o artefato emitido.
- Incremento desta sessão: a extensão VS Code deixou de gerar stub com `TODO` e passou a emitir scaffold inicial de teste com metadados de origem, método determinístico e `[Fact(Skip = ...)]`, mantendo compilação válida sem mascarar que o cenário ainda precisa ser implementado.
- Incremento desta sessão: o `Configure Mappings` da VSIX deixou de reaplicar um padrão global a toda a malha e passou a editar apenas o tipo de objeto selecionado na conexão atual, eliminando drift acidental na geração principal de classes.
- Incremento desta sessão: o diálogo `Configure Mappings` da VSIX passou a oferecer também perfis `API` e `Worker/Batch` para aplicar defaults versionados por tipo de objeto, aproximando a baseline operacional do fluxo real de geração de testes.
- Incremento desta sessão: a extensão VS Code passou a gravar também cabeçalho padronizado `// DBSqlLikeMem:*` nas classes de teste geradas, alinhando o scaffold principal ao mesmo contrato de snapshot usado pela geração estruturada da VSIX.

#### 5.1.2 Geração de classes de modelos

- Implementação estimada: **100%**.
- Geração de artefatos de aplicação além de testes.
- Útil para bootstrap inicial de camadas de domínio/dados.
- Incremento desta sessão: a trilha de templates da VSIX passou a suportar `{{Namespace}}` no conteúdo de Model, alinhando a substituição de tokens com o fluxo já existente na extensão do VS Code.
- Incremento desta sessão: a VSIX passou a permitir padrão configurável de nome de arquivo para `Model`, persistido em `TemplateConfiguration` e reaproveitado também na checagem de consistência.
- Incremento desta sessão: a extensão VS Code passou a persistir e aplicar padrão configurável de nome de arquivo para `Model`, usando o mesmo cálculo na geração e no check de consistência.
- Incremento desta sessão: a extensão VS Code passou a incluir também objetos `Sequence` no fluxo operacional de geração de Model quando a metadata real do provider os expõe, fechando o gap entre documentação, árvore e template generation.
- Incremento desta sessão: a geração de `Model` nas duas extensões passou a prependar cabeçalho padronizado `// DBSqlLikeMem:*`, preservando rastreabilidade do objeto de origem mesmo com template customizado.
- Incremento desta sessão: o snapshot gerado pela extensão VS Code para `Model` passou a incluir também estrutura mínima (`Columns`/`ForeignKeys`) quando disponível, permitindo checagem posterior de drift estrutural do artefato.
- Incremento desta sessão: a VSIX passou a reaproveitar esse snapshot também para validar coerência estrutural do `Model` frente à classe principal gerada, incluindo `Triggers` quando presentes no objeto de origem.
- Incremento desta sessão: a extensão VS Code passou a carregar também metadata real de `Sequence` no provider SQL Server e a gravá-la no snapshot de `Model`, fechando o último gap estrutural remanescente desse artefato.

#### 5.1.3 Geração de classes de repositório

- Implementação estimada: **100%**.
- Auxilia criação consistente de componentes de acesso a dados.
- Reduz repetição em soluções com múltiplos módulos.
- Incremento desta sessão: a geração de Repository na VSIX agora também injeta `{{Namespace}}` a partir do mapeamento persistido, mantendo paridade com a trilha de Model e reduzindo edição manual pós-geração.
- Incremento desta sessão: a VSIX passou a permitir padrão configurável de nome de arquivo para `Repository`, usando o mesmo resolvedor na geração e no cálculo dos artefatos complementares da consistência.
- Incremento desta sessão: a extensão VS Code passou a persistir e aplicar padrão configurável de nome de arquivo para `Repository`, mantendo o check de consistência alinhado ao arquivo efetivamente gerado.
- Incremento desta sessão: a extensão VS Code passou a tratar `Sequence` como tipo de objeto de primeira classe também na geração de Repository e no manager de mappings, removendo a assimetria que ainda deixava esse tipo só na documentação.
- Incremento desta sessão: a geração de `Repository` nas duas extensões passou a emitir também snapshot padronizado `// DBSqlLikeMem:*`, reduzindo drift silencioso entre arquivo local e objeto-fonte quando o artefato é movido/copiado manualmente.
- Incremento desta sessão: o snapshot emitido pela extensão VS Code para `Repository` passou a registrar também estrutura mínima do objeto quando disponível, reduzindo falso positivo de consistência em artefatos que mantêm nome correto mas ficaram defasados do schema.
- Incremento desta sessão: a VSIX passou a comparar também o snapshot estrutural do `Repository` com a classe principal gerada, reduzindo falso verde quando o arquivo complementar mantém identidade correta mas ficou defasado nas propriedades salvas.
- Incremento desta sessão: a extensão VS Code passou a incluir também `StartValue/IncrementBy/CurrentValue` de `Sequence` no snapshot estrutural de `Repository`, eliminando o último falso verde relevante para esse tipo de artefato.

#### 5.1.4 Ganhos operacionais

- Implementação estimada: **100%**.
- Menor tempo de setup de projeto.
- Maior consistência estrutural entre times e repositórios.
- Incremento desta sessão: a paridade de tokens entre VS Code e VSIX foi ampliada com `{{Namespace}}`, reduzindo drift entre extensões irmãs na configuração de geração.
- Incremento desta sessão: a paridade operacional entre VS Code e VSIX avançou também na geração de testes e no critério de consistência, reduzindo assimetria prática entre as duas extensões.
- Incremento desta sessão: a validação de tokens suportados em templates agora existe nas duas extensões, reduzindo risco operacional de configuração divergente entre VS Code e VSIX.
- Incremento desta sessão: a paridade operacional entre VS Code e VSIX passou a incluir também o padrão configurável de nome para `Model`/`Repository`, reduzindo setup manual e drift de nomenclatura entre times.
- Incremento desta sessão: a VSIX passou a respeitar de forma real o escopo `conexão + tipo de objeto` ao configurar mappings, removendo uma fonte de sobrescrita silenciosa que ainda atrapalhava setups multi-módulo.
- Incremento desta sessão: a extensão VS Code passou a alinhar manager, comando rápido, árvore e metadata real também para `Sequence`, reduzindo mais uma divergência operacional remanescente em relação à VSIX.
- Incremento desta sessão: a VSIX passou a consumir os mesmos perfis `api/worker` também no `Configure Mappings`, reduzindo mais uma convenção manual que ainda separava a baseline documentada do uso diário na extensão.
- Incremento desta sessão: a árvore da VSIX passou a exibir tooltip com o diagnóstico persistido da consistência, incluindo os artefatos faltantes do trio local, reduzindo mais uma assimetria operacional em relação ao detalhamento já presente no VS Code.
- Incremento desta sessão: os diálogos `Configure Mappings` e `Configure Templates` da VSIX passaram a exibir resumo operacional do perfil selecionado, tornando foco de testes, revisão planejada e recomendações de saída mais visíveis no fluxo diário.
- Incremento desta sessão: os quick picks de baseline do VS Code passaram a reaproveitar também `review-metadata.json` e a acusar drift de governança no ponto de uso, reduzindo a última assimetria relevante de contexto operacional em relação à VSIX.
- Incremento desta sessão: as duas extensões passaram a compartilhar também o contrato operacional de snapshot `// DBSqlLikeMem:*` nos artefatos gerados e a usar esse cabeçalho para detectar drift de origem, fechando a última assimetria funcional relevante da trilha de produtividade.

### 5.2 Templates e consistência

#### 5.2.1 Configuração de templates

- Implementação estimada: **100%**.
- Suporte a templates textuais com tokens:
  - `{{ClassName}}`
  - `{{ObjectName}}`
  - `{{Schema}}`
  - `{{ObjectType}}`
  - `{{DatabaseType}}`
  - `{{DatabaseName}}`
  - `{{Namespace}}`
- Permite adaptar saída para padrões internos de cada equipe.
- Incremento desta sessão: a VSIX ganhou renderizador compartilhado de tokens (`TemplateContentRenderer`) para Model/Repository e persistência de `namespace` no `ObjectTypeMapping`, fechando o gap que ainda deixava `{{Namespace}}` restrito ao fluxo do VS Code.
- Incremento desta sessão: o mesmo `namespace` passou a ser aceito também no padrão de nome de arquivo da VSIX via `{Namespace}`, mantendo coerência entre conteúdo gerado, preview de conflitos e checagem de consistência.
- Incremento desta sessão: o fluxo rápido `Configure Mappings` da extensão VS Code passou a preservar/configurar `namespace`, evitando que a capacidade já presente no manager visual fosse perdida ao reconfigurar mapeamentos pelo comando contextual.
- Incremento desta sessão: `Configure Templates` na extensão VS Code passou a oferecer perfis prontos baseados em `templates/dbsqllikemem/vCurrent`, reduzindo configuração manual e removendo a dependência de caminhos fictícios de exemplo.
- Incremento desta sessão: o diálogo `Configure Templates` da VSIX passou a aplicar diretamente os perfis `api` e `worker` quando encontra `templates/dbsqllikemem`, evitando drift entre as duas extensões irmãs no consumo da baseline.
- Incremento desta sessão: a VSIX passou a validar templates customizados contra um catálogo explícito de tokens suportados antes de salvar a configuração, reduzindo risco de placeholders que o runtime não consegue substituir.
- Incremento desta sessão: a extensão VS Code passou a validar templates customizados no salvamento e também a fazer fallback para o template padrão quando encontra tokens inválidos durante a geração.
- Incremento desta sessão: a configuração de templates da VSIX passou a persistir também padrões de nome de arquivo para `Model`/`Repository`, eliminando o nome fixo que ainda limitava os fluxos baseados em template.
- Incremento desta sessão: a extensão VS Code passou a persistir e aplicar também padrões de nome de arquivo para `Model`/`Repository`, fechando a paridade de configuração com a VSIX.

#### 5.2.2 Check visual de consistência

- Implementação estimada: **100%**.
- Indicação de ausência, divergência ou sincronização de artefatos.
- Apoia revisão rápida antes de commit/publicação.
- Incremento desta sessão: a extensão VS Code passou a validar de fato o trio `teste + model + repository` por objeto, usando os caminhos determinísticos da própria geração em vez de conferir apenas Model/Repository.
- Incremento desta sessão: a VSIX passou a distinguir explicitamente o caso de trio local incompleto (`classe/model/repositório`) antes da comparação de metadados, alinhando o estado visual intermediário ao critério já adotado no VS Code.
- Incremento desta sessão: a extensão VS Code passou a persistir o detalhe dos artefatos faltantes por objeto, reaproveitando helper puro para classificar `ok/partial/missing`, exibindo tooltip na árvore e limpando estado residual quando o trio volta a ficar íntegro.
- Incremento desta sessão: a VSIX passou a expor tooltip na árvore com a mensagem persistida da checagem e a listar os artefatos faltantes (`class/model/repository`) em ordem determinística, aproximando a leitura visual do diagnóstico ao fluxo já consolidado no VS Code.
- Incremento desta sessão: a extensão VS Code passou a diferenciar também `drift` de artefato, usando cabeçalhos `// DBSqlLikeMem:*` em `teste/model/repository` para detectar quando o arquivo presente não corresponde ao objeto atualmente selecionado.
- Incremento desta sessão: a VSIX passou a aplicar a mesma detecção de `drift` sobre `class/model/repository`, lendo o snapshot `// DBSqlLikeMem:*` dos três artefatos antes da comparação com o banco e fechando a última lacuna funcional dessa checagem visual.

#### 5.2.3 Estratégia de governança

- Implementação estimada: **100%**.
- Versionar templates junto ao repositório quando possível.
- Definir baseline de geração por tipo de projeto.
- Incremento desta sessão: o repositório passou a versionar uma baseline física em `templates/dbsqllikemem/vCurrent`, com catálogo explícito no core (`TemplateBaselineCatalog`) e trilha `vNext` reservada para a próxima promoção controlada.
- Incremento desta sessão: `scripts/check_release_readiness.py` agora valida presença e contrato mínimo dessas baselines versionadas, transformando a governança de templates em gate automatizado e não só convenção documental.
- Incremento desta sessão: o mesmo catálogo passou a resolver a raiz mais próxima do repositório para reaproveitamento pela VSIX, eliminando necessidade de duplicar caminhos fixos na UI.
- Incremento desta sessão: o contrato de placeholders suportados foi centralizado em `TemplateTokenCatalog`, com checagem de tokens inválidos na VSIX e checklist de revisão periódica versionado junto da baseline.
- Incremento desta sessão: a extensão VS Code passou a aplicar o mesmo contrato de placeholders suportados no fluxo operacional de configuração/geração, reduzindo risco de governança divergente entre as duas ferramentas.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a falhar também quando alguma baseline versionada usa placeholders `{{...}}` fora do contrato suportado, fechando o loop de governança no artefato publicado.
- Incremento desta sessão: os perfis `api` e `worker` passaram a orientar também os defaults de mapeamento de testes na extensão VS Code, reduzindo mais uma fonte de convenção solta fora da baseline operacional.
- Incremento desta sessão: a trilha de revisão periódica passou a ter metadado versionado em `templates/dbsqllikemem/review-metadata.json`, com cadência, última revisão, próxima janela-alvo e evidências mínimas validadas pelo auditor de release.
- Incremento desta sessão: a VSIX passou a consumir a mesma baseline versionada também no diálogo `Configure Mappings`, reaproveitando o catálogo central para aplicar defaults por tipo de objeto sem duplicar convenções na UI.
- Incremento desta sessão: a apresentação da baseline na VSIX foi centralizada em formatter compartilhado do core, mantendo descrição, foco, revisão e recomendação por tipo sob a mesma fonte de verdade do catálogo versionado.
- Incremento desta sessão: a governança da baseline passou a detectar e expor também drift entre `review-metadata.json` e o catálogo do core, reduzindo risco de divergência silenciosa nos diálogos da VSIX.
- Incremento desta sessão: a extensão VS Code passou a consumir o mesmo `review-metadata.json` nos quick picks de baseline de templates e mappings, expondo cadência, evidências e drift de governança sem depender só da VSIX para esse feedback operacional.

### 5.3 Padrões recomendados para adoção em equipe

#### 5.3.1 Template baseline por tipo de solução

- Implementação estimada: **100%**.
- API: foco em repositórios e testes de integração leve.
- Worker/Batch: foco em comandos DML e validação de consistência.
- Incremento desta sessão: perfis iniciais `api` e `worker` foram materializados em `templates/dbsqllikemem/vCurrent`, com templates de Model/Repository e diretórios sugeridos distintos para cada tipo de solução.
- Incremento desta sessão: a VSIX agora também consome operacionalmente esses perfis no próprio diálogo de configuração, em vez de deixá-los apenas como convenção documental/manual.
- Incremento desta sessão: a extensão VS Code passou a consumir também os padrões de nome presentes nesses perfis, eliminando divergência residual entre baseline documentada e saída efetiva da geração.
- Incremento desta sessão: a extensão VS Code passou a oferecer também defaults de mapeamento de testes coerentes com o perfil selecionado (`API` com foco em integração leve; `Worker/Batch` com foco em consistência), aproximando a baseline do fluxo real de adoção em equipe.
- Incremento desta sessão: a mesma trilha de defaults por perfil no VS Code passou a cobrir também `Sequence`, evitando que o último tipo suportado pela documentação ficasse fora da baseline operacional adotada pela equipe.
- Incremento desta sessão: a VSIX passou a aplicar esses mesmos perfis também no `Configure Mappings`, cobrindo `Table`, `View`, `Procedure` e `Sequence` com defaults recomendados diretamente no fluxo de adoção da equipe.
- Incremento desta sessão: a VSIX passou a exibir também o contexto operacional desses perfis diretamente nos diálogos, reduzindo a distância entre baseline documentada e decisão efetiva de adoção por solução/equipe.
- Incremento desta sessão: o VS Code passou a exibir no quick pick dos perfis o mesmo contexto operacional de revisão/cadência/evidências, aproximando a decisão de adoção em equipe do contrato versionado da baseline.
- Incremento desta sessão: os resumos compartilhados de baseline na VSIX e no VS Code passaram a explicitar também os diretórios recomendados de saída para `Model` e `Repository`, fechando o último gap entre catálogo versionado e decisão operacional no ponto de configuração.

#### 5.3.2 Revisão periódica de templates

- Implementação estimada: **100%**.
- Revisão trimestral para refletir novas convenções arquiteturais.
- Checklist de compatibilidade antes de atualizar templates compartilhados.
- Incremento desta sessão: `templates/dbsqllikemem/vNext/README.md` formaliza a trilha de promoção da próxima baseline e amarra a atualização ao backlog, status operacional e changelog.
- Incremento desta sessão: `templates/dbsqllikemem/review-checklist.md` formaliza a revisão de tokens, promoção de baseline e paridade entre VSIX/VS Code, e o auditor passou a vigiar sua presença/contrato mínimo.
- Incremento desta sessão: o auditor agora verifica também se as baselines versionadas continuam usando apenas placeholders suportados, transformando o checklist de revisão em regra objetiva.
- Incremento desta sessão: `templates/dbsqllikemem/review-metadata.json` passou a registrar a última revisão executada e a próxima janela planejada em formato estruturado, e o auditor valida datas, baseline corrente, staging path e arquivos mínimos de evidência.
- Incremento desta sessão: a próxima janela de revisão e a cadência do perfil passaram a aparecer diretamente nos diálogos da VSIX, reduzindo o risco de a revisão periódica ficar restrita ao arquivo `review-metadata.json`.
- Incremento desta sessão: os diálogos da VSIX passaram a acusar explicitamente quando o metadata versionado de revisão diverge do catálogo de baseline, reforçando a revisão periódica como regra operacional e não apenas convenção documental.
- Incremento desta sessão: os quick picks equivalentes do VS Code passaram a mostrar também a última revisão, a próxima janela planejada, a contagem de evidências e o drift de governança, reforçando a revisão periódica no fluxo diário fora da VSIX.
- Incremento desta sessão: VSIX, VS Code e `scripts/check_release_readiness.py` passaram a tratar `nextPlannedReviewOn` vencido como gap operacional explícito, transformando a cadência trimestral em regra objetiva também depois que o metadata já existe.

---

## 6) Distribuição e ciclo de vida

### 6.0 Objetivo de ciclo de vida

- Assegurar distribuição estável para consumidores legados e modernos.
- Garantir alinhamento entre versão de pacote, documentação e ferramentas associadas.

### 6.1 Targets e compatibilidade .NET

#### 6.1.1 Bibliotecas de provedores

- Implementação estimada: **100%**.
- Alvos configurados centralmente em `src/code/Directory.Build.props`: `.NET Framework 4.6.2`, `.NET Standard 2.0` e `.NET 8.0`.
- `net6.0` aparece no override para projetos `.Test` e `.TestTools`, não como target das bibliotecas de produção.

#### 6.1.2 Núcleo DbSqlLikeMem

- Implementação estimada: **100%**.
- Alvos configurados centralmente em `src/code/Directory.Build.props`: `.NET Framework 4.6.2`, `.NET Standard 2.0` e `.NET 8.0`.
- Estratégia atual maximiza reuso entre legado (`net462`), compatibilidade ampla (`netstandard2.0`) e runtime moderno (`net8.0`); `net6.0` fica concentrado na malha de testes conforme o override central.

#### 6.1.3 Implicações para consumidores

- Implementação estimada: **98%**.
- Projetos antigos e novos podem adotar a biblioteca com fricção reduzida.
- Planejamento de upgrade pode ser progressivo.
- Incremento desta sessão: `README.md` da raiz foi corrigido para refletir os alvos reais do repositório (`net462`, `netstandard2.0`, `net8.0`, com `net6.0` restrito a `.Test`/`.TestTools`), removendo referências antigas a `net48`, `net10.0` e `netstandard2.1`.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a vigiar esse contrato documental também no `README.md`, reduzindo risco de descompasso para consumidores que entram pelo guia principal do repositório.
- Incremento desta sessão: `src/README.md` também foi alinhado ao mesmo contrato de targets/override e entrou na trilha de auditoria, reduzindo drift entre documentação de pacote e documentação raiz.
- Incremento desta sessão: `docs/getting-started.md` passou a explicitar o mesmo contrato de frameworks/override e também entrou na trilha de auditoria, reduzindo ambiguidade para consumidores que chegam pelo guia de instalação.
- Incremento desta sessão: `docs/Wiki/Getting-Started.md` foi alinhado ao mesmo contrato de frameworks/override e entrou na auditoria, reduzindo drift entre wiki espelhada e documentação canônica.
- Incremento desta sessão: `docs/old/providers-and-features.md` passou a explicitar o contrato central de frameworks para consumidores e entrou na auditoria, reduzindo drift no guia secundário de compatibilidade por provider.
- TODO: manter o mesmo contrato de compatibilidade em novos pontos de entrada de documentação/pacote assim que surgirem artefatos ou providers adicionais, evitando regressão documental silenciosa.

### 6.2 Publicação

#### 6.2.1 NuGet

- Implementação estimada: **97%**.
- Fluxo de empacotamento e distribuição de pacotes.
- Controle de versão semântica para evolução previsível.
- Incremento desta sessão: validação de metadados dos `.nupkg` foi extraída para `scripts/check_nuget_package_metadata.py`, removendo lógica inline duplicada do workflow `nuget-publish.yml` e permitindo auditoria local pós-pack.
- Incremento desta sessão: `docs/nuget-readiness-validation-report.md` foi alinhado ao estado atual do `Directory.Build.props`, incluindo presença de `PackageLicenseExpression` e trilha explícita de auditoria pós-pack.
- Incremento desta sessão: `scripts/check_nuget_package_metadata.py` passou a usar `src/code/Directory.Build.props` como fonte de verdade para validar `authors`, `repository`, `projectUrl`, `readme`, `tags`, `releaseNotes` e licença do `.nuspec`, além da presença física do `README.md` dentro do pacote.
- Incremento desta sessão: o mesmo gate pós-pack passou a validar também `requireLicenseAcceptance` no `.nuspec`, reaproveitando `PackageRequireLicenseAcceptance` do `src/code/Directory.Build.props` e cobrindo esse contrato com `unittest` dedicado.
- Incremento desta sessão: o workflow `nuget-publish.yml` passou a respeitar opcionalmente `vars.NUGET_PUBLISH_ENVIRONMENT` com fallback para `nuget-publish`, alinhando o contrato documentado de Environment ao YAML real e ao auditor de readiness.
- Incremento desta sessão: o workflow `nuget-publish.yml` passou a executar também `scripts/check_release_readiness.py` antes do `restore`, levando o gate documental/operacional do release para o próprio fluxo de publicação NuGet e prendendo isso no `unittest` do auditor.
- TODO: ampliar o gate NuGet para símbolos/source metadata e demais artefatos opcionais de publicação quando essa trilha entrar no processo oficial de release.

#### 6.2.2 Extensões IDE

- Implementação estimada: **94%**.
- Publicação VSIX (Visual Studio).
- Publicação de extensão VS Code.
- Expande adoção em diferentes perfis de desenvolvedor.
- Incremento desta sessão: metadados objetivos de repositório/bugs/homepage da extensão VS Code e `repo` do manifesto VSIX foram alinhados ao repositório oficial, reduzindo drift documental antes da publicação.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a validar também scripts/arquivos essenciais do pacote VS Code, `activationEvents` apontando para comandos/views existentes e campos mínimos (`overview`, `tags`, `categories`) do manifesto de publicação VSIX.
- Incremento desta sessão: documentação da VSIX foi alinhada ao suporte real (`Visual Studio 2022+`) e a auditoria passou a cruzar `MinimumVisualStudioVersion` do projeto com o range suportado no `source.extension.vsixmanifest`.
- Incremento desta sessão: workflows `vsix-publish.yml` e `vscode-extension-publish.yml` passaram a executar o auditor de readiness antes do empacotamento; no caso da VSIX, o publish usa `--strict-marketplace-placeholders` para impedir publicação com `publisher` não resolvido.
- Incremento desta sessão: o pacote VS Code passou a ter validação de placeholders `%...%` contra `package.nls*.json` e presença de `l10n`, reduzindo drift de metadata/localização antes do publish.
- Incremento desta sessão: os READMEs operacionais das extensões VS Code/VSIX entraram na trilha de auditoria e o README da VSIX passou a expor workflow, manifesto e gate estrito de publicação, reduzindo drift entre pacote e instrução operacional.
- Incremento desta sessão: a documentação operacional das extensões também passou a explicitar a fonte de versão (`package.json`/`source.extension.vsixmanifest`) e o prefixo de tag de publicação, alinhando instrução humana e workflow automatizado.
- Incremento desta sessão: os workflows `vsix-publish.yml` e `vscode-extension-publish.yml` passaram a validar explicitamente a presença da fonte de versão antes do build/pack, reduzindo drift entre o prefixo de tag documentado e o artefato efetivamente publicado.
- Incremento desta sessão: os READMEs operacionais das extensões passaram a explicitar também o contrato `workflow -> fonte de versão -> publish`, e o auditor passou a vigiar essa mensagem diretamente no ponto de uso.
- Gap remanescente explicitado: o `publisher` final do Visual Studio Marketplace ainda depende de definição operacional externa ao código.
- TODO: fechar a definição operacional do `publisher`/identidade final de marketplace e automatizar a última etapa que hoje ainda depende de valor externo ao repositório.

#### 6.2.3 Operação contínua

- Implementação estimada: **100%**.
- Checklist de release para validação de artefatos.
- Sincronização entre documentação, pacote e extensões.
- Incremento desta sessão: `docs/publishing.md` passou a incluir checklist explícito de release conectando versão, `CHANGELOG.md`, backlog, status operacional e snapshots cross-dialect (`smoke`/`aggregation`/`parser`/`strategy`) antes da publicação.
- Incremento desta sessão: auditoria executável de readiness adicionada em `scripts/check_release_readiness.py`, reaproveitando a validação estrutural dos snapshots e conferindo presença/coerência de workflows, documentação e metadados de publicação.
- Incremento desta sessão: workflow `provider-test-matrix.yml` passou a validar também o novo auditor (`py_compile`, `--help` e execução padrão) na etapa de automações.
- Incremento desta sessão: o gate de metadados NuGet foi extraído para `scripts/check_nuget_package_metadata.py`, integrando automação pós-pack reutilizável e eliminando duplicação de lógica no pipeline de publicação.
- Incremento desta sessão: a mesma auditoria passou a cobrir integridade mínima das extensões, reduzindo a dependência de revisão manual nos fluxos VSIX/VS Code antes do publish.
- Incremento desta sessão: a mesma trilha agora valida também coerência de compatibilidade declarada da VSIX (`MinimumVisualStudioVersion` x range do manifesto), reduzindo drift entre build/publish/docs.
- Incremento desta sessão: os próprios workflows de publish das extensões agora consomem o auditor de readiness, trazendo o gate para o ponto exato de publicação em vez de deixá-lo apenas no pipeline geral.
- Incremento desta sessão: a automação geral também passou a executar `check_nuget_package_metadata.py --allow-missing-artifacts`, validando CLI/integração do gate NuGet mesmo fora do fluxo de `pack`.
- Incremento desta sessão: o gate documental foi estendido também aos READMEs operacionais das extensões, reduzindo risco de workflow/manifests estarem corretos enquanto a instrução de publicação do próprio artefato deriva.
- Incremento desta sessão: a auditoria contínua de release passou a falhar também quando a revisão trimestral das baselines versionadas expira, conectando governança de templates e readiness de publicação no mesmo gate executável.
- Incremento desta sessão: o contrato de Environment do publish NuGet (`vars.NUGET_PUBLISH_ENVIRONMENT` com fallback `nuget-publish`) passou a ser validado também pelo auditor, reduzindo drift entre documentação e workflow.
- Workflow CI matricial por provider e smoke cross-dialeto inicial já suportam auditoria contínua de regressão.
- Evolução de concorrência deve separar rotinas CI em smoke vs completo, com traits por categoria (isolamento, savepoint, conflito de escrita, stress).
- Próximos ciclos incluem trilhas de observabilidade, performance, concorrência e ecossistema (.NET/ORM/tooling) já descritas no pipeline de prompts e no plano executável P7–P14.

### 6.3 Organização da solução e ritmo de desenvolvimento

#### 6.3.1 Arquivo de solução (`.slnx`) e cobertura de projetos

- Implementação estimada: **99%**.
- Solução `DbSqlLikeMem.slnx` já estruturada por domínio/provedor e pronta para uso no Visual Studio 2026.
- Validação operacional indica cobertura completa dos projetos `*.csproj` do repositório na solução.
- Verificação automatizada já adicionada ao CI via `scripts/check_slnx_project_coverage.py` e com alternativa local Windows em `scripts/check_slnx_project_coverage.ps1` para detectar drift entre árvore `src` e conteúdo da solução.
- Incremento desta sessão: o checker Python passou a normalizar separadores de caminho também nos `Project Path="..."` lidos do `.slnx`, com suíte `unittest` dedicada para evitar falso positivo quando a solução usa `\` no Windows e a validação roda com `/` no CI Linux.
- Incremento desta sessão: o checker da solução passou a sinalizar também projetos de raiz fora dos buckets compartilhados aprovados, reduzindo drift de organização por domínio/provedor quando novos projetos entram no repositório.

#### 6.3.2 Matriz compartilhada de testes por capability

- Implementação estimada: **94%**.
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
  - base compartilhada de agregação textual passou a expor helpers neutros para ordenação interna nativa da agregação, permitindo cobrir o caminho SQLite `GROUP_CONCAT(... ORDER BY ...)` sem duplicar seed/assert específico no provider.
  - o mesmo contrato compartilhado passou a cobrir também o caminho nativo do MySQL (`GROUP_CONCAT(... ORDER BY ... SEPARATOR ...)`), mantendo o runtime comum e limitando a variação ao parser/capability do dialeto.
- Próximos incrementos da capability matrix:
  - ampliar contratos compartilhados para cenários adicionais de ordenação dentro da agregação textual quando habilitados por dialeto além das trilhas já cobertas (`WITHIN GROUP`, sintaxe nativa do SQLite e sintaxe nativa do MySQL);
  - expandir bloco comum para cenários de `CASE` com literais textuais e numéricos mistos no mesmo campo (coerção implícita cross-dialect);
  - consolidar assertions de mensagens de erro para `NotSupported` em uma camada única reutilizável.

#### 6.3.3 Entrada única de execução (build/test)

- Implementação estimada: **98%**.
- Script padronizado já existe para smoke cross-provider (`run_cross_dialect_equivalence.sh`); a trilha desta sessão adicionou também os perfis `parser` e `strategy`, consolidando uma entrada única incremental para core/smoke, agregação Dapper, regressão dedicada de parser e regressão comportamental da camada Strategy.
- Perfis de execução já explícitos no runner (`smoke`/`aggregation`/`parser`/`strategy`) para acelerar feedback local e CI; modo `--continue-on-error` permite varredura completa com resumo de falhas por execução e snapshots com quadro-resumo por perfil; `--dry-run` permite inspecionar a matriz planejada sem execução de testes.
- O perfil `parser` cobre MySQL, SQL Server, SQL Azure, Oracle, Npgsql, SQLite e DB2 usando o trait compartilhado `Category=Parser`; para `SqlAzure`, a suíte dedicada valida o mapeamento entre nível de compatibilidade e gates do dialeto SQL Server compartilhado.
- O perfil `strategy` cobre MySQL, SQL Server, SQL Azure, Oracle, Npgsql, SQLite e DB2 usando o trait compartilhado `Category=Strategy`, trazendo para a entrada única a mesma trilha que já existia dispersa nos projetos por provider.
- Refresh em lote e validação estrutural dos snapshots agora também contemplam os perfis `parser` e `strategy`, com placeholders versionados em `docs/` e jobs dedicados no workflow `provider-test-matrix.yml` para publicação dos artefatos correspondentes.
- CI inclui job dedicado de validação de automações (sintaxe shell, `py_compile`, `unittest`, `--help`, check `.slnx` e validação estrutural dos snapshots markdown) antes da matriz de testes por provider.
- Vincular categorias/traits para habilitar execução seletiva por domínio de regressão.

#### 6.3.4 Governança do backlog de documentação

- Implementação estimada: **100%**.
- Incremento desta sessão: status operacional separado em `docs/features-backlog/status-operational.md`, definindo o `index.md` como visão estável e o novo arquivo como trilha de sprint/andamento para reduzir conflito de merge em percentuais e notas voláteis.
- Incremento desta sessão: checklist de evidência mínima formalizado em `docs/features-backlog/progress-update-checklist.md`, cobrindo item do backlog, arquivos/testes afetados, providers, comando/resultado, limitação conhecida e mitigação de descompasso documental.
- Incremento desta sessão: template de PR adicionado em `.github/pull_request_template.md`, exigindo vínculo explícito entre mudança de código, testes afetados, atualização do backlog, providers cobertos e evidência de validação.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a verificar presença e contrato mínimo do checklist de evidência e do template de PR, transformando a convenção documental em gate automatizado.
- Incremento desta sessão: `docs/Wiki/Home.md` teve links corrigidos para o repositório oficial e essa base passou a ser verificada pelo mesmo auditor, reduzindo drift entre docs canônicos e wiki espelhada.
- Incremento desta sessão: `docs/Wiki/Getting-Started.md` entrou na mesma trilha de auditoria dos guias principais, ampliando a governança de docs espelhados sem criar um fluxo paralelo de revisão.
- Incremento desta sessão: `docs/info/multi-target-compat-audit.md` passou a identificar explicitamente seu caráter histórico e, quando presente no checkout, o auditor valida essa advertência para reduzir risco de leitura equivocada de artefatos estáticos fora da trilha canônica.
- Incremento desta sessão: `docs/Wiki/Publishing.md` e `docs/Wiki/Providers-and-Compatibility.md` entraram no gate documental do auditor, estendendo a governança para as demais páginas espelhadas mais acessadas.
- Incremento desta sessão: os índices `docs/README.md` e a wiki em `docs/Wiki` passaram a expor a trilha de versão/tag por artefato, reduzindo drift já no ponto de descoberta da documentação.
- Incremento desta sessão: a trilha de baselines versionadas em `templates/dbsqllikemem` passou a ser exposta nos READMEs relevantes e validada pelo auditor, conectando backlog, docs e artefatos reais de geração no mesmo gate.
- Incremento desta sessão: o checklist de revisão periódica dos templates entrou no mesmo gate documental, conectando a governança de baseline ao contrato operacional do backlog.
- Incremento desta sessão: o gate documental/evidencial passou a incluir também a validade do contrato de placeholders nas baselines versionadas, reduzindo risco de backlog/documentação afirmarem suporte a templates que o runtime não renderiza.
- Incremento desta sessão: o auditor e os pontos de entrada da documentação foram alinhados ao caminho canônico da wiki espelhada em submódulo (`docs/Wiki`), com compatibilidade defensiva ao layout legado e cobertura explícita do playbook `docs/wiki_setup/README.md`.
- Convenção operacional adotada para os próximos ciclos:
  - toda atualização de percentual deve registrar evidência objetiva (arquivo de teste, comando executado e resultado);
  - itens com escopo multi-provider devem indicar explicitamente onde houve cobertura total e onde permanece gap;
  - quando houver apenas atualização documental, incluir seção de risco de descompasso com o código e ação de mitigação planejada.

### 6.4 Política sugerida de versionamento

#### 6.4.1 SemVer para consumidores

- Implementação estimada: **99%**.
- Incremento major para quebras comportamentais/documentadas.
- Incremento minor para novos recursos compatíveis.
- Incremento patch para correções sem alteração contratual.
- Auditoria operacional agora valida presença centralizada da versão em `src/code/Directory.Build.props`, reduzindo risco de release documental sem referência de versão.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a validar formato SemVer no núcleo e nas extensões (VS Code/VSIX), endurecendo a trilha de versionamento sem forçar igualdade artificial entre artefatos distintos.
- Incremento desta sessão: `docs/publishing.md`, wiki e READMEs das extensões passaram a explicitar também a fonte de verdade da versão por artefato (`Directory.Build.props`, `source.extension.vsixmanifest`, `package.json`) e o prefixo de tag correspondente; o auditor agora vigia esse contrato.
- Incremento desta sessão: `scripts/check_nuget_package_metadata.py` passou a validar também a versão efetivamente publicada no `.nuspec` contra `src/code/Directory.Build.props` e o sufixo do arquivo `.nupkg`, reduzindo risco de pacote NuGet sair com SemVer divergente da fonte de verdade central.
- Incremento desta sessão: os workflows de publish passaram a validar explicitamente a presença da fonte de versão de cada artefato (`Directory.Build.props`, `source.extension.vsixmanifest`, `package.json`), e o auditor agora exige esse contrato para manter tag, arquivo-fonte e publish sob a mesma trilha verificável.
- Incremento desta sessão: o auditor de release passou a emitir uma sugestão de impacto SemVer a partir das notas de `CHANGELOG.md` em `## [Unreleased]`, reduzindo subjetividade na triagem entre `PATCH`, `MINOR` e `MAJOR`.

#### 6.4.2 Comunicação de mudanças

- Implementação estimada: **99%**.
- Incremento desta sessão: `CHANGELOG.md` adicionado na raiz com estrutura orientada a impacto por provedor/dialeto, automação cross-dialect e limitações ainda abertas da release corrente.
- Incremento desta sessão: `CHANGELOG.md` e `docs/publishing.md` passaram a incorporar a nova trilha de auditoria de release e o gap remanescente do publisher VSIX, tornando a limitação visível antes da publicação.
- Incremento desta sessão: a documentação de release passou a registrar explicitamente que a auditoria também valida SemVer dos artefatos publicados, deixando o critério de governança mais explícito para revisão humana.
- Incremento desta sessão: comunicação de release agora inclui mapeamento explícito entre artefato, arquivo-fonte da versão e prefixo de tag (`v*`, `vsix-v*`, `vscode-v*`) nos guias principais e espelhados, reduzindo ambiguidade operacional.
- Incremento desta sessão: `scripts/check_release_readiness.py` passou a validar também o contrato mínimo de comunicação de release (`CHANGELOG.md` com `Unreleased` + subseções + `Known limitations still open`, além de referências explícitas a release notes nos guias de publicação e nos READMEs das extensões), tornando release notes um gate objetivo.
- Incremento desta sessão: os READMEs operacionais das extensões passaram a repetir explicitamente o contrato entre workflow, fonte de versão e prefixo de tag, reduzindo ambiguidade no ponto de execução manual do publish.
- Incremento desta sessão: o contrato de comunicação por artefato passou a ficar visível também dentro dos próprios workflows de publish, que agora expõem e validam a fonte de versão associada ao prefixo de tag documentado.
- Changelog orientado a impacto por provedor/dialeto.
- Destaque para gaps fechados e limitações ainda abertas.
- TODO: gerar resumo de impacto por provider/dialeto a partir do backlog/changelog para reaproveitar a mesma mensagem em release notes, wiki e comunicação operacional.

---

## 7) Mapa de TODOs restantes

### 7.0 Núcleo e DDL

- Objetos programáveis: `PROCEDURE`, `TRIGGER` e variantes avançadas de `FUNCTION`.

### 7.1 Execução avançada

- `FOR JSON`, `CROSS APPLY/OUTER APPLY`, `LATERAL` e `DISTINCT ON`.
- `json_each/json_tree`, `PIVOT/UNPIVOT` e outras formas tabulares por provider.
- Batches mistos com `RETURNING`, `OUTPUT`, rowcount e trigger.

### 7.2 Famílias futuras

- `DuckDB`.
- `ClickHouse`.
- `Snowflake`.

### 7.3 Camada compartilhada

- `SqlCompatibilityCheck` / `ValidateAcrossDialects(query)`.
- `TestAcrossDialects(query)`.
- Matriz cross-dialect e snapshots de CI.

### 7.4 Observabilidade e release

- Resumo de impacto por provider/dialeto.
- Governança de SemVer e publicação.

### 7.5 Pendências por provider

- MySQL: ampliar o pruning fora do subset seguro de `YEAR` e `EXTRACT`.
- SQL Server/SqlAzure: fechar nuances avançadas de `FOR JSON`, `CROSS APPLY/OUTER APPLY`, `PIVOT/UNPIVOT`, `STRING_SPLIT` e `STRING_AGG`.
- Oracle: completar `CONNECT BY/START WITH`, `MATCH_RECOGNIZE` e `MODEL`.
- PostgreSQL/Npgsql: fechar `DISTINCT ON`, `LATERAL` e `MERGE`.
- SQLite: completar `MATERIALIZED/NOT MATERIALIZED`, `json_each/json_tree` e frames avançados.

## 8) Mapa de aprofundamento sugerido

### 8.0 Como usar este índice no dia a dia

- Planejamento de sprint: usar as seções 1–4 para quebrar itens técnicos.
- Definição de padrões internos: usar seção 5 para operacionalizar templates e geração.
- Preparação de release: usar seção 6 como checklist de governança.

### 8.1 Primeiro nível (macro)

- Entender proposta do engine em memória.
- Mapear provedores usados no contexto do produto.
- Definir fronteira entre teste unitário e integração.

### 8.2 Segundo nível (funcional)

- Explorar parser/executor e API fluente.
- Consolidar padrões de seed e setup.
- Validar cenários críticos com Dapper/ADO.NET.

### 8.3 Terceiro nível (especialização)

- Monitorar métricas e planos de execução mock.
- Trabalhar gaps por dialeto com regressão automatizada.
- Refinar matriz de compatibilidade por domínio de negócio.

### 8.4 Quarto nível (ecossistema)

- Incorporar fluxos de extensão e templates no dia a dia.
- Padronizar publicação e governança documental.
- Manter backlog evolutivo com trilhas por prioridade.

### 8.5 Quinto nível (estratégia de produto)

- Definir roadmap anual de compatibilidade SQL.
- Balancear manutenção de legado e inovação de recursos.
- Criar indicadores de adoção e qualidade para direcionar próximos ciclos.
