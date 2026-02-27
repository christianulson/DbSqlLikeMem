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
- Implementação estimada: **72%**.
- Processamento de comandos de escrita e leitura.
- Tradução da consulta para operações no estado em memória.
- Preservação da experiência de uso próxima ao fluxo SQL tradicional.

#### 1.2.3 Regras por dialeto e versão
- Implementação estimada: **88%**.
- Ativa/desativa construções sintáticas por provedor e versão.
- Trata incompatibilidades históricas entre bancos diferentes.
- Direciona comportamento esperado em testes de compatibilidade.
- Checklist de known gaps indica cobertura concluída para MERGE por dialeto, WITH RECURSIVE e normalização de paginação/quoting.

#### 1.2.4 Governança de evolução do parser
- Implementação estimada: **92%**.
- Backlog guiado por gaps observados em testes reais.
- Track global de normalização Parser/AST consolidado em ~90%, com foco atual em refinos finais por dialeto.
- Priorização por impacto em frameworks de acesso a dados.
- Expansão incremental para reduzir regressões.
- Backlog operacional segue cadência priorizada P0→P14 para reduzir dispersão de implementação entre parser/executor/docs.

#### 1.2.5 Funções SQL agregadoras e de composição de texto
- Implementação estimada: **68%**.

#### 1.2.6 Funções de data/hora cross-dialect
- Implementação estimada: **40%**.
- Consolidar no `dialect` o catálogo de funções temporais sem argumento (data, hora e data/hora).
- Garantir suporte de avaliação tanto para função com parênteses quanto para tokens sem parênteses em `SELECT`, `WHERE`, `HAVING` e expressões de `INSERT/UPSERT`.
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
- Implementação estimada: **62%**.
- Fluxo macro: parse → validação → execução no estado em memória → materialização de resultado.
- Track global de alinhamento de runtime estimado em ~55%, com evolução incremental por contracts de dialeto.
- Recalibrado por evidências de código: executor AST, estratégias de mutação por dialeto e ampla suíte `*StrategyTests`/`*GapTests` por provider.
- Tratamento de execução orientado por semântica do dialeto escolhido.
- Retorno previsível para facilitar asserts em testes.

#### 1.3.2 Operações comuns suportadas
- Implementação estimada: **82%**.
- Fluxos DDL/DML de uso frequente em aplicações corporativas .NET.
- Cenários com múltiplos comandos por contexto de teste.
- Execução orientada a simulação funcional (não benchmark de banco real).
- Tracker de concorrência transacional aponta evolução atual em ~35%, com base compartilhada de testes em 100% e próximas fases focadas em isolamento/visibilidade/savepoint/stress.
- Known gaps concluídos reforçam UPDATE/DELETE com JOIN multi-tabela e evolução de JSON por provider com bloqueio padronizado quando não suportado.
- Roadmap operacional cobre SQL Core, composição de consulta, SQL avançado, DML avançado e paginação por versão.
- Plano executável P7–P14 aponta trilhas ativas para UPSERT/UPDATE/DELETE avançados (P7), paginação/ordenação (P8) e JSON por provider (P9).

#### 1.3.3 Resultados e consistência
- Implementação estimada: **84%**.
- Entrega de resultados em formatos esperados por consumidores ADO.NET.
- Coerência entre operação executada e estado final da base simulada.
- Comportamento determinístico para repetição do mesmo script.
- Hardening recente reforçou previsibilidade de regressão com foco em mensagens de erro não suportado e consistência de diagnóstico.
- Checklist operacional confirma padronização de `SqlUnsupported.ForDialect(...)` no runtime para fluxos não suportados.

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
- Escolha de provedor por string/configuração (`mysql`, `sqlserver`, `oracle`, `postgresql`, `sqlite`, `db2`).
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
- Implementação estimada: **82%**.
- Parser/executor para DDL/DML comuns.
- Suporte a `INSERT ... ON DUPLICATE KEY UPDATE`.
- Backlog de funções: ampliar cobertura de `GROUP_CONCAT` em cenários com `GROUP BY`.
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
- Implementação estimada: **82%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Backlog de funções: priorizar `STRING_AGG` e validar ordenação/separador em agregação.
- P8 consolidado: paginação por versão (`OFFSET/FETCH`, `TOP`) com gates explícitos de dialeto.
- Funções-chave do banco: `STRING_AGG`, `ISNULL`, `DATEADD`, `JSON_VALUE`/`OPENJSON` (subset no mock).

#### 3.2.3 Aplicações típicas
- Implementação estimada: **90%**.
- Sistemas .NET com forte dependência de SQL Server.
- Testes de compatibilidade evolutiva por geração da plataforma.

### 3.3 Oracle (`DbSqlLikeMem.Oracle`)

#### 3.3.1 Versões simuladas
- Implementação estimada: **100%**.
- 7, 8, 9, 10, 11, 12, 18, 19, 21, 23.

#### 3.3.2 Recursos relevantes
- Implementação estimada: **82%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Backlog de funções: priorizar `LISTAGG` para agregação textual por grupo.
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
- Implementação estimada: **82%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Backlog de funções: priorizar `STRING_AGG` com cobertura de ordenação por grupo.
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
- Implementação estimada: **82%**.
- `WITH`/CTE disponível.
- Operadores JSON `->` e `->>` disponíveis no parser do dialeto.
- Backlog de funções: consolidar suporte a `GROUP_CONCAT` e regras de separador.
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
- Implementação estimada: **82%**.
- `WITH`/CTE disponível.
- `MERGE` disponível (>= 9).
- `FETCH FIRST` suportado.
- Backlog de funções: avaliar `LISTAGG` conforme versão simulada e compatibilidade desejada.
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
- Implementação estimada: **90%**.
- Executar casos críticos em todos os provedores prioritários do produto.
- Definir perfil mínimo de compatibilidade por módulo.
- Execução matricial por provider já iniciada em CI, com publicação de artefatos de resultado por projeto.
- Cobertura de regressão inclui suíte cross-dialeto com snapshot para SQL comum em múltiplos providers.
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
- Implementação estimada: **92%**.
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

### 6.3 Política sugerida de versionamento

#### 6.3.1 SemVer para consumidores
- Implementação estimada: **84%**.
- Incremento major para quebras comportamentais/documentadas.
- Incremento minor para novos recursos compatíveis.
- Incremento patch para correções sem alteração contratual.

#### 6.3.2 Comunicação de mudanças
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
