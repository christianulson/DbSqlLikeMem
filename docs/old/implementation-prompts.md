# Prompts de implementação (copy/paste)

> Arquivo gerado por `scripts/update_implementation_prompts.py`.

Prompts prontos para colar em outras janelas e implementar as próximas features de maior uso no DbSqlLikeMem, cobrindo **MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2**.

> Dica: execute em ordem **P0 → P14**. Os prompts já incluem objetivo, escopo, critérios de aceite e validação.

---

## P0 — Baseline multi-provider (alinhar realidade de código, testes e docs)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Validar e alinhar documentação, matriz de compatibilidade e versões simuladas para TODOS os providers: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2.

Arquivos-alvo:
- README.md
- docs/old/providers-and-features.md
- *DbVersions.cs de cada provider

Tarefas:
1) Conferir providers e versões simuladas no código.
2) Atualizar docs para remover qualquer divergência.
3) Garantir consistência de nomenclatura (PostgreSQL/Npgsql, SQLite/Sqlite, SQL Server, DB2 etc.).
4) Não alterar runtime nesta etapa (somente baseline e clareza).

Critérios de aceite:
- Documentação condizente com o código.
- Mesma lista de providers em README e docs.
```

---

## P1 — SQL Core mais utilizado (todos os providers)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Fechar gaps de SQL Core mais usados em aplicações reais para todos os providers.

Escopo:
1) WHERE com precedência AND/OR e parênteses.
2) SELECT expressions: aritmética e CASE WHEN.
3) Funções comuns por dialeto: COALESCE, IFNULL/ISNULL/NVL, CONCAT.
4) ORDER BY por alias e ordinal.

Referência de testes:
- *SqlCompatibilityGapTests.cs de cada provider

Requisitos técnicos:
- Reaproveitar AST/Executor existentes.
- Diferenças de sintaxe/semântica devem ficar no Dialect.
- Evitar duplicação excessiva de lógica por provider.

Critérios de aceite:
- Cenários SQL Core verdes nos 6 providers.
- Sem regressões nos testes já verdes.
```

---

## P2 — Composição de consulta (todos os providers): GROUP BY/HAVING + UNION + CTE

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Evoluir a composição de consultas para cenários de API e relatório em todos os providers.

Escopo:
1) GROUP BY + HAVING com agregações (SUM/COUNT).
2) UNION com ORDER BY final.
3) UNION dentro de subselect.
4) WITH (CTE) simples.

Referência de testes:
- *SqlCompatibilityGapTests.cs de MySQL, SQL Server, Oracle, PostgreSQL, SQLite e DB2.

Requisitos:
- Preferir evolução da AST ao invés de parser ad-hoc.
- Definir regra determinística para alias/ordinal no ORDER BY.

Critérios de aceite:
- Casos acima verdes no conjunto de providers.
- Sem quebra em SELECT simples já suportado.
```

---

## P3 — Advanced SQL de alto impacto (todos os providers)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Implementar recursos avançados populares para analytics/relatórios, respeitando diferenças por dialeto.

Escopo:
1) Window functions (ex.: ROW_NUMBER OVER PARTITION BY ORDER BY).
2) Subquery correlacionada em SELECT list.
3) CAST básico string -> numérico.
4) Operações de data por dialeto (DATEADD / INTERVAL / equivalentes).

Referência de testes:
- *AdvancedSqlGapTests.cs de cada provider.

Requisitos:
- Implementação incremental e compatível com arquitetura atual.
- Quando houver diferença real entre bancos, explicitar regra no Dialect e documentar.

Critérios de aceite:
- Testes avançados relevantes verdes por provider.
- Limitações conhecidas documentadas.
```

---

## P4 — Tipagem, collation e coerção implícita (consistência cross-provider)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Padronizar regras de comparação textual e coerção implícita, com comportamento previsível entre providers.

Escopo:
1) Case sensitivity/collation em comparações de string.
2) Coerção número vs string (ex.: id = '2').
3) Regras por provider documentadas em docs/old/providers-and-features.md.

Referência de testes:
- testes de Typing_ImplicitCasts_And_Collation* e Collation_CaseSensitivity* dos providers.

Critérios de aceite:
- Regras implementadas e documentadas.
- Sem comportamento surpresa em cenários básicos de filtro/comparação.
```

---

## P5 — Backlog automático a partir dos GapTests (todos os providers)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Gerar backlog técnico priorizado a partir de *SqlCompatibilityGapTests e *AdvancedSqlGapTests de todos os providers.

Formato de saída (Markdown):
- Feature
- Providers impactados
- Complexidade (P/M/G)
- Risco de regressão (baixo/médio/alto)
- Dependências
- Status sugerido (Now/Next/Later)

Regras:
1) Agrupar por épico: Parser, Executor, Funções SQL, Tipagem/Collation, Performance.
2) Priorizar por impacto x esforço.
3) Cada item deve citar arquivo(s) de teste-alvo.
```

---

## P6 — Hardening obrigatório antes de PR

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Garantir qualidade antes de abrir PR, mesmo em mudanças focadas em um provider.

Checklist:
1) Rodar testes do provider alterado.
2) Rodar smoke tests dos demais providers.
3) Adicionar ao menos 1 teste de regressão por bug corrigido.
4) Atualizar docs/old/providers-and-features.md com a capacidade implementada.
5) Registrar limitações conhecidas no corpo da PR.

Formato da PR:
- O que foi implementado
- O que ainda não cobre
- Testes executados
- Riscos conhecidos
- Próximos passos
```

---

## P7 — DML avançado e mutações cross-dialect

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Expandir cobertura de DML avançado por dialeto (INSERT/UPDATE/DELETE) com foco em cenários reais de aplicação.

Escopo:
1) UPSERT por dialeto:
   - MySQL: ON DUPLICATE KEY UPDATE
   - PostgreSQL/SQLite: ON CONFLICT DO UPDATE
   - SQL Server/Oracle/DB2: MERGE (subset seguro)
2) UPDATE/DELETE com subquery e JOIN (quando aplicável por dialeto).
3) Suporte a RETURNING/OUTPUT/RETURNING INTO (mínimo viável por provider).

Referência de testes:
- *InsertOnDuplicateTests.cs
- *UpdateStrategyTests.cs
- *DeleteStrategyTests.cs
- criar novos testes para RETURNING/OUTPUT quando faltarem.

Critérios de aceite:
- Cenários de mutação mais comuns suportados nos 6 providers.
- Diferenças de sintaxe/semântica isoladas no Dialect.
```

---

## P8 — Paginação e ordenação avançada por versão

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Padronizar paginação e ordenação avançada, respeitando versões simuladas de cada banco.

Escopo:
1) LIMIT/OFFSET, FETCH FIRST/NEXT, OFFSET ... FETCH.
2) ORDER BY com NULLS FIRST/LAST (quando suportado).
3) TOP (SQL Server) e equivalentes por dialeto.
4) Regras claras por versão no Dialect (feature gates).

Referência de testes:
- *UnionLimitAndJsonCompatibilityTests.cs
- adicionar suite dedicada de pagination compatibility por provider.

Critérios de aceite:
- Comportamento previsível para consultas paginadas.
- Erros descritivos quando sintaxe não for suportada pelo provider/versão.
```

---

## P9 — JSON e funções especializadas por provider

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Melhorar suporte a consultas JSON em cada provider com um núcleo comum de avaliação.

Escopo:
1) Operadores e funções por dialeto:
   - PostgreSQL: ->, ->>, #>, #>>
   - MySQL/SQLite: JSON_EXTRACT (e aliases mais usados)
   - SQL Server: JSON_VALUE / OPENJSON (subset)
2) Fallback consistente para providers sem suporte (ex.: DB2 sem operadores JSON no momento).
3) Documentar matriz de suporte JSON no docs/old/providers-and-features.md.

Referência de testes:
- *UnionLimitAndJsonCompatibilityTests.cs
- novos testes JsonFunctionCompatibilityTests por provider.

Critérios de aceite:
- JSON parsing/execution estável nos providers com suporte.
- Mensagens claras de "não suportado" nos demais.
```

---

## P10 — Procedimentos, funções e parâmetros de saída

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Evoluir simulação de rotinas de banco para cenários enterprise.

Escopo:
1) Stored procedures com IN/OUT/INOUT quando aplicável.
2) Mapeamento robusto de DbParameterDirection e retorno de status.
3) Simulação mínima de scalar functions por provider.
4) Melhorias no comportamento de execução via Dapper para procedures.

Referência de testes:
- *StoredProcedureExecutionTests.cs
- *StoredProcedureSignatureTests.cs
- ampliar testes de parâmetros de saída por provider.

Critérios de aceite:
- Execução previsível de procedures nos providers com suporte.
- Compatibilidade de parâmetros coerente com ADO.NET.
```

---

## P11 — Confiabilidade transacional e concorrência

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Aumentar fidelidade de transações e comportamento concorrente para cenários críticos.

Escopo:
1) Savepoints (BEGIN/ROLLBACK TO/RELEASE SAVEPOINT).
2) Isolamento básico por nível (ReadCommitted, RepeatableRead, Serializable) em modelo simplificado.
3) Consistência de commit/rollback com múltiplos comandos.
4) Thread-safety em operações concorrentes de leitura/escrita.

Referência de testes:
- *TransactionTests.cs
- adicionar testes de savepoint/isolation por provider.

Critérios de aceite:
- Transações reproduzem efeitos esperados em cenários comuns.
- Sem regressões em testes existentes de transaction/strategy.
```

---

## P12 — Observabilidade, diagnóstico e ergonomia de erro

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Melhorar a experiência de debug e troubleshooting para reduzir tempo de investigação de falhas em testes.

Escopo:
1) Mensagens de erro com contexto (trecho SQL, posição aproximada, token esperado).
2) Códigos de erro por provider (quando aplicável à simulação).
3) Modo verbose de parser/executor para testes (trace opcional).
4) Padronização de exceptions factory por provider.

Referência de testes:
- *ExceptionFactory* tests
- criar ParserDiagnosticsTests e ExecutorDiagnosticsTests.

Critérios de aceite:
- Falhas mais explicativas e acionáveis para o desenvolvedor.
- Sem ruído excessivo quando modo verbose estiver desativado.
```

---

## P13 — Performance e escala do engine em memória

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Reduzir custo de execução em cenários de massa de dados e suites grandes de testes.

Escopo:
1) Índices em memória mais eficientes para filtros e joins.
2) Melhorias de alocação (menos boxing/cópias em hot paths).
3) Caching de parsing/planos simples quando seguro.
4) Benchmarks e métricas por provider.

Referência de testes:
- *PerformanceTests.cs
- adicionar benchmarks focados em SELECT/WHERE/JOIN/GROUP BY.

Critérios de aceite:
- Ganho mensurável de tempo/memória em cenários-alvo.
- Sem alteração de comportamento funcional.
```

---

## P14 — Conformidade de ecossistema (.NET, ORM, tooling)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Fortalecer integração com ecossistema .NET e garantir estabilidade em pipelines de CI/CD.

Escopo:
1) Cobertura adicional para Dapper (multi-mapping, buffered/unbuffered, QueryMultiple).
2) Compatibilidade básica com padrões de acesso de EF Core (quando aplicável ao mock).
3) Matriz de TFM e validação contínua em CI para providers.
4) Documentação de integração por stack (Dapper/ADO.NET/uso híbrido).

Referência de testes:
- DapperTests.cs / DapperUserTests*.cs
- testes de integração de pipeline por target framework.

Critérios de aceite:
- Uso mais robusto em projetos reais com diferentes stacks.
- Regressões capturadas cedo via CI.
```
