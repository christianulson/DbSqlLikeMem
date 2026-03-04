# Roadmap revalidado do Core (Parser/Executor) sem duplicação e sem risco de build

> Reavaliação do roadmap anterior com foco em: **não duplicar código**, **não criar abstrações paralelas** e **evitar quebra de build**.

## Contexto de revalidação

O core já possui base importante para evolução orientada a dialect:

- `ISqlDialect` já centraliza várias capacidades de parser/runtime (CTE, UPSERT, JSON, paginação, hints, comparação textual, date add, window etc.).
- `SqlQueryParser` já aplica diversos gates por capability/version.
- `AstQueryExecutorFactory` ainda é parcial (fallback de não implementado para dialetos sem executor AST registrado), então mudanças no executor precisam ser incrementais para não quebrar compatibilidade.

## Decisão principal

Para evitar duplicação de código e problemas de build:

1. **Não criar uma nova interface paralela (`IDialectCapabilities`) agora**.
2. Evoluir o que já existe em `ISqlDialect` com propriedades/métodos pequenos e específicos.
3. Implementar funcionalidades novas atrás de feature gate + testes por provider/versão.

---


## Status de implementação (estimado)

> Revalidação de progresso feita com base no estado atual de documentação técnica e suites de regressão por provider/versão (known gaps, global evolution plan e trackers dedicados).

| Item | Progresso atual | Tendência | Resumo consolidado |
| --- | --- | --- | --- |
| 1) CTE avançada | **75%** | ⬆️ | Gates principais já cobrem o core e houve avanço por versão/dialeto; faltam bordas específicas de cobertura. |
| 2) Window functions além de `ROW_NUMBER` | **100%** | ✅ | Gating por nome/versão, validação de aridade/`ORDER BY`, parser com suporte a frames `ROWS`/`RANGE`/`GROUPS` e runtime consolidado para ranking/distribution/value functions (incluindo `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`LAG`/`LEAD`/`RANK`/`DENSE_RANK`/`PERCENT_RANK`/`CUME_DIST`/`NTILE`), com hardening semântico para peers, ORDER BY composto em cenários válidos e mensagens fail-fast objetivas para combinações inválidas. Sem pendências abertas neste item no core parser/executor. |
| 3) UPSERT por família de banco | **65%** | ⬆️ | `ON DUPLICATE`/`ON CONFLICT` e subset de `MERGE` avançaram; pendem harmonizações finais de semântica no executor. |
| 4) Tipos/literais/coerção | **100%** | ✅ | Escopo incremental consolidado no core com centralização estável em `SqlExtensions` e `DbTypeParser/DbTypeExtension`: coerção booleana/textual numérica com `InvariantCulture`, suporte a famílias integrais signed/unsigned (incluindo borda `ulong` sem overflow), semântica temporal consistente (`DateTime`/`DateTimeOffset`/`TimeSpan`) em coerção/comparação, semântica binária por conteúdo/ordenação lexicográfica para `byte[]`, parsing abrangente de `DbType.Object` (`JSON`, `bool`, números, `Guid`, `DateTimeOffset`, `DateTime`, `TimeSpan`, `NULL` quoted por tipo) e binário hexadecimal (`0x...`, `\\x...`, `X'...'`), com cobertura dedicada de regressão no projeto core. |
| 5) JSON cross-dialect | **68%** | ⬆️ | Runtime/cobertura evoluíram nos caminhos suportados, com fallback padronizado para não suportado. |
| 6) Plano físico com custo | **100%** | ✅ | Heurísticas incrementais de custo no `ExecutionPlan` consolidadas para o escopo do item (custos por formato, cardinalidade de chaves em `GROUP BY`/`ORDER BY`, acoplamento `DISTINCT + GROUP BY + ORDER BY` com sensibilidade adicional a `OFFSET`, complexidade estrutural de expressões e `HAVING` acoplado inclusive com joins de risco de expansão e complexidade de `ON`, IN-list, CASE/JSON em predicados incluindo funções JSON no AST (`FunctionCallExpr`/`CallExpr`) e fallback `RawSqlExpr` por tokens lógicos/JSON/subquery com transições `AND/OR` e profundidade lógica, granularidade de agregados na projeção com robustez a whitespace e distinção de agregação `DISTINCT` incluindo `MIN`/`MAX` e ordenação de pesos `COUNT`/`SUM`/`AVG`, custo de subqueries na projeção por cardinalidade de ocorrência, sensibilidade a múltiplas ocorrências `CASE`/`OVER` na projeção, custo de funções JSON e operadores JSON (`->`, `->>`, `#>`, `#>>`) na projeção, complexidade de expressões em `GROUP BY`/`ORDER BY` e no `ORDER BY` de `UNION` incluindo JSON, custo de transição entre operadores de conjunto (`UNION ALL`/`UNION DISTINCT`) e fan-in de merge em `UNION ORDER BY`, acoplamento de sort aninhado para `ORDER BY` externo sobre fontes derivadas ordenadas na tabela base e em fontes de `JOIN` com sensibilidade também a `LIMIT/OFFSET` internos de fonte derivada, complexidade de CTE alinhada ao caminho principal, sensibilidade a row-limit/offset, fan-out de joins, largura/curinga de projeção e monotonicidade, incluindo `DerivedUnion`), mantendo abordagem sem engine física completa. |
| 7) JOIN/subquery com heurística | **100%** | ✅ | Execução multi-tabela e padronização de não suportado avançaram; o escopo incremental do item foi consolidado com cache leve por linha externa para subqueries correlacionadas em `EXISTS`, `IN (subquery)` e subquery escalar no executor, seleção de chave por identificadores externos efetivamente referenciados, canonização textual da subquery (casing/whitespace preservando literais), normalização de aliases locais (`FROM/JOIN`) com padronização entre presença/ausência de `AS`, normalização de espaçamento em operadores relacionais, canonização específica de `EXISTS` para ignorar variações de payload de projeção no `SELECT` (preservando cláusulas relacionais), normalização de aliases explícitos `AS` no payload de `SELECT` para subqueries `IN`, escalares e comparações quantificadas, semântica SQL de três valores para `NOT IN` com candidatos `NULL` (lista e subquery), suporte funcional a comparações quantificadas `ANY/ALL` com subquery incluindo `SOME` como alias de `ANY` (com cenários de candidatos `NULL`, conjunto vazio e wrapper extra de parênteses), além de canonização de predicados comutativos de topo (`WHERE/HAVING` com cadeias `AND` seguras), normalização de parênteses externos redundantes e canonização de igualdades simples com operandos invertidos para reduzir misses em SQL semanticamente equivalente, sem framework novo. |
| 8) Semântica transacional por isolamento | **35%** | ⬆️ | Hardening/confiabilidade avançaram; isolamento completo por provider/versão ainda está em fase inicial. |
| 9) `RETURNING`/`OUTPUT`/`RETURNING INTO` | **64%** | ⬆️ | Parser/capabilities evoluíram com suporte explícito a `RETURNING` em `INSERT/UPDATE/DELETE` nos caminhos PostgreSQL e SQLite (incluindo parsing dedicado no gate de dialeto e materialização incremental no executor via `ExecuteReader` para DML básico com projeções diretas/alias/wildcard e snapshot de linhas afetadas); payload homogêneo multi-provider, semântica de `OUTPUT` (SQL Server), `RETURNING INTO` (Oracle) e expressões avançadas de `RETURNING` seguem como gap principal. |
| 10) Collation/null ordering | **50%** | ⬆️ | Evolução de gates para `NULLS FIRST/LAST` em parte dos dialetos; collation detalhada cross-provider ainda pendente. |

### Leitura rápida da revalidação

- **Estáveis:** item 2 (window functions concluídas no core parser/executor).
- **Concluído no escopo incremental:** item 6 (plano físico com custo) consolidado com heurísticas de cardinalidade, acoplamento `DISTINCT + GROUP BY + ORDER BY` com sensibilidade adicional a `OFFSET`, complexidade estrutural, `HAVING` e joins com risco de expansão/complexidade de `ON`, CASE/JSON em predicados (incluindo funções JSON em nós `FunctionCallExpr`/`CallExpr` e fallback tokenizado para `RawSqlExpr` com transições lógicas `AND/OR` e profundidade), granularidade de funções agregadas na projeção (incluindo variações de whitespace, agregação `DISTINCT`, `MIN`/`MAX` e ordenação de pesos `COUNT`/`SUM`/`AVG`), custo de subqueries e funções/operadores JSON na projeção por cardinalidade de ocorrência e sensibilidade a múltiplas ocorrências `CASE`/`OVER`, complexidade de expressões em `GROUP BY`/`ORDER BY` (incluindo `UNION`, funções JSON e operadores JSON), custo de transição entre operadores de conjunto em `UNION` e fan-in de merge para `ORDER BY` com múltiplas partes, acoplamento de sort aninhado para `ORDER BY` externo sobre fontes derivadas ordenadas na tabela base e em JOINs com sensibilidade a `LIMIT/OFFSET` interno de fonte derivada, custo de CTE alinhado ao caminho principal, fan-out de joins, largura/curinga de projeção e monotonicidade (incluindo `DerivedUnion`), sem mudança arquitetural ampla.
- **Em evolução:** itens 1, 3, 5, 7, 8, 9 e 10, com impacto recente em JSON/runtime, UPSERT subset e confiabilidade transacional.
- **Observação:** percentuais são referência executiva (não métrica automática) e devem ser confirmados no corte de release via suíte local/CI.

## Reavaliação das propostas (válido x ajustar x adiar)

## 1) CTE avançada

**Status:** ✅ **Válido** (já existe base).

- Já há flags como `SupportsWithCte`, `SupportsWithRecursive` e `SupportsWithMaterializedHint`.
- A ação correta é ampliar cobertura de testes e casos sem criar novo eixo arquitetural.

**Implementação recomendada:**

- manter parser usando os gates já existentes;
- adicionar testes por versão para cada provider onde houver comportamento diferente.

## 2) Window functions além de `ROW_NUMBER`

**Status:** ✅ **Concluído no Core (parser/executor)**.

- Já existe `SupportsWindowFunctions` e inferência de tipo para window function no dialect.
- Evitar engine nova nesse momento; evoluir no executor atual por passos pequenos.

**Escopo entregue e manutenção recomendada:**

- manter a suíte de regressão cobrindo `ROWS`/`RANGE`/`GROUPS`, `ORDER BY` composto e peers;
- preservar fail-fast para função desconhecida e validações de aridade/ranges literais (`NTILE`/`LAG`/`LEAD`/`NTH_VALUE`);
- tratar apenas hardening incremental, sem abrir novo eixo arquitetural para este item.

## 3) UPSERT por família de banco

**Status:** ✅ **Válido**.

- Já há `SupportsOnDuplicateKeyUpdate`, `SupportsOnConflictClause`, `SupportsMerge` no dialect.
- O parser já usa essas capacidades.

**Implementação recomendada:**

- padronizar nó AST de upsert sem quebrar contratos existentes;
- completar execução por provider sem duplicar regras em parser + strategy.

## 4) Tipos/literais/coerção

**Status:** ✅ **Concluído no escopo incremental**.

- Hoje há semântica compartilhada em `SqlExtensions` (`Compare`, `EqualsSql`, `ToDec`, `ToBool`).
- Risco de duplicação é alto se cada provider reimplementar isso.

**Implementação recomendada (manutenção):**

- manter helpers centrais e cobertura de regressão no projeto core;
- preservar variação por dialeto apenas via hooks existentes (`TextComparison`, `SupportsImplicitNumericStringComparison`, `LikeIsCaseInsensitive`).

## 5) JSON cross-dialect

**Status:** ✅ **Válido**.

- Parser/tokenizer já têm gates (`SupportsJsonArrowOperators`, `SupportsJsonExtractFunction`, `SupportsJsonValueFunction`, `SupportsOpenJsonFunction`).

**Implementação recomendada:**

- ampliar executor por estratégia de função/operador;
- manter fallback explícito de não suportado quando necessário.

## 6) Plano físico completo com custo

**Status:** ⚠️ **Parcial incremental** (sem engine física completa).

- Refatorar para engine física completa agora ainda aumenta risco de regressão/build.
- A trilha atual prioriza heurísticas localizadas de custo no `ExecutionPlan` com cobertura de monotonicidade, mantendo baixo risco de churn.

## 7) JOIN/subquery com heurística de custo

**Status:** ⚠️ **Parcial (somente otimizações localizadas)**.

- Válido otimizar `EXISTS` short-circuit e cache de subquery correlacionada em pontos críticos;
- não recomendado introduzir framework de otimização novo nessa etapa.

## 8) Semântica transacional por isolamento completo

**Status:** ⚠️ **Parcial**.

- Implementar subset mínimo por provider quando houver demanda clara de testes;
- adiar modelagem ampla de isolation levels para fase posterior.

## 9) `RETURNING` / `OUTPUT` / `RETURNING INTO`

**Status:** ✅ **Válido**.

- Já existe capability `SupportsReturning`; evolução natural é completar payload no executor por provider.
- Priorizar PostgreSQL primeiro (menor ambiguidade no modelo atual).

## 10) Collation/null ordering

**Status:** ✅ **Válido com foco em centralização**.

- Base existente: `TextComparison` e regras de comparação centralizadas.
- Próximo passo: consolidar `NULLS FIRST/LAST` e manter `ORDER BY` coerente com dialect.

---

## O que foi considerado inválido no roadmap anterior

- ❌ **Criar agora uma interface paralela grande (`IDialectCapabilities`)**.
  - Motivo: o projeto já tem `ISqlDialect` robusto; criar outro contrato agora tende a duplicar regra e aumentar custo de manutenção/build.

- ❌ **Troca ampla imediata para novo motor físico completo**.
  - Motivo: alto risco de regressão antes de fechar gaps mais diretos de parser/executor já mapeados.

---

## Plano enxuto recomendado (sem quebrar build)

1. **Parser gates + testes por versão** (CTE, paginação, JSON, hints, upsert).
2. **Executor: completar semântica pendente por feature** (`RETURNING`, JSON subset e ajustes pontuais por provider).
3. **Centralização de coerção/comparação** evitando duplicação por provider.
4. **Hardening** com matriz fixa provider/versão antes de refatorações estruturais grandes.

## Critérios de aceite para cada feature nova

Uma implementação só entra se cumprir os quatro itens:

- parser com gate em dialect;
- executor sem regra duplicada por provider (apenas variação necessária);
- testes por versão cobrindo aceitação/rejeição + semântica;
- fallback padronizado (`SqlUnsupported` / `NotSupported`) quando não suportado.

## Backlog revisado (ordem segura)

1. Completar `ON CONFLICT ... DO UPDATE` e `excluded` sem duplicar parsing.
2. `MERGE` com subset estável e testes de regressão por SQL Server/DB2/Oracle.
3. `RETURNING` funcional no executor para `INSERT/UPDATE/DELETE` (PostgreSQL primeiro).
4. Consolidar comportamento de comparação/ordenação (`TextComparison`, `NULLS FIRST/LAST`).
5. Expandir cobertura JSON por função/operador já gateada no dialect.
6. Hardening contínuo de window functions por provider/versão (sem novo eixo arquitetural).

## Referências relacionadas

- [Provedores, versões e compatibilidade](providers-and-features.md)
- [Matriz SQL (feature x dialeto)](sql-compatibility-matrix.md)
- [Checklist de known gaps](known-gaps-checklist.md)
- [Testes por versão de dialeto](testes-por-versao-dialect.md)
