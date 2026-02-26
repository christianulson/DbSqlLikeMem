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
| 2) Window functions além de `ROW_NUMBER` | **100%** | ✅ | Gating por nome/versão, validação de aridade/`ORDER BY`, parser com suporte a frames `ROWS`/`RANGE`/`GROUPS` e runtime já implementado para ranking/distribution/value functions (incluindo `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`LAG`/`LEAD`/`RANK`/`DENSE_RANK`/`PERCENT_RANK`/`CUME_DIST`/`NTILE`), com hardening de limites e fail-fast para casos inválidos. Sem pendências abertas neste item no core parser/executor. |
| 3) UPSERT por família de banco | **65%** | ⬆️ | `ON DUPLICATE`/`ON CONFLICT` e subset de `MERGE` avançaram; pendem harmonizações finais de semântica no executor. |
| 4) Tipos/literais/coerção | **50%** | ⬆️ | Base central em `SqlExtensions` evoluiu, porém ainda faltam regras finas por dialeto/versão. |
| 5) JSON cross-dialect | **68%** | ⬆️ | Runtime/cobertura evoluíram nos caminhos suportados, com fallback padronizado para não suportado. |
| 6) Plano físico com custo | **15%** | ➡️ | Sem mudança estrutural: segue como backlog de maior risco e menor prioridade imediata. |
| 7) JOIN/subquery com heurística | **40%** | ⬆️ | Execução multi-tabela e padronização de não suportado avançaram; heurística explícita de custo/caching segue pendente. |
| 8) Semântica transacional por isolamento | **35%** | ⬆️ | Hardening/confiabilidade avançaram; isolamento completo por provider/versão ainda está em fase inicial. |
| 9) `RETURNING`/`OUTPUT`/`RETURNING INTO` | **40%** | ⬆️ | Parser/capabilities e subset por provider evoluíram; payload homogêneo multi-provider no executor é o principal gap. |
| 10) Collation/null ordering | **50%** | ⬆️ | Evolução de gates para `NULLS FIRST/LAST` em parte dos dialetos; collation detalhada cross-provider ainda pendente. |

### Leitura rápida da revalidação

- **Estáveis:** item 2 (window functions concluídas no core parser/executor) e item 6 (plano físico com custo).
- **Em evolução:** itens 1, 3, 4, 5, 7, 8, 9 e 10, com impacto recente em JSON/runtime, UPSERT subset e confiabilidade transacional.
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

**Status:** ✅ **Válido com prioridade alta**.

- Hoje há semântica compartilhada em `SqlExtensions` (`Compare`, `EqualsSql`, `ToDec`, `ToBool`).
- Risco de duplicação é alto se cada provider reimplementar isso.

**Implementação recomendada:**

- manter helpers centrais;
- extrair somente pontos variáveis para hooks do dialect (`TextComparison`, `SupportsImplicitNumericStringComparison`, `LikeIsCaseInsensitive` já existem).

## 5) JSON cross-dialect

**Status:** ✅ **Válido**.

- Parser/tokenizer já têm gates (`SupportsJsonArrowOperators`, `SupportsJsonExtractFunction`, `SupportsJsonValueFunction`, `SupportsOpenJsonFunction`).

**Implementação recomendada:**

- ampliar executor por estratégia de função/operador;
- manter fallback explícito de não suportado quando necessário.

## 6) Plano físico completo com custo

**Status:** ⚠️ **Adiar** (alto risco de churn).

- Embora desejável, refatorar para engine física completa agora aumenta risco de regressão/build.
- Melhor seguir com melhorias localizadas no executor atual e instrumentação incremental de plano.

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
