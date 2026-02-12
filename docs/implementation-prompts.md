# Prompts de implementação (copy/paste)

Prompts prontos para colar em outras janelas e implementar as próximas features de maior uso no DbSqlLikeMem, cobrindo **MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2**.

> Dica: execute em ordem **P0 → P6**. Os prompts já incluem objetivo, escopo, critérios de aceite e validação.

---

## P0 — Baseline multi-provider (alinhar realidade de código, testes e docs)

```text
Você está no repositório DbSqlLikeMem.

Objetivo:
Validar e alinhar documentação, matriz de compatibilidade e versões simuladas para TODOS os providers: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2.

Arquivos-alvo:
- README.md
- docs/providers-and-features.md
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
3) Regras por provider documentadas em docs/providers-and-features.md.

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
4) Atualizar docs/providers-and-features.md com a capacidade implementada.
5) Registrar limitações conhecidas no corpo da PR.

Formato da PR:
- O que foi implementado
- O que ainda não cobre
- Testes executados
- Riscos conhecidos
- Próximos passos
```
