# Análise: testes que valem migrar de `[Fact]` para `MemberData{Db}Version`

## Contexto
Hoje os testes de parser já usam bastante `MemberDataMySqlVersion`, `MemberDataNpgsqlVersion` e `MemberDataSqlServerVersion`, mas vários testes de execução/compatibilidade ainda estão em `[Fact]` fixo. Isso pode mascarar diferenças reais de dialeto por versão.

## Critério usado
Priorizar migração para testes versionados quando a query usa recurso que mudou por versão do banco:
- CTE/`WITH RECURSIVE`.
- Funções/operators JSON.
- `OFFSET ... FETCH` (SQL Server).
- Window functions (`OVER (...)`).

---

## MySQL: candidatos fortes

### 1) `Window_RowNumber_PartitionBy_ShouldWork`
- Query usa `ROW_NUMBER() OVER (PARTITION BY ...)`.
- Em MySQL, window functions são recurso de linha 8.x.
- Recomendação: trocar para `[Theory] + [MemberDataMySqlVersion(VersionGraterOrEqual = 8)]`. Para versões `< 8`, criar teste separado de rejeição (`NotSupportedException`).

Referência: `MySqlAdvancedSqlGapTests` com `ROW_NUMBER() OVER (...)`.【F:src/DbSqlLikeMem.MySql.Test/MySqlAdvancedSqlGapTests.cs†L50-L57】

### 2) `Cte_With_ShouldWork`
- Query usa `WITH u AS (...)`.
- CTE em MySQL é esperado apenas em versões novas (8.x).
- Recomendação: versionar em `>= 8` e ter cenário inverso para `< 8`.

Referência: `WITH u AS (...) SELECT ...`.【F:src/DbSqlLikeMem.MySql.Test/MySqlSqlCompatibilityGapTests.cs†L261-L267】

### 3) `JsonExtract_SimpleObjectPath_ShouldWork`
- Query usa `JSON_EXTRACT(...)`.
- JSON nativo é sensível a versão no ecossistema MySQL (na matriz do projeto, isso deve ser explicitado por versão).
- Recomendação: `[MemberDataMySqlVersion(VersionGraterOrEqual = 5)]` (ou recorte mais estrito se quiser refletir versão menor suportada internamente).

Referência: `JSON_EXTRACT(payload, '$.a.b')`.【F:src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs†L83-L87】

---

## PostgreSQL/Npgsql: candidatos fortes

### 4) `JsonPathExtract_ShouldWork`
- Query usa `payload::jsonb #>> '{a,b}'`.
- `jsonb`/operadores JSON são recursos historicamente sensíveis a versão.
- Recomendação: migrar para `[Theory] + [MemberDataNpgsqlVersion(VersionGraterOrEqual = 9)]` (ou limite escolhido conforme política do mock).

Referência: cast para `jsonb` e operador `#>>`.【F:src/DbSqlLikeMem.Npgsql.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs†L71-L75】

---

## SQL Server: candidatos fortes

### 5) `OffsetFetch_ShouldWork`
- Query usa `ORDER BY ... OFFSET ... FETCH NEXT ...`.
- Recurso depende de versão no SQL Server.
- Recomendação: `[Theory] + [MemberDataSqlServerVersion(VersionGraterOrEqual = 2012)]`; para versões anteriores, validar rejeição.

Referência: `OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY`.【F:src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs†L60-L61】

### 6) `JsonValue_SimpleObjectPath_ShouldWork`
- Query usa `JSON_VALUE(...)`.
- Funções JSON no SQL Server também são sensíveis a versão.
- Recomendação: `[MemberDataSqlServerVersion(VersionGraterOrEqual = 2016)]`.

Referência: `TRY_CAST(JSON_VALUE(payload, '$.a.b') ...)`.【F:src/DbSqlLikeMem.SqlServer.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs†L71-L72】

---

## O que NÃO precisa versionar agora (baixo risco)
- Testes de `UNION` básico (`UNION`, `UNION ALL`) sem recursos adicionais.
- Ordenação/aliases simples.
- `COALESCE`, operações aritméticas simples e precedência booleana básica.

Exemplos de testes estáveis (bom manter em `[Fact]`):【F:src/DbSqlLikeMem.MySql.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs†L34-L52】【F:src/DbSqlLikeMem.MySql.Test/MySqlSqlCompatibilityGapTests.cs†L48-L75】

---

## Estratégia prática de migração
1. Começar pelos 6 testes acima (alto impacto de dialeto).
2. Para cada teste positivo versionado (`>= X`), adicionar teste complementar de rejeição (`< X`).
3. Evitar explosão combinatória: só versionar quando a query traz sintaxe/semântica conhecida como variante por versão.

