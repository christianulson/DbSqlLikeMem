# Plano executável — P7 a P14

Documento gerado por `scripts/generate_p7_p10_plan.py` para orientar implementação técnica dos itens P7 a P14 com base nos testes existentes por provider.

## Como usar

1. Escolha um pilar (P7–P14).
2. Implemente provider por provider, priorizando os arquivos mapeados abaixo.
3. Rode os testes por provider e atualize o status.

## P7 (DML avançado)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Strategy/MySqlDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/Strategy/SqlServerInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/Strategy/OracleInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Strategy/OracleDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/Strategy/PostgreSqlInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteInsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteUpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteDeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2InsertOnDuplicateTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2UpdateStrategyTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Strategy/Db2DeleteStrategyTests.cs` | 3 arquivos | 🟨 Em evolução |

## P8 (Paginação/ordenação)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |

## P9 (JSON)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteUnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2UnionLimitAndJsonCompatibilityTests.cs` | 1 arquivo | 🟨 Em evolução |

## P10 (Procedures/OUT params)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/StoredProcedureExecutionTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/StoredProcedureSignatureTests.cs` | 2 arquivos | 🟨 Em evolução |

## P11 (Confiabilidade transacional e concorrência)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Strategy/MySqlTransactionTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/MySqlTransactionTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/MySqlTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Strategy/SqlServerTransactionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerTransactionTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Strategy/OracleTransactionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/OracleTransactionTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/OracleTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Strategy/PostgreSqlTransactionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlTransactionTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Strategy/SqliteTransactionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteTransactionTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteTransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Strategy/Db2TransactionTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/Db2TransactionTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/Db2TransactionReliabilityTests.cs` | 3 arquivos | 🟨 Em evolução |

## P12 (Observabilidade, diagnóstico e ergonomia de erro)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Dapper.Test/MySqlAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.MySql.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Dapper.Test/SqlServerAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Dapper.Test/OracleAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Oracle.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Dapper.Test/PostgreSqlAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Dapper.Test/SqliteAdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Dapper.Test/Db2AdditionalBehaviorCoverageTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/ExecutionPlanTests.cs`<br>`src/DbSqlLikeMem.Db2.Test/Parser/SqlQueryParserCorpusTests.cs` | 3 arquivos | 🟨 Em evolução |

## P13 (Performance e escala do engine em memória)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.Test/Performance/MySqlPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.Test/Performance/SqlServerPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.Test/Performance/OraclePerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.Test/Performance/PostgreSqlPerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.Test/Performance/SqlitePerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.Test/Performance/Db2PerformanceTests.cs` | 1 arquivo | 🟨 Em evolução |

## P14 (Conformidade de ecossistema .NET/ORM/tooling)

| Provider | Arquivos de teste-alvo | Cobertura | Status sugerido |
| --- | --- | --- | --- |
| MySQL | `src/DbSqlLikeMem.MySql.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.MySql.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.MySql.Test/MySqlLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| SQL Server | `src/DbSqlLikeMem.SqlServer.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.SqlServer.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.SqlServer.Test/SqlServerLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| Oracle | `src/DbSqlLikeMem.Oracle.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Oracle.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Oracle.Test/OracleLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| PostgreSQL (Npgsql) | `src/DbSqlLikeMem.Npgsql.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Npgsql.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Npgsql.Test/PostgreSqlLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| SQLite | `src/DbSqlLikeMem.Sqlite.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Sqlite.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Sqlite.Test/SqliteLinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |
| DB2 | `src/DbSqlLikeMem.Db2.NHibernate.Test/NHibernateSmokeTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/DapperTests.cs`<br>`src/DbSqlLikeMem.Db2.Dapper.Test/FluentTest.cs`<br>`src/DbSqlLikeMem.Db2.Test/Db2LinqProviderTest.cs` | 4 arquivos | 🟨 Em evolução |

## Checklist de saída por PR

- [ ] Parser e Dialect atualizados para o pilar.
- [ ] Executor atualizado para os casos do pilar.
- [ ] Testes do provider alterado verdes.
- [ ] Smoke tests dos demais providers sem regressão.
- [ ] Documentação de compatibilidade atualizada.



## Melhorias práticas para o plano de execução (Execution Plan Advisor)

### Index Advisor
- [x] Incluir seção `IndexRecommendations` no plano para queries SELECT com alto `EstimatedRowsRead`.
- [x] Sugerir índice composto com colunas de `WHERE/JOIN` e complementar com `ORDER BY` quando aplicável.
- [x] Exibir `Confidence` por recomendação para facilitar priorização técnica.
- [x] Cobrir cenários com e sem índice nos testes `ExecutionPlanTests` dos providers.

### PlanWarnings (MVP)
- [x] Incluir seção `PlanWarnings` no plano de execução para recomendações práticas ao desenvolvedor.
- [x] Implementar alerta para `ORDER BY` sem `LIMIT/TOP/FETCH` em consultas com alto `EstimatedRowsRead`.
- [x] Implementar alerta para baixa seletividade com `EstimatedRowsRead` alto.
- [x] Implementar alerta opcional para `SELECT *` em leitura estimada alta.
- [x] Exibir para cada alerta: `Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`.
- [x] Internacionalizar labels/mensagens do advisor mantendo keywords SQL canônicas (ex.: `WHERE`, `ORDER BY`, `LIMIT/TOP/FETCH`).
- [x] Cobrir cenários positivos e negativos por regra em `ExecutionPlanTests` (MySQL, SQL Server, SQLite).
- [x] Aplicar gate de alto volume de leitura para warnings (`EstimatedRowsRead` alto), evitando ruído em consultas pequenas.


### PlanWarnings (etapa evolução)
- [x] Refinar severidade por contexto: `PW002` escala para `High` em seletividade muito alta e `PW003` escala para `Warning` em leitura muito alta.
- [x] Adicionar metadados técnicos opcionais por alerta (`MetricName`, `ObservedValue`, `Threshold`) preservando compatibilidade do contrato textual.
- [x] Expandir testes de borda para thresholds (abaixo/igual/acima) das regras `PW001`, `PW002` e `PW003` nos 3 providers.
- [x] Validar não regressão de `IndexRecommendations` em cenários com `PlanWarnings` simultâneos.
- [x] Atualizar resources (base + culturas suportadas) para novas labels/mensagens mantendo keywords SQL canônicas.
- [x] Validar borda de severidade contextual para `PW002` (`84%` => `Warning`, `85%` => `High`) nos 3 providers.
- [x] Validar borda de severidade contextual para `PW003` (`999` => `Info`, `1000` => `Warning`) nos 3 providers.
- [x] Garantir consistência entre severidade e texto/metadados (`Threshold`/`ObservedValue`) no output textual.
- [x] Refinar severidade de `PW003` com faixa crítica de leitura (`>=5000` => `High`) para priorização de risco extremo.
- [x] Padronizar `Threshold` em formato técnico estável/language-neutral (ex.: `gte:100;warningGte:1000;highGte:5000`) para evitar texto não-localizável no payload.
- [x] Implementar `PW004` para consultas sem `WHERE` com alto `EstimatedRowsRead`, com severidade contextual (`Warning`/`High`) e metadados técnicos estáveis.
- [x] Implementar `PW005` para `DISTINCT` em alto `EstimatedRowsRead`, com severidade contextual (`Warning`/`High`) e metadados técnicos estáveis.



### PlanWarnings (rodada de manutenção e robustez de contrato)
- [x] Extrair cenários de `PlanWarnings` para base compartilhada entre providers com wiring mínimo por provider.
- [x] Reduzir duplicação nos testes `ExecutionPlanTests` de MySQL/SQL Server/SQLite removendo cenários duplicados de warnings.
- [x] Adicionar testes explícitos para ordem estável do contrato textual de warning: `Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`, `MetricName`, `ObservedValue`, `Threshold`.
- [x] Adicionar validação de formato parseável para `Threshold` (`key:value;key:value`) no output de warnings.
- [x] Revisar sobreposição `PW004` (sem `WHERE`) vs `PW005` (`DISTINCT`) e suprimir ruído redundante quando `DISTINCT` já caracteriza leitura alta sem filtro.
- [x] Manter cobertura de não regressão de `IndexRecommendations` coexistindo com `PlanWarnings`.
- [x] Validar consistência i18n: todas as chaves de `SqlExecutionPlanMessages` presentes em `resx` base + `de/es/fr/it/pt`.
- [x] Validar preservação de tokens SQL canônicos sem tradução (`WHERE`, `ORDER BY`, `DISTINCT`, `LIMIT/TOP/FETCH`, `SELECT *`).

### PlanWarnings (rodada corretiva - sem perda de cobertura)
- [x] Preservar os testes de `ExecutionPlanTests` específicos por provider (wiring/dialeto/comportamento próprio) sem deleções massivas.
- [x] Consolidar apenas duplicação real de PlanWarnings na base compartilhada (`ExecutionPlanPlanWarningsTestsBase`).
- [x] Cobrir explicitamente a matriz `PW004` vs `PW005`: (a) sem `WHERE` e sem `DISTINCT`, (b) com `DISTINCT` e sem `WHERE`, (c) com `WHERE` e `DISTINCT`.
- [x] Reforçar teste unitário/formatação para confirmar ordem fixa dos campos: `Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`, `MetricName`, `ObservedValue`, `Threshold`.
- [x] Reforçar validação de `Threshold` em padrão técnico parseável no formatter e na integração de warnings.
- [x] Garantir por teste que `IndexRecommendations` permanece ativo quando coexistem `PlanWarnings`.
- [x] Validar por reflexão que todas as chaves acessadas em `SqlExecutionPlanMessages` existem no `resx` base e que as culturas (`de/es/fr/it/pt`) contêm o conjunto completo.

Decisões adotadas nesta rodada:
- A deduplicação permaneceu restrita aos cenários comuns de `PlanWarnings`; cenários de índice e wiring continuaram nos arquivos de provider.
- A heurística de baixo risco manteve supressão de `PW004` quando `DISTINCT` já explica leitura alta sem filtro, com cobertura adicional para o caso `WHERE + DISTINCT` (mantém `PW005`, não emite `PW004`).
- O contrato textual foi reforçado com testes unitários do formatter, evitando depender apenas de integração end-to-end.
- A matriz `PW004/PW005` recebeu verificação adicional para preservar `PW002` quando aplicável (`WHERE + DISTINCT`), reduzindo ruído sem ocultar sinal relevante.
- Foi adicionado caso negativo explícito para `PW005` sem `DISTINCT`, evitando falso-positivo regressivo.
- A geração de `Threshold` técnico no advisor foi centralizada em helper de baixo risco para reduzir duplicação e preservar formato estável por regra.
