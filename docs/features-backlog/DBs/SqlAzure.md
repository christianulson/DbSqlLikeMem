# SqlAzure (`DbSqlLikeMem.SqlAzure`)

## SQL Server link

- `SqlAzure` compartilha a base do runtime SQL Server e mantém o catálogo do provider on-premises em [DBs/SqlServer.md](SqlServer.md).

## 1 Versões simuladas

- Implementação estimada: **100%**.
- Compatibilidade simulada: 100, 110, 120, 130, 140, 150, 160, 170.
- Convenção da documentação: usar os níveis de compatibilidade `100` a `170`; na leitura humana, isso acompanha a base de versão SQL Server `2008` até `2025` que o provider compartilha.

## 2 Recursos relevantes

- Implementação estimada: **95%**.
- Provider Azure sobre a base compartilhada do dialeto SQL Server, com mapping explícito de `compatibility level` para gates de parser e executor.
- Suporte ao subconjunto comum de `STRING_AGG`, `OFFSET/FETCH`, `OUTPUT`, `MERGE`, `PIVOT/UNPIVOT`, `CROSS APPLY`/`OUTER APPLY`, `OPENJSON`, `STRING_SPLIT` e `FOR JSON`.
- Regras específicas de compatibilidade já refletidas no backlog: `STRING_AGG ... WITHIN GROUP` no parser, transação/lifecycle (`commit`, `rollback`, savepoint, `Close`/`Open`, reset volátil) e regressões do caminho compartilhado de batch.
- Usa as mesmas bases de execução e de testes do SQL Server, mas mantém identidade própria para explicitar os níveis de compatibilidade Azure e evitar misturar contrato de deployment com o provider on-premises.
- TODO: fechar a malha de diferenças residuais por `compatibility level` onde ainda houver drift comprovado entre Azure e SQL Server compartilhado.

## 3 Aplicações típicas

- Testes de portabilidade para apps que usam `SqlClient` com Azure SQL Database.
- Validação de comportamento por `compatibility level` sem abrir mão do runtime compartilhado com SQL Server.
- Regresões de transação, batch e JSON tabular com foco em compatibilidade de nuvem.
