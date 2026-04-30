# PostgreSQL / Npgsql (`DbSqlLikeMem.Npgsql`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17.

## 2 Recursos relevantes

- Implementação estimada: **90%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `STRING_AGG` ampliada para agregação textual com `DISTINCT`, `NULL` e ordenação por grupo via `WITHIN GROUP`, com gate por função/dialeto e mensagens acionáveis em sintaxe malformada.
- P7/P10 consolidado: `RETURNING` sintático mínimo em caminhos suportados e fluxo de procedures no contrato Dapper.
- Funções-chave do banco: `STRING_AGG`, operadores JSON (`->`, `->>`, `#>`, `#>>`) e expressões de data por intervalo.
- `COT` já está coberta como função numérica auxiliar do PostgreSQL nas surfaces de mock e Dapper.
- `GREATEST`, `LEAST` e `MOD` já estão cobertas como funções numéricas auxiliares do PostgreSQL nas surfaces de mock e Dapper.
- `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `COS`, `EXP`, `SIN` e `TAN` já estão cobertas como funções transcendentais do PostgreSQL nas surfaces de mock e Dapper.
- Cobertura de `DISTINCT ON (...)` no parser/executor do PostgreSQL, incluindo a regra do banco real que exige compatibilidade com os itens mais à esquerda de `ORDER BY`.
- Cobertura de `LATERAL` em `FROM`/`JOIN` no parser/executor do Npgsql para subqueries/funções correlacionadas à esquerda, com o contrato já refletido no mock e na suite de fidelidade.
- Cobertura de `MERGE` nas versões do PostgreSQL que suportam a instrução, com contrato de fidelidade alinhado ao parser e ao executor do mock.

## 3 Aplicações típicas

- Implementação estimada: **90%**.
- Projetos modernos com Npgsql em APIs/serviços.
- Ensaios de portabilidade SQL entre PostgreSQL e outros bancos.

# SQLite (`DbSqlLikeMem.Sqlite`)
