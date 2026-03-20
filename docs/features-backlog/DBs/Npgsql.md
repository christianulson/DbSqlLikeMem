# PostgreSQL / Npgsql (`DbSqlLikeMem.Npgsql`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17.

## 2 Recursos relevantes

- Implementação estimada: **88%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `STRING_AGG` ampliada para agregação textual com `DISTINCT`, `NULL` e ordenação por grupo via `WITHIN GROUP`, com gate por função/dialeto e mensagens acionáveis em sintaxe malformada.
- P7/P10 consolidado: `RETURNING` sintático mínimo em caminhos suportados e fluxo de procedures no contrato Dapper.
- Funções-chave do banco: `STRING_AGG`, operadores JSON (`->`, `->>`, `#>`, `#>>`) e expressões de data por intervalo.
- TODO: implementar `DISTINCT ON (...)` no parser/executor do PostgreSQL, incluindo a regra do banco real que exige compatibilidade com os itens mais à esquerda de `ORDER BY`.
- TODO: implementar `LATERAL` em `FROM`/`JOIN` no parser/executor do Npgsql para subqueries/funções correlacionadas à esquerda, hoje fora da malha principal do mock.

## 3 Aplicações típicas

- Implementação estimada: **90%**.
- Projetos modernos com Npgsql em APIs/serviços.
- Ensaios de portabilidade SQL entre PostgreSQL e outros bancos.

# SQLite (`DbSqlLikeMem.Sqlite`)
