# DB2 (`DbSqlLikeMem.Db2`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 8, 9, 10, 11.

## 2 Recursos relevantes

- Implementação estimada: **100%**.
- `WITH`/CTE disponível.
- `MERGE` disponível (>= 9).
- `FETCH FIRST` suportado.
- Cobertura de `LISTAGG` ampliada com separador customizado, `DISTINCT`, tratamento de `NULL` e ordenação ordered-set via `WITHIN GROUP`, incluindo validações sintáticas malformadas.
- P9 consolidado: fallback explícito de não suportado para JSON avançado e cobertura de `FETCH FIRST` no dialeto DB2.
- Funções-chave do banco: `LISTAGG` (por versão), `COALESCE`, `TIMESTAMPADD` e `FETCH FIRST` no fluxo de paginação.
- `JSON_QUERY`, `JSON_VALUE` e `JSON_TABLE` cobertos no subset JSON do DB2, com gate por versão e materialização em runtime.
- `CREATE OR REPLACE FUNCTION`, `CREATE OR REPLACE PROCEDURE` e `CREATE OR REPLACE TRIGGER` cobertos no parser e no runtime do mock DB2.
- `RAND` já está coberta como função numérica aleatória do DB2 nas surfaces de mock e Dapper.
- `COT` já está coberta como função numérica auxiliar do DB2 nas surfaces de mock e Dapper.
- `ABSVAL`, `MOD`, `TRUNC` e `TRUNCATE` já estão cobertas como funções/aliases matematicos do DB2 nas surfaces de mock e Dapper.
- `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `COS`, `EXP`, `SIN` e `TAN` já estão cobertas como funções transcendentais do DB2 nas surfaces de mock e Dapper.

## 3 Restrições relevantes

- Implementação estimada: **100%**.
- `LIMIT/OFFSET` não suportado no dialeto DB2.
- `ON DUPLICATE KEY UPDATE` não suportado.
- Operador null-safe `<=>` não suportado.
- Operadores JSON `->` e `->>` não suportados.

## 4 Aplicações típicas

- Implementação estimada: **90%**.
- Cenários corporativos com DB2 legado.
- Testes de SQL portado de outros dialetos para DB2.

