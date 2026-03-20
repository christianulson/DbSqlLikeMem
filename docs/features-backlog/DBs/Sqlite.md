# SQLite (`DbSqlLikeMem.Sqlite`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 3.

## 2 Recursos relevantes

- Implementação estimada: **88%**.
- `WITH`/CTE disponível.
- Operadores JSON `->` e `->>` disponíveis no parser do dialeto.
- Cobertura de `GROUP_CONCAT` ampliada com separador customizado, `DISTINCT`, tratamento de `NULL` e ordenação interna via sintaxe nativa `ORDER BY` dentro da função; `WITHIN GROUP` permanece explicitamente bloqueado no dialeto.
- P8 consolidado: `LIMIT/OFFSET` e ordenação com regras de compatibilidade por versão simulada.
- Funções-chave do banco: `GROUP_CONCAT`, `IFNULL`, funções de data (`date`, `datetime`, `strftime`) e `JSON_EXTRACT` (subset).
- TODO: implementar table-valued JSON functions `json_each(...)`/`json_tree(...)` no parser/executor do SQLite para cenários reais de shredding de JSON em `FROM`.
- TODO: ampliar a malha de window functions do SQLite para cobrir explicitamente `EXCLUDE`, window chaining e os detalhes adicionais de frame que o banco real suporta.

## 3 Restrições relevantes

- Implementação estimada: **100%**.
- `ON DUPLICATE KEY UPDATE` não suportado (usa `ON CONFLICT`).
- Operador null-safe `<=>` não suportado.

## 4 Aplicações típicas

- Implementação estimada: **90%**.
- Testes leves com dependência mínima de infraestrutura.
- Simulação de cenários embarcados/offline.

# DB2 (`DbSqlLikeMem.Db2`)
