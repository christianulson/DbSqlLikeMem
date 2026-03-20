# MySQL (`DbSqlLikeMem.MySql`)

## MariaDB link

- `MariaDB` tem backlog próprio em [DBs/MariaDb.md](MariaDb.md) e compartilha parte da base com este provider.

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 3.0, 4.0, 5.5, 5.6, 5.7, 8.0, 8.4.
- Convenção da documentação: usar `3.0`, `4.0`, `5.5`, `5.6`, `5.7`, `8.0` e `8.4`; na API/tipos de teste, os valores equivalentes seguem como `30`, `40`, `55`, `56`, `57`, `80` e `84`.

## 2 Recursos relevantes

- Implementação estimada: **89%**.
- Parser/executor para DDL/DML comuns.
- Suporte a `INSERT ... ON DUPLICATE KEY UPDATE`.
- Cobertura de `GROUP_CONCAT` ampliada com regressão para `DISTINCT`, tratamento de `NULL` e ordenação interna pela sintaxe nativa `ORDER BY ... SEPARATOR ...` dentro da função.
- P7 consolidado: UPSERT por família (`ON DUPLICATE`/`ON CONFLICT`/`MERGE subset`) e mutações avançadas com contracts por strategy tests.
- Funções-chave do banco: `GROUP_CONCAT`, `IFNULL`, `DATE_ADD` e `JSON_EXTRACT` (subset no mock).
- Status por versão já explicitado nesta trilha:
  - `5.0+`: `JSON_EXTRACT`, `->` e `->>`.
  - `8.0+`: `WITH`/`WITH RECURSIVE` e window functions.
  - Todas as versões simuladas atuais do mock: `LIMIT/OFFSET`, `ON DUPLICATE KEY UPDATE`, `MATCH ... AGAINST`, `SQL_CALC_FOUND_ROWS`/`FOUND_ROWS`, `USE/IGNORE/FORCE INDEX`, `<=>` e `GROUP_CONCAT` dentro do subset já coberto.
- TODO: implementar `JSON_TABLE(...)` no parser/executor do MySQL, hoje ainda só com gate explícito de não suportado, apesar de o banco real suportar a função de tabela JSON.
- TODO: avaliar subset de particionamento lógico por tabela (`PARTITION BY RANGE/LIST`) para aproximar testes de retenção/time-series de capacidades reais do MySQL/InnoDB.

## 3 Restrições relevantes

- Implementação estimada: **100%**.
- Nenhuma restrição adicional relevante além das limitações já cobertas pela família compartilhada.

## 4 Aplicações típicas

- Implementação estimada: **100%**.
- Legados com SQL histórico do ecossistema MySQL.
- Validação de comportamento de upsert no fluxo de escrita.
- Benchmark comparativo controlado já disponível contra MySQL real por `Testcontainers` e modo `preprovisioned`, com suites dedicadas, baseline publicada na wiki e reutilização do mesmo catálogo de cenários do mock.
