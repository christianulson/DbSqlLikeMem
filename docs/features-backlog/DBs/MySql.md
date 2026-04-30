# MySQL (`DbSqlLikeMem.MySql`)

## MariaDB link

- `MariaDB` tem backlog próprio em [DBs/MariaDb.md](MariaDb.md) e compartilha parte da base com este provider.

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 3.0, 4.0, 5.5, 5.6, 5.7, 8.0, 8.4.
- Convenção da documentação: usar `3.0`, `4.0`, `5.5`, `5.6`, `5.7`, `8.0` e `8.4`; na API/tipos de teste, os valores equivalentes seguem como `30`, `40`, `55`, `56`, `57`, `80` e `84`.

## 2 Recursos relevantes

- Implementação estimada: **99%**.
- Parser/executor para DDL/DML comuns.
- Suporte a `INSERT ... ON DUPLICATE KEY UPDATE`.
- Cobertura de `GROUP_CONCAT` ampliada com regressão para `DISTINCT`, tratamento de `NULL` e ordenação interna pela sintaxe nativa `ORDER BY ... SEPARATOR ...` dentro da função.
- P7 consolidado: UPSERT por família (`ON DUPLICATE`/`ON CONFLICT`/`MERGE subset`) e mutações avançadas com contracts por strategy tests.
- Funções-chave do banco: `GROUP_CONCAT`, `IFNULL`, `DATE_ADD` e `JSON_EXTRACT` (subset no mock).
- Status por versão já explicitado nesta trilha:
  - `5.0+`: `JSON_EXTRACT`, `->` e `->>`.
  - `8.0+`: `WITH`/`WITH RECURSIVE`, `JSON_TABLE` e window functions.
  - Todas as versões simuladas atuais do mock: `LIMIT/OFFSET`, `ON DUPLICATE KEY UPDATE`, `MATCH ... AGAINST`, `SQL_CALC_FOUND_ROWS`/`FOUND_ROWS`, `USE/IGNORE/FORCE INDEX`, `<=>` e `GROUP_CONCAT` dentro do subset já coberto.

### 2.1 Particionamento

- Implementação estimada: **99%**.
- **Já implementado:**
  - metadata de `CREATE TABLE`;
  - `PARTITION BY RANGE (YEAR(...))`;
  - `PARTITION BY LIST (YEAR(...))`;
  - `INSERT ... PARTITION (...)`;
  - roteamento automático quando a linha cai em uma partição conhecida;
  - leitura com `FROM ... PARTITION (...)`;
  - `MAXVALUE`;
  - pruning seguro:
    - igualdade;
    - `IN (...)`;
    - `BETWEEN`;
    - `OR`;
    - faixa direta por data alinhada ao ano;
    - `YEAR(col)`;
    - `EXTRACT(YEAR FROM col)`;
    - `EXTRACT(YEAR FROM col) IN (...)`;
    - `EXTRACT(YEAR FROM col) BETWEEN ... AND ...`;
    - `EXTRACT(YEAR FROM col) >= ... AND EXTRACT(YEAR FROM col) < ...`;
    - faixa invertida com `EXTRACT(YEAR FROM col)`;
    - `EXTRACT(YEAR FROM col)` com `OR` em faixas distintas;
    - `EXTRACT(YEAR FROM col)` com `OR` em `BETWEEN`.
- **A implementar:**
  - pruning mais amplo fora do subset seguro de `YEAR` e `EXTRACT`.

## 3 Restrições relevantes

- Implementação estimada: **100%**.
- Nenhuma restrição adicional relevante além das limitações já cobertas pela família compartilhada.

## 4 Aplicações típicas

- Implementação estimada: **100%**.
- Legados com SQL histórico do ecossistema MySQL.
- Validação de comportamento de upsert no fluxo de escrita.
- Benchmark comparativo controlado já disponível contra MySQL real por `Testcontainers` e modo `preprovisioned`, com suites dedicadas, baseline publicada na wiki e reutilização do mesmo catálogo de cenários do mock.
