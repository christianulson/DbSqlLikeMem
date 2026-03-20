# Oracle (`DbSqlLikeMem.Oracle`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 7, 8, 9, 10, 11, 12, 18, 19, 21, 23.

## 2 Recursos relevantes

- Implementação estimada: **90%**.
- Parser/executor para DDL/DML comuns.
- Diferenças de dialeto por versão simulada.
- Cobertura de `LISTAGG` ampliada com separador customizado, comportamento padrão sem delimitador quando omitido e ordenação interna via `WITHIN GROUP` (incluindo combinações com `DISTINCT`).
- P8 consolidado: suporte a `FETCH FIRST/NEXT` por versão e contratos de ordenação por dialeto.
- Funções-chave do banco: `LISTAGG`, `NVL`, `JSON_VALUE` (subset escalar) e operações de data por versão.
- TODO: implementar `JSON_TABLE` no parser/executor do Oracle, hoje ainda fora do subset apesar de o banco real suportar projeção relacional de JSON em `FROM`.
- Incremento desta sessão: o executor de `PIVOT` passou a cobrir também `MIN`, `MAX` e `AVG` no caminho Oracle, além de alinhar buckets vazios/nulos à semântica agregadora compartilhada.
- TODO: completar executor de `PIVOT` para agregadores avançados relevantes do Oracle além do conjunto comum `COUNT/SUM/MIN/MAX/AVG`, mantendo coerência com `SupportsPivotClause`.
- TODO: avaliar `MATCH_RECOGNIZE` como trilha separada de parser/executor avançado para cenários analíticos reais do Oracle.

## 3 Aplicações típicas

- Implementação estimada: **90%**.
- Ambientes com legado Oracle e migração gradual de versões.
- Validação de SQL de camada de integração sem depender do ambiente corporativo.

# PostgreSQL / Npgsql (`DbSqlLikeMem.Npgsql`)
