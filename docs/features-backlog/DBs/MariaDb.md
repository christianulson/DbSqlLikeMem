# MariaDB (`DbSqlLikeMem.MariaDb`)

## MySQL link

- `MariaDB` compartilha a base do runtime MySQL e mantém o catálogo do provider principal em [DBs/MySql.md](MySql.md).

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 10.3, 10.5, 10.6, 11.0.
- Convenção da documentação: usar `10.3`, `10.5`, `10.6` e `11.0`; na API/tipos de teste, os valores equivalentes seguem como `103`, `105`, `106` e `110`.

## 2 Recursos relevantes

- Implementação estimada: **100%**.
- Provider dedicado sobre a família compartilhada de runtime MySQL, com `MariaDbDbMock`/`MariaDbConnectionMock` e resolução própria no `DbMockConnectionFactory`.
- Parser/executor compartilhado para DML comum, incluindo `INSERT`, `REPLACE`, `INSERT ... SET`, `INSERT ... SELECT`, `ON DUPLICATE KEY UPDATE`, `RETURNING` em `INSERT`/`DELETE`/`REPLACE` e expressões de `SEQUENCE`.
- `INSERT ... SET` com `LOW_PRIORITY`, `PARTITION` e `ON DUPLICATE KEY UPDATE ... RETURNING` também está coberto, consolidando a forma curta de escrita com os modificadores específicos do MariaDB.
- `INSERT ... SET ... ON DUPLICATE KEY UPDATE ... RETURNING` também está coberto, mantendo o contrato de escrita curta com resolução de conflito e projeção retornada.
- `INSERT ... SELECT ... RETURNING` agora está coberto no caminho compartilhado, preservando a projeção retornada enquanto insere linhas vindas de outro `SELECT`.
- `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE ... RETURNING` também está coberto, combinando seleção de origem, resolução de conflito e projeção de retorno no mesmo fluxo.
- `REPLACE ... SELECT ... RETURNING` também está coberto, mantendo o mesmo padrão de projeção no fluxo de substituição em lote.
- `REPLACE ... SET ... RETURNING` também está coberto, espelhando a escrita curta com substituição e projeção retornada.
- `JSON_TABLE` também cobre fontes correlacionadas contra a linha externa, reutilizando colunas do `FROM` anterior no documento JSON de entrada, inclusive com `NESTED PATH`, `EXISTS PATH`, `ON EMPTY`, `ON ERROR`, `ERROR ON EMPTY`, ramos aninhados irmãos independentes, ordinality por ramo e mistura de `EXISTS PATH` com `FOR ORDINALITY`.
- `JSON_TABLE` correlacionado também aplica `ON EMPTY` no caminho raiz enquanto continua expandindo ramos `NESTED PATH`, mantendo a projeção aninhada coerente com os valores herdados da linha externa.
- `JSON_TABLE` também trata documento externo `NULL` como ausência de linhas, sem quebrar a expansão do restante do conjunto.
- `INSERT` e `REPLACE` também cobrem `DELAYED` e `IGNORE` além de `LOW_PRIORITY`, `VALUE` singular e `PARTITION (...)` na sintaxe específica do MariaDB.
- `JSON_TABLE` executável no subset atual com `PATH`, `FOR ORDINALITY`, `EXISTS PATH`, `NESTED PATH`, `ON EMPTY`, `ON ERROR`, `strict` em paths raiz e aninhados, e fallback estrito em ramos `NESTED PATH`.
- `SOUNDS LIKE` também segue o caminho compartilhado de expressão com gate por dialeto.
- `JSON_TABLE` correlacionado com colunas da raiz usando `FOR ORDINALITY` e `EXISTS PATH` também está coberto, fechando a matriz de projeção externa na raiz.
- `JSON_TABLE` correlacionado também omite documentos externos `NULL` mesmo quando há `NESTED PATH`, mantendo a ausência de linhas e o complemento nulo consistente.
- `JSON_TABLE` correlacionado também cobre `strict` combinado com fallbacks `ON EMPTY`/`ON ERROR` em cenários com múltiplos ramos aninhados.

## 3 Restrições relevantes

- `UPDATE ... RETURNING` permanece indisponível no MariaDB e continua bloqueado pelo dialeto no mock.
- `DELETE ... RETURNING` é suportado apenas para `DELETE` de tabela única; a forma multi-tabela continua bloqueada.
- Funções agregadas continuam proibidas dentro de `RETURNING`, seguindo a limitação do banco real.

## 4 Aplicações típicas