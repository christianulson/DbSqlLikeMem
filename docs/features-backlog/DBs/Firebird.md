# Firebird / InterBase (`DbSqlLikeMem.Firebird`)

## 1 Versões simuladas

- Implementação estimada: **100%**.
- 2.1, 2.5, 3.0, 4.0, 5.0.

## 2 Recursos relevantes

- Implementação estimada: **93%**.
- Dialeto com `||` para concatenação, `FIRST`, `SKIP`, `ROWS`, `RETURNING`, `MERGE`, `WITH`, `EXECUTE BLOCK`, `ALTER SEQUENCE RESTART`, `SET GENERATOR`, `GEN_ID` e aliases de `GENERATOR` para sequence DDL, além de funções analíticas básicas, temporais de sistema e variáveis de contexto do Firebird nas surfaces de mock e Dapper.
- Sintaxe nativa de limitação de linhas do Firebird já cobre `FIRST/SKIP` e `ROWS ... TO ...` no parser e no executor do mock.
- `ORDER BY ... NULLS FIRST/LAST` já está refletido no parser e na surface de consulta do mock.
- Gates explícitos para `FUNCTION DDL` a partir da versão 3.0.
- `CREATE OR ALTER`, `RECREATE`, `ALTER PROCEDURE`, `ALTER TRIGGER`, `DROP PROCEDURE` e `DROP TRIGGER` já estão cobertos no mock.
- `CREATE PROCEDURE` e `CREATE FUNCTION` já aceitam valores padrão literais nos parametros suportados, e as funções escalares definidas pelo usuário usam esses valores quando o argumento final é omitido.
- Esses defaults precisam ficar no final da lista de parametros, como no Firebird real.
- A suite Firebird agora cobre fidelidade de CALL para procedures com parametro padrao omitido e inclui benchmark para chamadas repetidas de funcao escalar e para EXECUTE BLOCK com parametros em escopo, atribuicao de variaveis RETURN, blocos compostos aninhados, ramificacao IF e loops WHILE/FOR SELECT/FOR EXECUTE STATEMENT com BREAK/LEAVE, incluindo a forma parametrizada de EXECUTE STATEMENT e as clausulas `WITH AUTONOMOUS TRANSACTION` e `WITH CALLER PRIVILEGES`, tanto no loop quanto no caso simples; `WITH AUTONOMOUS TRANSACTION` ja preserva as alteracoes mesmo quando a transacao externa sofre rollback, e `WITH COMMON TRANSACTION` ja se mantém preso à transacao externa quando a execucao usa `ON EXTERNAL`.
- Semântica de concatenação alinhada ao Firebird real: o operador `||` segue o comportamento nativo e o subset de funções ainda é parcial.
- `EXECUTE BLOCK` segue parcial no mock: aceita parâmetros, `RETURNS`, `SUSPEND`, `EXIT`, `EXECUTE STATEMENT` simples, `IF ... THEN ... ELSE` com blocos compostos, loops `WHILE`, `FOR SELECT` e `FOR EXECUTE STATEMENT` com `BREAK`/`LEAVE`, tratamento básico de `WHEN ANY DO`/`WHEN SQLCODE [<codigo>] DO`/`WHEN SQLSTATE <codigo> DO`/`WHEN GDSCODE [<nome>] DO`/`WHEN EXCEPTION <nome> DO` e blocos compostos `BEGIN ... END`; os parametros declarados agora entram no escopo do corpo, a forma parametrizada de `EXECUTE STATEMENT` em loop também já está coberta, `WITH AUTONOMOUS TRANSACTION` já executa em transacao independente, e as cláusulas `WITH COMMON TRANSACTION` e `WITH CALLER PRIVILEGES` já são aceitas no subset também no caso simples, com ordem livre das clausulas opcionais já coberta pelos testes; `WITH COMMON TRANSACTION` em `ON EXTERNAL` já respeita a transação externa no mock, os handlers de erro básicos já são escolhidos em ordem, `WHEN EXCEPTION` já casa com o nome lógico da exceção simulada (`E_FAIL`), `WHEN SQLCODE` já compara o `ErrorCode` da exceção Firebird simulada e agora também cobre a forma `SQLCODE -803` para violações de chave, `WHEN SQLSTATE` já cobre `23000` para as falhas de integridade simuladas, e `WHEN GDSCODE` já reconhece um pequeno conjunto de aliases do provider em falhas Firebird simuladas, incluindo `no_dup`, `unique_key_violation`, `primary_key`, `primary_key_violation`, `primary_key_exists`, `primary_key_notnull`, `not_valid`, `foreign_key`, `foreign_key_violation`, `referenced_row` e `not_null_violation`, com suporte a listas separadas por virgula nos seletores de `WHEN SQLCODE` e `WHEN GDSCODE`, mas ainda faltam as variacoes PSQL mais completas.
- `AS CURSOR` já é aceito nos loops `FOR SELECT` e `FOR EXECUTE STATEMENT` do subset Firebird.
- As cláusulas `ON EXTERNAL [DATA SOURCE]`, `AS USER`, `PASSWORD` e `ROLE` já são aceitas pelo parser do `EXECUTE STATEMENT` no subset, com ordem livre das clausulas opcionais coberta pelos testes, o mock já reflete `AS USER`/`ROLE` nos valores de contexto `CURRENT_USER`/`CURRENT_ROLE` durante a execução, e o clone externo preserva um `ConnectionString` observável com `DATA SOURCE`, `USER`, `ROLE` e `PASSWORD` tanto no caso simples quanto no loop `FOR EXECUTE STATEMENT`, com `DataSource` derivado dessa string, mas a semântica de conexão externa ainda não foi implementada.
- `ALTER SEQUENCE RESTART [WITH]` já está coberto no mock e respeita rollback transacional.
- `CREATE/DROP GENERATOR` e `ALTER GENERATOR RESTART [WITH]` já estão cobertos como aliases de sequence DDL.
- `SET GENERATOR <name> TO <value>` já está coberto como alias Firebird legado de sequence DDL.
- `GEN_ID(sequence, increment)` já está coberto como alias Firebird de sequence function, com `GEN_ID(sequence, 0)` preservando o valor atual.
- `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME` e `LOCALTIMESTAMP` já estão cobertos na surface temporal do mock.
- `EXTRACT(YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/WEEK/WEEKDAY/YEARDAY/MILLISECOND FROM CURRENT_DATE/CURRENT_TIMESTAMP)` já está coberto na surface temporal do mock e na surface Dapper.
- `DATEADD(1 DAY TO CURRENT_TIMESTAMP)` já está coberto no Firebird como expressão temporal nativa.
- O parser do Firebird agora reconhece `DATEADD(1 DAY TO CURRENT_TIMESTAMP)` e traduz a sintaxe `TO` para a surface de execução do mock.
- O parser do Firebird agora reconhece `CRYPT_HASH('texto' USING SHA256)` e `HASH('texto' USING CRC32)` e traduz a sintaxe `USING` para a surface de execução do mock.
- O ponto `Auto` agora também registra o subconjunto Firebird para cobrir `DATEADD`, `HASH`, `CRYPT_HASH` e `GEN_ID` no modo automatico.
- O fallback `Auto` do assembly base tambem executa esse subconjunto Firebird quando o provider esta disponivel.
- As formas keyword com espaço `CURRENT DATE`, `CURRENT TIME` e `CURRENT TIMESTAMP` já estão cobertas na surface temporal do mock.
- `CURRENT_USER`, `USER`, `CURRENT_ROLE`, `CURRENT_DATABASE` e `CURRENT_CONNECTION` já estão cobertos como variáveis de contexto do Firebird.
- As mesmas variáveis de contexto também estão cobertas na surface Dapper do Firebird.
- `CURRENT_TRANSACTION` também está coberto quando existe transacao ativa.
- `SESSION_ID` e `TRANSACTION_ID` também estão cobertos como aliases Firebird das variáveis de contexto.
- `CURRENT_CATALOG` também está coberto como alias de database do Firebird.
- `RDB$GET_CONTEXT('SYSTEM', ...)` já está coberto para as variáveis de contexto suportadas, incluindo `NETWORK_PROTOCOL`, `CLIENT_HOST`, `CLIENT_PID`, `CLIENT_ADDRESS`, `CLIENT_PROCESS`, `ENGINE_VERSION`, `ISOLATION_LEVEL`, `ROW_COUNT`, `WIRE_COMPRESSED` e `WIRE_ENCRYPTED`.
- Os literais temporais Firebird `NOW`, `TODAY`, `TOMORROW` e `YESTERDAY` já estão cobertos na surface de avaliação.
- `SQLSTATE`, `SQLCODE` e `GDSCODE` já estão cobertos como variáveis de contexto Firebird em modo sem erro.
- `COALESCE`, `IIF`, `DECODE` e `NULLIF` já estão cobertas como funções condicionais do Firebird nas surfaces de mock e Dapper.
- O registro escalar do Firebird também cobre `RDB$GET_CONTEXT`, `RDB$SET_CONTEXT`, `GEN_ID`, `OVERLAY`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `ASCII_CHAR`, `ASCII_VAL`, `UNICODE_CHAR`, `UNICODE_VAL`, `BASE64_ENCODE`, `BASE64_DECODE`, `HEX_ENCODE`, `HEX_DECODE`, `CRYPT_HASH`, `CHAR_TO_UUID`, `UUID_TO_CHAR`, `GEN_UUID`, `HASH`, `TRUNC`, `BIN_AND`, `BIN_OR`, `BIN_XOR`, `BIN_NOT` e `BIN_SHL`/`BIN_SHR`.
- `MAXVALUE` e `MINVALUE` já estão cobertos como funções de comparação Firebird nas surfaces de mock e Dapper.
- `CHAR`, `NCHAR`, `ASCII_CHAR` e `ASCII_VAL` já estão cobertos como funções de caractere Firebird nas surfaces de mock e Dapper.
- `UNICODE_CHAR` e `UNICODE_VAL` já estão cobertos como funções de caractere Firebird nas surfaces de mock e Dapper.
- `CHAR_TO_UUID` e `UUID_TO_CHAR` já estão cobertos como funções Firebird de conversão de UUID nas surfaces de mock e Dapper.
- `GEN_UUID` já está coberto como função Firebird de geração de UUID nas surfaces de mock e Dapper.
- `MD5`, `HEX` e `UNHEX` seguem cobertas como aliases de compatibilidade de hash e conversão hexadecimal, enquanto `BASE64_ENCODE`, `BASE64_DECODE`, `HEX_ENCODE`, `HEX_DECODE` e `CRYPT_HASH` já cobrem os nomes oficiais do Firebird 4+/5 nas surfaces de mock e Dapper.
- `HASH` já está coberto como função Firebird de hash estável nas surfaces de mock e Dapper.
- `RAND` já está coberto como função Firebird de número aleatório nas surfaces de mock e Dapper.
- `POSITION`, `LOCATE`, `REPLACE` e `REVERSE` já estão cobertas como funções Firebird de busca e transformação de texto nas surfaces de mock e Dapper.
- `REPEAT` e `TRANSLATE` já estão cobertas como funções Firebird de repetição e translacao de texto nas surfaces de mock e Dapper.
- `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM` e `SUBSTRING` já estão cobertas como funções Firebird de transformação de texto nas surfaces de mock e Dapper.
- `SPACE`, `LEFT`, `RIGHT`, `LPAD` e `RPAD` já estão cobertas como funções Firebird de preenchimento e corte de texto nas surfaces de mock e Dapper.
- `CHAR_LENGTH` e `CHARACTER_LENGTH` já estão cobertas como funções Firebird de comprimento de texto nas surfaces de mock e Dapper.
- `ABS`, `CEILING`, `FLOOR`, `SIGN` e `SQRT` já estão cobertas como funções Firebird numéricas auxiliares nas surfaces de mock e Dapper.
- `TRUNC` já está coberta como função Firebird numérica de truncamento nas surfaces de mock e Dapper.
- `ABSVAL` e `BIN` já estão cobertas como aliases Firebird numéricos nas surfaces de mock e Dapper.
- `CEIL` e `POW` já estão cobertas como aliases Firebird numéricos nas surfaces de mock e Dapper.
- `DEGREES`, `GREATEST` e `LEAST` já estão cobertas como funções Firebird numéricas auxiliares nas surfaces de mock e Dapper.
- `MOD`, `PI`, `POWER` e `RADIANS` já estão cobertas como funções Firebird numéricas auxiliares nas surfaces de mock e Dapper.
- `COT` e `LOG(base, value)` já estão cobertas como funções Firebird numéricas auxiliares nas surfaces de mock e Dapper.
- `ROUND` já está coberta como função Firebird numérica de arredondamento nas surfaces de mock e Dapper.
- `BIT_LENGTH` e `OCTET_LENGTH` já estão cobertos como funções de comprimento de string Firebird nas surfaces de mock e Dapper.
- `OVERLAY` já está coberto como função Firebird de substituição parcial de texto nas surfaces de mock e Dapper.
- `BIN_AND`, `BIN_OR`, `BIN_XOR`, `BIN_NOT`, `BIN_SHL` e `BIN_SHR` já estão cobertos como funções bitwise Firebird nas surfaces de mock e Dapper.
- Agregação Firebird com `COUNT`, `SUM`, `HAVING`, `ORDER BY` ordinal e `LIST()` já está coberta nas surfaces de mock e Dapper.
- As variáveis de trigger Firebird `INSERTING`, `UPDATING` e `DELETING` já estão cobertas durante a execução de triggers da surface mock e Dapper.
- `RDB$SET_CONTEXT` já está coberto para `USER_SESSION` e `USER_TRANSACTION`.
- Surface do provider já inclui factory, data source, batch, projection/returning, suíte de testes dedicada e benchmarks Firebird em memória, incluindo o slice `ExecuteBlockSqlState23000` (`EXECUTE BLOCK` com `WHEN SQLSTATE`), e o projeto `benchmark/DbSqlLikeMem.Benchmarks` já ganhou as suítes Firebird em memória e Firebird Testcontainers e os catálogos da matriz; o que ainda falta é ampliar a comparação externa, publicar a Wiki e completar as matrizes de performance do Firebird.
- `EXECUTE STATEMENT ... INTO` no caso simples já está coberto no mock e na surface Dapper, com atribuição da primeira linha retornada às variáveis locais do bloco.

### TODO Implementação

- TODO: modelar com mais fidelidade a conexão externa de `EXECUTE STATEMENT`, incluindo alvo externo real e as diferenças de isolamento que ainda não existem no mock.
- TODO: revisar as variações PSQL restantes do `EXECUTE BLOCK`, especialmente os handlers mais específicos de exceção ainda não modelados.

### TODO Fidelidade

- TODO: criar testes de fidelidade para os handlers de exceção mais específicos que ainda faltam no `EXECUTE BLOCK`.

### TODO Benchmark

- TODO: completar a cobertura Firebird no projeto `benchmark/DbSqlLikeMem.Benchmarks`, fechando o catálogo/wiki e ampliando a comparação externa além da suíte `DbSqlLikeMem` já criada.
- TODO: ampliar a comparação externa do Firebird no benchmark e revisar a publicação da Wiki com os relatórios gerados.
- TODO: configurar benchmarks separados para `EXECUTE STATEMENT` simples, `EXECUTE STATEMENT ... INTO`, `FOR EXECUTE STATEMENT` parametrizado e cenários com transacao/autenticacao externa.
- TODO: criar mais benchmarks para as novas variações PSQL assim que os caminhos de execução entrarem no mock.

### TODO Documentação e Wiki

- TODO: atualizar a documentação Firebird com tudo o que ainda estiver pendente na implementação, nos testes de fidelidade e nos benchmarks.
- TODO: refletir na Wiki o progresso real do Firebird, incluindo os itens já entregues e os itens que ainda faltam.
- TODO: manter o backlog Firebird sincronizado com os novos cenários cobertos no mock, na surface Dapper e no projeto de benchmark.

## 3 Aplicações típicas

- Implementação estimada: **90%**.
- Sistemas legados e aplicações corporativas com Firebird embutido ou on-premises.
- Cenários de migração gradual para bancos com sintaxe SQL próxima, sem perder as regras específicas do Firebird.
