# Fase 5 - Funções e semântica

## Status

IN PROGRESS

## Percentual de entrega

98%

## O que foi feito

- Adicionado o primeiro wrapper de `JsonTableFunctionTestsBase` na suite de fidelidade e iniciado o coverage de `json_each` e `json_tree`.
- Adicionado o wrapper de fidelidade para `JSON insert/cast`, cobrindo o benchmark escalar de leitura JSON com coerção.
- Ligados os handlers de `json_each` e `json_tree` ao executor de table functions do mock.
- Mantida a validacao negativa quando o provider nao suporta funcoes JSON tabulares.
- Expandido o parser e o avaliador para `CAST`, `CONVERT`, `TRY_CAST`, `TRY_CONVERT`, `PARSE` e `TRY_PARSE`, com restricao por capability no dialect.
- Adicionados caminhos de sintaxe e avaliacao para funcoes Firebird como `DATEADD`, `SUBSTRING`, `HASH` e `CRYPT_HASH` quando o dialect suporta a chamada.
- Adicionado suporte a `DATETRUNC` no SQL Server 2022+, com registro de capability, parser e avaliacao temporal compartilhada.
- Adicionado o contrato de capability para `DATETRUNC` e `FROM PARTS` no SQL Server, com cobertura de parser para o limite de versao e o grupo de construtores nativos.
- Adicionada cobertura de `DATEPART(tz)`, `DATENAME(tz)`, `DATEPART(tzoffset)`, `DATENAME(tzoffset)`, `TODATETIMEOFFSET` e `SWITCHOFFSET` na suite de fidelidade temporal compartilhada para SQL Server e SQL Azure.
- Adicionada cobertura compartilhada para os construtores temporais `DATEFROMPARTS`, `DATETIMEFROMPARTS`, `DATETIME2FROMPARTS`, `DATETIMEOFFSETFROMPARTS`, `TIMEFROMPARTS` e `SMALLDATETIMEFROMPARTS` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `EOMONTH` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `DATEDIFF_BIG` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `DATETRUNC` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `JSON_QUERY` sem path no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `JSON_MODIFY` com substituicao de propriedade aninhada no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `STRING_ESCAPE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `TRANSLATE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `COMPRESS` e `DECOMPRESS` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `FORMATMESSAGE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `ISJSON` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `FORMAT` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para o nucleo matematico comum com `ABS`, `CEILING`/`CEIL`, `DEGREES`, `FLOOR`, `LN`, `LOG10`, `LOG(base, value)`, `PI`, `POWER`, `RADIANS`, `ROUND`, `SIGN`, `SQRT` e `SQUARE` nos providers com suporte ao contrato correspondente.
- Adicionada cobertura compartilhada para `COT` nos providers com suporte ao contrato correspondente.
- Adicionada cobertura compartilhada para `BIN`, `GREATEST`, `LEAST`, `LOG2`, `MOD`, `POW` e `TRUNCATE` nos providers MySQL e MariaDB.
- Adicionada cobertura compartilhada para `GREATEST`, `LEAST` e `MOD` nos providers Npgsql e Firebird.
- Adicionada cobertura compartilhada para `ABSVAL`, `MOD`, `TRUNC` e `TRUNCATE` no DB2.
- Adicionada cobertura compartilhada para `TRUNC` numerico no Oracle.
- Adicionada cobertura compartilhada para `ABSVAL`, `BIN`, `COSH`, `SINH`, `TANH` e `TRUNC` no Firebird.
- Adicionada cobertura compartilhada para a extensao matematica `SQLITE_ENABLE_MATH_FUNCTIONS` no SQLite.
- Adicionada cobertura compartilhada para `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `COS`, `EXP`, `SIN` e `TAN` nos providers com suporte ao contrato transcendente correspondente.
- O inventario das funcoes matematicas aceitas pelos providers que ainda nao entram no contrato compartilhado foi registrado em [Fase-09-Funcoes-Matematicas.md](./Fase-09-Funcoes-Matematicas.md).
- Adicionada cobertura compartilhada para `ASCII`, `CHARINDEX`, `BINARY_CHECKSUM`, `CHECKSUM`, `REPLICATE`, `REVERSE`, `SPACE` e `STUFF` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `PARSENAME`, `QUOTENAME` e `STR` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `APP_NAME`, `CONNECTIONPROPERTY`, `DATABASEPROPERTYEX`, `DATABASE_PRINCIPAL_ID`, `CURRENT_USER`, `COLUMNPROPERTY`, `COL_LENGTH`, `COL_NAME`, `DB_ID`, `DB_NAME`, `OBJECT_ID`, `OBJECTPROPERTY`, `OBJECTPROPERTYEX`, `OBJECT_NAME`, `OBJECT_SCHEMA_NAME`, `ORIGINAL_DB_NAME`, `SCHEMA_ID`, `SCHEMA_NAME`, `GETUTCDATE`, `SYSDATETIMEOFFSET` e `SYSUTCDATETIME` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `SCOPE_IDENTITY` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `@@DATEFIRST`, `@@IDENTITY`, `@@MAX_PRECISION`, `SERVERPROPERTY`, `ORIGINAL_LOGIN`, `CURRENT_REQUEST_ID`, `SESSION_ID`, `TYPE_ID`, `TYPE_NAME`, `TYPEPROPERTY`, `SESSION_USER`, `SUSER_ID`, `SUSER_NAME`, `SUSER_SID`, `SUSER_SNAME`, `SYSTEM_USER`, `USER_ID`, `USER_NAME`, `XACT_STATE`, `CURRENT_TIMESTAMP`, `GETDATE` e `SYSDATETIME` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `CURRENT_TRANSACTION_ID` e `XACT_STATE` em transacao ativa no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `CONTEXT_INFO` e `SESSION_CONTEXT` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `GETANSINULL`, `DATALENGTH`, `GROUPING`, `GROUPING_ID`, `HOST_ID`, `HOST_NAME`, `IS_MEMBER`, `IS_ROLEMEMBER`, `IS_SRVROLEMEMBER`, `ISDATE` e `PATINDEX` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `LEN`, `LTRIM`, `RTRIM` e `UNICODE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `PARSE`, `TRY_CONVERT` e `TRY_PARSE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `SOUNDEX` e `DIFFERENCE` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `APPROX_COUNT_DISTINCT`, `PERCENTILE_CONT` e `PERCENTILE_DISC` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `COUNT_BIG`, `CHECKSUM_AGG`, `STRING_AGG`, `STDEV`, `STDEVP`, `VAR` e `VARP` no SQL Server e no SQL Azure.
- Adicionada cobertura compartilhada para `STRING_SPLIT` com `CROSS APPLY` no SQL Server e no SQL Azure.
- Adicionados contratos tipados de `RunTestAsync` em `NotFidelityTestService` e `FidelityTestService` para suportar retornos fortes nos wrappers de funcoes.
- Expandido o `ProviderSqlDialect` de testes com contratos de `APPLY`, `STRING_SPLIT`, `FOR JSON`, `OPENJSON` e categorias de funcoes SQL Server, com implementacao no `SqlServerProviderSqlDialect`.
- Ajustados os testes de `QueryServiceScalarTemporalTest` para ler `SYSDATETIMEOFFSET`, `COMPRESS` e `DECOMPRESS` sem casts diretos fragilizados por tipo de runtime.
- Tipados os wrappers de `FieldTypeFunctionTestsBase` e os retornos correspondentes de `QueryServiceScalarTemporalTest` para JSON e funcoes escalares do SQL Server, removendo casts manuais nesses caminhos.
- Tipado o fluxo de `CONTEXT_INFO` e `SESSION_CONTEXT` para manter o valor ausente como `string?` no helper compartilhado do SQL Server.
- Tipados os retornos temporais de `QueryServiceScalarTemporalTest` e os testes de `ScalarTemporalTestsBase` para `DateTime`, `DateTimeOffset`, `TimeSpan` e tuplas, removendo casts manuais nos benchmarks temporais.
- Tipados os wrappers de `STRING_AGG` e os testes de `StringAggregateTestsBase` para `string` e `QueryResultSnapshot`, removendo unwrapping manual nos asserts de agregacao.
- Tipados os wrappers de parametros e tempo atual de `QueryServiceScalarTemporalTest` para `string`, `int` e `DateTime`, removendo casts manuais nos testes de `SelectTestsBase` e `ScalarTemporalTestsBase`.
- Tipados os wrappers relacionais de `QueryServiceRelationalTest` que retornam `QueryResultSnapshot`, `int`, `long` e `string?`, e o teste relacional composto de `SelectTestsBase` passou a consumir um resultado estruturado tipado em vez de `object?`.
- Tipados os fluxos de `NEXT VALUE FOR`, `currval`, `lastval` e `session-local sequence` para retornar `long[]` diretamente nos helpers compartilhados e nos testes de provider.
- Tipados os fluxos de `ALTER SEQUENCE INCREMENT BY` e `ALTER SEQUENCE RESTART WITH` para retornar `long[]` diretamente nos helpers compartilhados e nos testes de provider.
- Tipados os fluxos de `setval` e `schema-qualified sequence` para retornar `long[]` diretamente nos helpers compartilhados e nos testes de provider.
- Tipados os fluxos de `OWNED BY`, `CYCLE`, `MAXVALUE`, `MINVALUE`, `DROP IF EXISTS`, `DROP ROLLBACK` e `CREATE SEQUENCE IF NOT EXISTS` para retornar `long[]` diretamente nos helpers compartilhados e nos testes de provider.
- Tipados os principais call sites de `SelectTestsBase` que ainda usavam `RunTestAsync<...>` implícito para rowsets de janela, `APPLY`, `UNION` e projeções simples.
- Adicionada sobrecarga de compatibilidade em `SqlSelectQuery` para manter os testes de plano de execucao alinhados com a assinatura usada pelos parsers e helpers.
- Adicionado o contrato de fidelidade para `FOR JSON PATH` com `ROOT('users')` na projeção compartilhada de usuarios do SQL Server e do SQL Azure.
- Adicionado o contrato de fidelidade para `OPENJSON` em array JSON compartilhado no SQL Server e no SQL Azure.
- Adicionado o contrato de `DISTINCT ON` no PostgreSQL/Npgsql, com parser/executor e validacao do prefixo esquerdo de `ORDER BY`.
- Adicionada cobertura compartilhada para `@@TEXTSIZE` e `NEWSEQUENTIALID` no SQL Server e no SQL Azure.
- Adicionado suporte a `iso_week` em `DATEPART`, `DATENAME` e `DATETRUNC` no SQL Server, com rejeicao explicita em `DATEADD` e `DATEDIFF` quando a unidade nao faz parte do contrato real.
- Expandido o suporte de `iso_week` para os aliases `isowk` e `isoww` em `DATEPART`, `DATENAME` e `DATETRUNC`, com a mesma rejeicao em `DATEADD` e `DATEDIFF`.
- Adicionado o contrato de `JSON_QUERY` sem path no SQL Server, preservando o fragmento JSON bruto da raiz e a insercao dentro de `FOR JSON PATH`.
- Validado `STRING_AGG` com `WITHIN GROUP (ORDER BY ...)` no SQL Server para manter a ordem explicita de concatenacao.
- Adicionado tratamento de `strict` nos caminhos JSON do SQL Server, com erro quando o caminho nao existe ou nao resolve para o tipo esperado.
- Adicionado tratamento de `strict` em `JSON_MODIFY` no SQL Server, com erro quando o caminho nao existe no documento JSON.
- Adicionado `append` em `JSON_MODIFY` no SQL Server, com append de valor em array JSON existente.
- Adicionado o limite de 4000 caracteres em `JSON_VALUE` do SQL Server, com retorno nulo no modo lax e erro no modo strict.
- Validado o limite exato de 4000 caracteres em `JSON_VALUE` do SQL Server, com retorno do valor no limite e rejeicao acima dele.
- Adicionado tratamento runtime para `NTILE` no SQL Server quando a expressao de buckets avalia para valor nao positivo.
- Adicionado tratamento runtime para `LAG` no SQL Server quando o offset avaliado em runtime fica abaixo de zero.
- Adicionado tratamento runtime para `LEAD` no SQL Server quando o offset avaliado em runtime fica abaixo de zero.
- Adicionado tratamento runtime para `NTH_VALUE` no SQL Server quando o ordinal avaliado em runtime fica abaixo de um.
- Corrigido o frame padrao de janela ordenada no SQL Server para tratar `LAST_VALUE` e `NTH_VALUE` como acumulados por linha.
- Tipados os ultimos wrappers de fidelidade que ainda retornavam `object?` nos testes de `FieldTypeFunctionTestsBase` e `SelectTestsBase`, removendo o ultimo helper de infraestrutura sem tipo explicito nesses fluxos.
- Tipado o contrato de `SelectByPKServiceTest` para `QueryResultSnapshot` e removido o boxing dos wrappers de `json_each`, `json_tree`, `OPENJSON` e JSON tree/array da suite compartilhada.
- Corrigido o frame padrao de janela ordenada no SQL Server para tratar agregados em janela como acumulados por linha.
- Reforcada a cobertura de agregados em janela ordenada no SQL Server com peers duplicados em `ORDER BY`, preservando o frame padrao acumulado por linha.
- Reforcada a diferenca entre `ROWS` e `RANGE` em janela ordenada no SQL Server com valores repetidos em `ORDER BY`.
- Reforcada a cobertura de agregados em janela ordenada no SQL Server com `AVG` e `MAX` acumulados por linha.
- Expandido o suporte temporal compartilhado para `millisecond` em `DATEADD`, `DATEDIFF`, `DATEPART`, `DATENAME` e `DATETRUNC`.
- Expandido o suporte temporal compartilhado para `microsecond` em `DATEADD`, `DATEDIFF`, `DATEPART`, `DATENAME` e `DATETRUNC`.
- Expandido o suporte SQL Server para `dayofyear`, `week` e `weekday` em `DATEADD`, `DATEPART` e `DATENAME`, com `dayofyear` e `week` tambem em `DATEDIFF` e `DATETRUNC`, mantendo `@@DATEFIRST` no valor padrao do provider.
- Expandido o suporte temporal compartilhado para `nanosecond` nas funcoes temporais do SQL Server, com `DATETRUNC` rejeitando a unidade conforme o banco real.
- Reforcados os aliases temporais do SQL Server para `dy`, `y`, `wk`, `ww`, `dw` e `w` em `DATEPART`, `DATENAME`, `DATEADD` e `DATEDIFF`.
- Reforcados os aliases temporais do SQL Server para `dy`, `y`, `wk` e `ww` em `DATETRUNC`.
- Reforcados os aliases temporais do SQL Server para `dw`, `w`, `dy`, `y` e `ww` com cobertura adicional em `DATEPART`, `DATENAME`, `DATEADD` e `DATEDIFF`.
- Adicionado suporte a `tzoffset` e `tz` em `DATEPART` e `DATENAME` no SQL Server, com leitura do offset em minutos a partir de `DateTime`, `DateTimeOffset`, literais com offset e `TODATETIMEOFFSET`.
- Adicionado suporte de parser para `DATEPART(tz, ...)` e `DATENAME(tz, ...)` no SQL Server, com cobertura de sintaxe para literal UTC e offset nativo.
- Expandido o suporte temporal do SQL Server para o alias `tzoffset`, mantendo o mesmo contrato de leitura de offset em minutos em `DATEPART` e `DATENAME`.
- Expandido o suporte equivalente de `tz` e `tzoffset` no SQL Azure, com cobertura de execucao e parser alinhada ao SQL Server.
- Expandido o suporte do SQL Azure para `tzoffset` com literal UTC e `TODATETIMEOFFSET`, mantendo o mesmo contrato de offset em minutos.
- Adicionado cobertura de offset negativo em `TODATETIMEOFFSET` para `tzoffset` no SQL Server e no SQL Azure.

## Próximos passos

- Separar funções por categoria.
- Cobrir tipos nativos de retorno e de parâmetro.
- Continuar reduzindo casts diretos nos wrappers de fidelidade que ainda dependem de forma de runtime.
- Alinhar temporais, JSON e window functions com o banco real.
- Fechar as funcoes matematicas restantes registradas em [Fase-09-Funcoes-Matematicas.md](./Fase-09-Funcoes-Matematicas.md).
