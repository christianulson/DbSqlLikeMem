# Function registry migration tracker

Este tracker acompanha a migracao do modelo de funcoes do core para `DbFunctionDef` e o fechamento da ponte de compatibilidade.

## Objetivo

- Centralizar o contrato de funcoes em um modelo unico e mais rico.
- Reduzir dependencias entre parser, dialetos, runtime e registries legados.
- Manter dois pontos de entrada distintos: dialeto para funcoes built-in e `SchemaMock` para funcoes/procedures criadas por scripts do usuario.
- Evoluir para um core agnostico ao banco, com parser e query sem depender de regras especificas de provider.
- Carregar as funcoes e procedures de cada banco nos projetos de banco correspondentes.
- Manter o projeto `Auto` em um projeto separado com dialeto proprio que agrega as funcoes dos demais bancos.
- Manter funcoes comuns no projeto principal quando elas forem reutilizaveis por mais de um banco, mas sempre consumidas pelo executor via registro do dialeto.
- Manter o projeto buildavel durante a transicao, sem quebrar provider-specific behavior.

## Status atual

- [x] Novo modelo detalhado de funcao criado em `src/DbSqlLikeMem/Models/DbFunctionDef.cs`.
- [x] Contrato de compatibilidade criado em `src/DbSqlLikeMem/Models/DbFunctionCompatibility.cs`.
- [x] Runtime de create function migrado para `DbFunctionDef`.
- [x] Parser de `CREATE FUNCTION` migrado para construir `DbFunctionDef`.
- [x] AST de `SqlCreateFunctionQuery` passou a carregar `Definition`.
- [x] Helpers de table function e window function foram alinhados ao novo modelo.
- [x] Registries de `SqlServer`, `Npgsql` e `Oracle` foram adaptados aos novos overloads principais.
- [x] Extensoes de registro passaram a aceitar `DbFunctionDef` diretamente.
- [x] Extensoes de registro passaram a aceitar `DbInvocationStyle` diretamente.
- [x] Ponte legada (`DbScalarFunctionDef`/`DbTableFunctionDef`) foi removida do caminho normal.
- [x] Registries de `MySql`, `Db2`, `Sqlite` e `SqlServer` foram migrados para o contrato novo onde ainda havia wrappers.
- [x] `Npgsql` deixou de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] `MySql` e `Oracle` deixaram de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] Registries centrais de `Auto`, `Db2`, `Sqlite` e `SqlServer` passaram a usar `DbInvocationStyle`.
- [x] Objetos e overloads legados foram marcados como `Obsolete` para guiar a migracao.
- [x] Testes de parser afetados por `SqlCreateFunctionQuery` foram ajustados para `Definition`.
- [x] `Auto`, `SqlServer`, `Db2` e `Sqlite` deixaram de depender de `SqlFunctionBodyFactory.Identity()` nos registros principais.
- [x] Remover a ponte `DbScalarFunctionDef` e `DbTableFunctionDef` quando nao houver mais consumidores.
- [x] Lógica interna repetida em `SqlDialectScalarFunctionRegistryExtensions` foi consolidada.
- [x] Consolidar overloads de registro para reduzir duplicacao entre `Func<SqlExpr, object>` e `AstQueryGeneralScalarFunctionHandler`.
- [x] Helper privado morto `AddScalarFunctionsCore` foi removido de `SqlDialectScalarFunctionRegistryExtensions`.
- [x] Helpers privados mortos `CreateScalarFunctionDefinition` e `WithScalarFunctionExecutor` foram removidos de `SqlDialectScalarFunctionRegistryExtensions`.
- [x] API condicional `AddScalarFunctionIf`/`AddScalarFunctionsIf` foi removida dos registries.
- [x] Overloads mortos baseados em `Func<SqlExpr, object>` foram removidos.
- [x] API condicional `AddTableFunctionsIf` e overload por aridade de table function foram removidos.
- [x] Overloads mortos de `FunctionDictionaryProcess` foram removidos.
- [x] Construtor interno morto de `DbFunctionDef` com executor direto foi removido.
- [x] API condicional `AddWindowFunctionsIf` e overload por aridade de window function foram removidos.
- [x] API condicional `AddProcedureIf`/`AddProceduresIf` foi removida dos helpers de procedure do dialeto.
- [x] Helper legado `SqlFunctionBodyFactory` foi removido por nao haver mais consumidores.
- [x] Factories publicas mortas `DbFunctionDef.CreateAggregate(...)` e `DbFunctionDef.CreateWindow(...)` foram removidas.
- [x] Migrar os providers restantes para `DbInvocationStyle` e remover o enum legado quando nao houver mais consumo.
- [x] Revisar `SqlAzure` e confirmar que nao havia registry de funcoes dedicado para migrar.
- [x] Remover DTOs legados de funcao quando a compatibilidade nao for mais necessaria.
- [x] Completar a documentacao XML de novos tipos publicos expostos pela migracao.
- [ ] Separar o contrato de funcoes por projeto de banco, mantendo o core agnostico.
- [ ] Consolidar o dialeto `Auto` em projeto proprio com registro agregado das funcoes dos demais bancos.
- [ ] Revisar parser e query para dependerem apenas do contrato do dialeto, sem conhecimento de provider especifico.
- [ ] Garantir que `SchemaMock` continue sendo o ponto de registro para funcoes e procedures criadas por scripts do usuario.
- [ ] Remover do executor geral do core as funcoes especificas de provider que ja possuem evaluators/registries dedicados.
- [ ] Remover do executor geral do core os helpers de data/hora especificos de provider quando houver registrador dedicado.
- [x] Remover aliases de provider do avaliador geral quando o dialeto ja registrar o helper diretamente.
- [x] Migrar os helpers de data/hora do MySQL para evaluator proprio do provider.
- [x] Remover helpers mortos de data/hora do avaliador geral quando nao houver consumidor real.
- [x] Migrar os helpers de data/hora do SQLite para evaluator proprio do provider.
- [x] Remover `TO_CHAR` do avaliador geral e manter a implementacao nos dialetos que a registram explicitamente.
- [x] Remover `GLOB`, `PATINDEX` e a familia `PRINTF`/`FORMAT`/`SQLITE3_MPRINTF` do avaliador geral.
- [x] Criar evaluator proprio de SQLite para `GLOB` e `PRINTF`/`FORMAT`/`SQLITE3_MPRINTF`.
- [x] Criar evaluator proprio de SQLite para `UNISTR`, `UNISTR_QUOTE`, `LIKELY`, `UNLIKELY` e `LIKELIHOOD`.
- [x] Criar evaluator proprio de SQLite para `RANDOMBLOB`, `ZEROBLOB`, `SQLITE3_RESULT_ZEROBLOB` e `TYPEOF`.
- [x] Mover as funcoes de sistema do SQLite para um evaluator proprio do provider.
- [x] Mover `JSON_EACH`, `JSON_TREE`, `JSONB_EACH`, `JSONB_TREE` e `JSONB_EXTRACT` para um evaluator proprio do SQLite.
- [x] Remover o handler legado de JSON do SQLite do avaliador geral.
- [x] Criar evaluator proprio de MySQL para `JSON_APPEND`, `JSON_ARRAY_INSERT`, `JSON_STORAGE_SIZE` e `JSON_OVERLAPS`.
- [x] Criar evaluator proprio de MySQL para `JSON_ARRAY` e `JSON_DEPTH`.
- [x] Mover `JSON_CONTAINS` e `JSON_OVERLAPS` para o evaluator JSON do MySQL.
- [x] Mover o coletor de busca de `JSON_SEARCH` para o evaluator JSON do MySQL.
- [x] Extrair `TryParseJsonElement` e `BuildJsonArray` para um helper JSON compartilhado.
- [x] Consolidar `CloneJsonNode` e `StripJsonNullProperties` em um helper JSON compartilhado.
- [x] Extrair `TryParseJsonNode` e os helpers de mutacao de JSON para um helper JSON compartilhado.
- [x] Extrair `TryParseJsonPathTokens` e `TryParseSqlServerJsonModifyPath` para um helper JSON path compartilhado.
- [x] Remover `TIME_FORMAT`, `TIME_TO_SEC`, `TIMEDIFF`, `TO_DAYS`, `TO_SECONDS`, `TRUNCATE`, `UNIX_TIMESTAMP`, `WEEK`, `WEEKDAY`, `WEEKOFYEAR` e `YEARWEEK` do avaliador geral e mantê-los em um evaluator proprio de MySQL.
- [x] Remover `LAST_DAY` do avaliador geral e mantê-lo no evaluator proprio de MySQL.
- [x] Remover `EOMONTH` do avaliador geral e mantê-lo no caminho de compatibilidade do SQL Server.
- [x] Remover `DATENAME` e `DATEPART` do temporal accessor geral e mantê-los em um evaluator proprio de SQL Server.
- [x] Remover `UNIXEPOCH` do avaliador geral e mantê-lo no evaluator proprio de SQLite.
- [x] Mover o helper de datas do DB2 para um caminho proprio de DB2 fora da pasta `General`.
- [x] Mover `DATE_ADD` e `DATE_SUB` do avaliador temporal geral para um caminho proprio de MySQL.
- [x] Remover o uso remanescente do evaluator geral nos registries de MySQL, Npgsql e SQL Server depois da extracao dos helpers compartilhados.
- [x] Remover o evaluator geral de data/hora depois da extracao dos helpers de MySQL e SQLite.
- [x] Remover os helpers MySQL `JSON_APPEND` e `JSON_ARRAY_INSERT` do evaluator SQLite.
- [x] Remover `SESSION_CONTEXT` do avaliador geral e mantê-lo no registrador do SQL Server.
- [x] Remover `GETANSINULL`, `HOST_ID`, `HOST_NAME`, `ISDATE`, `ISJSON` e `ISNUMERIC` do avaliador geral e mantê-los no utility evaluator do SQL Server.
- [x] Remover `SESSION_USER` e `SYSTEM_USER` do avaliador geral e mantê-los no utility evaluator do SQL Server.
- [x] Remover `UUID_SHORT` do avaliador geral e mantê-lo no evaluator proprio de MySQL.
- [x] Remover `IS_UUID` e os helpers `IS_IPV4*` do avaliador geral e mantê-los no utility evaluator de MySQL.
- [x] Remover `USER` do avaliador geral e mantê-lo nos evaluators específicos dos dialetos que o registram.
- [x] Registrar `PATINDEX` pelo evaluator proprio de SQL Server.
- [x] Remover `NAME_CONST` do avaliador geral e mantê-lo no evaluator proprio de MySQL.
- [x] Remover `FIELD` do avaliador geral e mantê-lo no helper compartilhado MariaDB/MySQL.
- [x] Remover `UTC_DATE`, `UTC_TIME` e `UTC_TIMESTAMP` do avaliador geral e mantê-los no evaluator proprio de MySQL.
- [x] Remover `GROUPING` e `GROUPING_ID` do avaliador geral e mantê-los em um evaluator compartilhado proprio.
- [x] Remover `JSON_MODIFY` e `OPENJSON` do avaliador geral e mantê-los no utility evaluator do SQL Server.
- [x] Remover `SUBDATE` do avaliador geral e mantê-lo no evaluator proprio de data do MySQL.
- [x] Extrair `JSON_UNQUOTE` para um helper compartilhado explicito.
- [x] Remover `DIFFERENCE` do avaliador geral e mantê-lo no utility evaluator do SQL Server.
- [x] Remover `SOUNDEX` do avaliador geral e mantê-lo no utility evaluator do SQL Server.
- [x] Remover `QUOTE` do avaliador geral e mantê-lo no utility evaluator do MySQL.
- [x] Remover `SUBSTRING_INDEX` do avaliador geral e mantê-lo no utility evaluator do MySQL.
- [x] Remover `HEX` e `UNHEX` do avaliador geral e mantê-los no utility evaluator do MySQL.
- [x] Remover `OCT` e `ORD` do avaliador geral e mantê-los no utility evaluator do MySQL.
- [x] Remover `RANDOM` do avaliador geral e mantê-lo no registrador de compatibilidade do PostgreSQL.
- [x] Remover `BIT_COUNT` do avaliador geral e mantê-lo no utility evaluator do MySQL.
- [x] Remover `LOG2` do avaliador geral e mantê-lo no utility evaluator do MySQL.
- [x] Remover `LEN`, `LTRIM`, `REVERSE` e `RTRIM` do registrador de compatibilidade SQL Server/Auto e mantê-los no evaluator compartilhado de texto.
- [x] Remover o wrapper `AstQueryGeneralScalarFunctionEvaluator` e mover o delegate para um contrato proprio.
- [x] Remover `SHA`, `SHA1` e `SHA2` do avaliador geral e mantê-los no utility evaluator do MySQL.
- [x] Remover `HEX`, `UNHEX` e `MD5` do avaliador geral e mantê-los no evaluator compartilhado de texto/hash.
- [x] Remover `ASCII`, `UNICODE` e `SPACE` do avaliador geral e mantê-los no evaluator compartilhado de texto.
- [x] Remover `INSTR`, `LPAD`, `REPLACE` e `REVERSE` do avaliador geral e mantê-los no evaluator compartilhado de texto.
- [x] Remover `REPEAT` do avaliador geral e mantê-lo no evaluator compartilhado de texto.
- [x] Remover `LEFT`, `RIGHT`, `RPAD`, `BIT_LENGTH`, `OCTET_LENGTH`, `POSITION` e `MOD` do avaliador geral e mantê-los no conjunto compartilhado de texto/numerico.
- [x] Remover `ABS`, `ABSVAL` e `BIN` do avaliador geral e mantê-los no evaluator compartilhado numerico.
- [x] Remover `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `CEIL`, `CEILING`, `COS`, `COT`, `LN`, `LOG` e `LOG10` do avaliador geral e mantê-los no evaluator compartilhado numerico.
- [x] Remover `CHAR` e `NCHAR` do avaliador geral e mantê-los no evaluator compartilhado de texto.
- [x] Remover `LOWER`, `LCASE`, `UPPER`, `UCASE`, `TRIM`, `RTRIM`, `LTRIM`, `LENGTH`, `CHAR_LENGTH`, `CHARACTER_LENGTH` e `LEN` do avaliador geral e mantê-los no evaluator compartilhado de texto.
- [x] Remover `SUBSTRING`, `SUBSTR` e `MID` do avaliador geral e mantê-los no evaluator compartilhado de texto.
- [x] Remover `LOCATE` do avaliador geral e mantê-lo no evaluator compartilhado de texto.
- [x] Remover `TRANSLATE` do avaliador geral e mantê-lo no evaluator compartilhado de texto.
- [x] Remover `LIKE` do avaliador geral e mantê-lo no evaluator compartilhado de texto.
- [x] Remover `GREATEST` e `LEAST` do avaliador geral e mantê-los no evaluator compartilhado numérico.
- [x] Mover os formatadores `FORMATMESSAGE`/`PRINTF` para o helper compartilhado de formatação.
- [x] Mover os helpers de rede e escala numérica do PostgreSQL para os evaluators do próprio provider.
- [x] Mover `TO_ASCII` e os helpers PostgreSQL de `inet`/identificador para evaluators do próprio provider.
- [x] Remover `DEGREES`, `EXP`, `FLOOR`, `PI`, `POWER`, `POW`, `RADIANS`, `RAND`, `ROUND`, `SIGN`, `SIN`, `SQRT` e `TAN` do avaliador geral e mantê-los no evaluator compartilhado numérico.
- [x] Remover `JSON_QUOTE` do avaliador geral e mantê-lo no helper de JSON do MySQL.
- [x] Remover `JSON_PRETTY` do avaliador geral e mantê-lo no helper de JSON do MySQL/MariaDB.
- [x] Remover os helpers MySQL de mutacao e busca JSON do avaliador geral e mantê-los no helper de JSON do MySQL.
- [x] Remover `JSON_VALID`, `JSON_TYPE` e `JSON_LENGTH` do avaliador geral e mantê-los no helper de JSON do MySQL.
- [x] Extrair `JSON_OBJECT` para um helper compartilhado e registrar MySQL, Npgsql e Auto nele.
- [x] Extrair `JSON_EXTRACT`/`JSON_QUERY`/`JSON_VALUE` para um helper compartilhado explicito.
- [x] Remover `STRCMP` do registrador compartilhado para mantê-lo no helper MySQL específico.
- [x] Remover o wrapper JSON utilitario morto do avaliador geral depois da extração de `JSON_OBJECT`.
- [x] Extrair `TO_NUMBER` para um helper compartilhado do cast flow e remover a versão geral.
- [x] Extrair `JSON_ARRAY` para um helper compartilhado e remover os fallbacks no-op de MySQL/Npgsql.
- [x] Remover o wrapper MySQL/Npgsql geral de `JSON_ARRAY` e registrar diretamente o helper compartilhado.
- [x] Remover o fallback SQLite hardcoded do executor geral e manter apenas o helper compartilhado de `JSON_OBJECT`.

## Fases

### Fase 1 - Estrutura do contrato

- [x] Criar `DbFunctionDef` com capacidades, assinaturas e estilo de invocacao.
- [x] Criar `DbFunctionParameterDef` e `DbFunctionSignature`.
- [x] Incluir suporte para funcoes escalares, de janela, de tabela e temporais.

### Fase 2 - Compatibilidade temporaria

- [x] Criar ponte para os registries existentes.
- [x] Manter runtime e parser funcionando enquanto a transicao acontece.
- [x] Encerrar a ponte depois da migração dos providers.

### Fase 3 - Parser e AST

- [x] Migrar `SqlCreateFunctionQuery` para armazenar `Definition`.
- [x] Migrar o parser de parametros de funcao para `DbFunctionParameterDef`.
- [x] Migrar o parser de corpo de funcao para `DbFunctionDef.CreateUserDefined(...)`.
- [x] Atualizar testes de parser que dependiam dos campos antigos.

### Fase 4 - Providers

- [x] Adaptar `SqlServer` aos overloads novos.
- [x] Adaptar `Npgsql` aos overloads novos.
- [x] Adaptar `Oracle` aos overloads novos.
- [x] Revisar `MySql`, `Db2` e `Sqlite` para reduzir ainda mais o uso de wrappers temporarios.
- [x] Revisar `SqlAzure` e qualquer provider restante para reduzir ainda mais o uso de wrappers temporarios.
- [x] Validar se existe alguma funcao especial que ainda merece registrador dedicado por provider.

### Fase 5 - Cleanup

- [x] Remover arquivos e tipos legados apos a eliminacao dos ultimos consumidores.
- [ ] Revisar warnings de nullability e XML docs nos novos tipos publicos.
- [x] Consolidar o backlog em um documento final quando a migracao estiver fechada.

### Fase 6 - Separacao por projeto

- [ ] Manter no core apenas as funcoes realmente comuns a todos os bancos.
- [ ] Extrair funcoes e procedures built-in para os projetos de banco correspondentes.
- [ ] Manter o core parser/query agnostico ao banco.
- [ ] Criar ou consolidar o projeto `Auto` com dialeto proprio agregando os demais bancos.
- [ ] Garantir que funcoes/procedures criadas por scripts permaneçam centralizadas em `SchemaMock`.

## Proximo passo recomendado

1. Definir o mapa de responsabilidades entre core, dialetos de banco, `SchemaMock` e `Auto`.
2. Consolidar os overloads restantes em `SqlDialectScalarFunctionRegistryExtensions` apenas se ainda fizerem parte do core comum.
3. Revisar warnings de nullability e XML docs nos novos tipos publicos.

## Notas

- Este tracker acompanha apenas a migracao das funcoes.
- O backlog funcional principal continua em `docs/features-backlog/index.md`.
- O status operacional de curto prazo continua em `docs/features-backlog/status-operational.md`.
