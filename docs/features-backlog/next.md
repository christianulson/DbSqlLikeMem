1. Objetivo do projeto

Criar um engine SQL em memória para testes unitários .NET que:

emule dialetos SQL reais

emule comportamento ADO.NET providers

execute queries SQL para validar lógica

rode rápido sem infraestrutura

ADO.NET fornece um modelo padrão onde data providers implementam interfaces como DbConnection e DbCommand para acessar diferentes bancos.

O projeto deve simular essas implementações.

2. Famílias de dialeto SQL

Em vez de implementar bancos isolados, o projeto deve organizar dialetos por família SQL.

Isso reduz drasticamente a complexidade.

Famílias principais
Família	Bancos
SQL Server	SQL Server, Azure SQL
MySQL	MySQL, MariaDB
PostgreSQL	PostgreSQL, DuckDB
Oracle	Oracle
IBM	DB2, Informix
Firebird	Firebird
Analytics SQL	ClickHouse, Snowflake

Hoje o projeto cobre:

SQL Server

MySQL

PostgreSQL

Oracle

SQLite

IBM DB2

Ou seja: quase todas as famílias já estão representadas.

3. Ordem recomendada de novos bancos

Implementar por impacto real em projetos .NET.

Fase 1 — expansão natural
1️⃣ MariaDB

Família:

MySQL family

Motivo:

fork direto do MySQL

pequenas diferenças de SQL

fácil reutilizar parser existente

Diferenças comuns:

RETURNING
SEQUENCE
JSON_TABLE

Implementação:

DbSqlLikeMem.MariaDb
inherits MySqlDialect
2️⃣ Firebird

Família:

Firebird family

Provider ADO.NET:

FirebirdSql.Data.FirebirdClient

Diferenças importantes:

SELECT FIRST 10
ROWS 10
GENERATOR

Esse dialeto é realmente diferente, então é ótimo para validar compatibilidade.

3️⃣ DuckDB

Família:

PostgreSQL family

Motivo:

crescente em analytics

SQL muito próximo do PostgreSQL

Implementação simples:

DuckDbDialect : PostgresDialect
Fase 2 — analytics SQL
4️⃣ ClickHouse

Família:

Analytics SQL

Diferenças relevantes:

ARRAY JOIN
LIMIT BY
ENGINE MergeTree
5️⃣ Snowflake

Família:

Analytics SQL

SQL relativamente padrão.

4. Estrutura sugerida para dialetos

Criar base comum:

SqlDialect

Implementações:

SqlServerDialect
MySqlDialect
PostgresDialect
OracleDialect
SqliteDialect
Db2Dialect

Novos:

MariaDbDialect
FirebirdDialect
DuckDbDialect
ClickHouseDialect
5. Implementação do SqlDialect.Auto

Objetivo:

Permitir que o parser aceite múltiplas sintaxes SQL simultaneamente.

Exemplo:

SELECT TOP 10
SELECT ... LIMIT 10
SELECT ... FETCH FIRST 10 ROWS

Todos devem funcionar.

Estratégia de implementação
Passo 1 — tokenização neutra

Parser deve reconhecer tokens genéricos:

TOP
LIMIT
FETCH
ROWNUM
Passo 2 — normalização

Converter para forma interna única.

Exemplo:

SELECT TOP 10 * FROM table

normaliza para

SELECT * FROM table LIMIT_INTERNAL 10
Passo 3 — executor comum

Executor usa apenas:

LIMIT_INTERNAL
OFFSET_INTERNAL
Exemplo de pipeline
SQL Text
   ↓
Tokenizer
   ↓
AST
   ↓
Dialect Adapter
   ↓
Normalized AST
   ↓
Executor
Pseudocódigo
if (options.SqlDialect == SqlDialect.Auto)
{
    DetectLimitSyntax(ast);
    DetectIdentitySyntax(ast);
    DetectConcatSyntax(ast);
}
Detecção automática

Heurísticas:

TOP → SQL Server
LIMIT → MySQL/Postgres
ROWNUM → Oracle
FETCH FIRST → ANSI
6. Feature #1 — Query Plan Debugger

Extremamente útil para testes.

Exemplo:

db.GetLastExecutionPlan()

Output:

SCAN users
FILTER age > 18
LIMIT 10

Isso ajuda a entender como o SQL foi interpretado.

7. Feature #2 — SQL compatibility validator

Algo tipo:

SqlCompatibilityCheck

Exemplo:

SELECT TOP 10 * FROM users

Resultado:

Compatible:
- SQL Server

Not compatible:
- MySQL
- PostgreSQL

Isso ajudaria projetos multi-database.

8. Feature #3 — Schema snapshot / replay

Permitir capturar schema real e rodar testes.

Exemplo:

DbSqlLikeMem.Schema.Export(connection)

gera:

schema.json

Depois:

DbSqlLikeMem.Schema.Load(schema.json)

Permite reproduzir schema real.

9. Feature bônus (que faria o projeto crescer muito)
Dialect fuzz testing

Executar a mesma query em vários dialetos.

Exemplo:

db.TestAcrossDialects(query)

Output:

SQL Server: OK
Postgres: OK
MySQL: FAIL (LIMIT syntax)
Oracle: FAIL

Isso é extremamente útil para libs multi-DB.

10. Arquitetura ideal futura
DbSqlLikeMem
 ├─ Core
 │   ├─ SQL Parser
 │   ├─ AST
 │   ├─ Execution Engine
 │
 ├─ Dialects
 │   ├─ SqlServer
 │   ├─ MySql
 │   ├─ Postgres
 │   ├─ Oracle
 │   ├─ Sqlite
 │   ├─ Db2
 │   ├─ MariaDb
 │   ├─ Firebird
 │   ├─ DuckDb
 │
 ├─ Features
 │   ├─ SqlDialect.Auto
 │   ├─ CompatibilityAnalyzer
 │   ├─ QueryPlanDebugger
 │   ├─ SchemaSnapshot
11. Roadmap resumido

Ordem recomendada:

1 MariaDB
2 Firebird
3 DuckDB
4 ClickHouse
5 Snowflake

Depois focar em features:

SqlDialect.Auto
QueryPlanDebugger
CompatibilityAnalyzer
SchemaSnapshot

💡 Opinião direta:

Se você implementar MariaDB + Firebird + SqlDialect.Auto, o projeto já vira algo bem raro no ecossistema .NET.

Praticamente um H2 / TestContainers killer para SQL tests.