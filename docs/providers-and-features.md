# Provedores, versões e compatibilidade SQL

> Este arquivo centraliza a matriz de compatibilidade por banco e as capacidades mais relevantes do parser/executor.

## Visão geral

| Banco | Projeto | Versões simuladas |
| --- | --- | --- |
| MySQL | `DbSqlLikeMem.MySql` | 3, 4, 5, 8 |
| SQL Server | `DbSqlLikeMem.SqlServer` | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| Oracle | `DbSqlLikeMem.Oracle` | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite | `DbSqlLikeMem.Sqlite` | 3 |
| DB2 | `DbSqlLikeMem.Db2` | 8, 9, 10, 11 |

## Capacidades comuns (MySQL / SQL Server / Oracle / PostgreSQL)

- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- Dialeto com diferenças por banco (parser e compatibilidade).
- Expressões `WHERE` (`AND`/`OR`, `IN`, `LIKE`, `IS NULL`, parâmetros).
- `CREATE VIEW` / `CREATE OR REPLACE VIEW`.
- `CREATE TEMPORARY TABLE` (incluindo variantes `AS SELECT`).
- Definição de schema via API fluente.
- Seed de dados e consultas compatíveis com Dapper.

## Particularidades por banco

### MySQL
- `INSERT ... ON DUPLICATE KEY UPDATE`: suportado.

### SQLite
- `WITH`/CTE: disponível (>= 3).
- `ON DUPLICATE KEY UPDATE`: não suportado (SQLite usa `ON CONFLICT`).
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: suportados pelo parser do dialeto.

### DB2
- `WITH`/CTE: disponível (>= 8).
- `MERGE`: disponível (>= 9).
- `FETCH FIRST`: suportado.
- `LIMIT/OFFSET`: não suportado pelo dialeto DB2.
- `ON DUPLICATE KEY UPDATE`: não suportado.
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: não suportados.

## Regras candidatas para extrair do parser para os Dialects

Para deixar o parser mais fiel por banco/versão, estas regras costumam dar bom ganho quando saem de `if` no parser e passam a ser capacidade do dialeto:

- **CTE recursiva e sintaxe de materialização**
  - Flags separadas para `WITH RECURSIVE`, `MATERIALIZED` e `NOT MATERIALIZED`.
- **UPSERT por dialeto**
  - Distinguir `ON DUPLICATE KEY UPDATE` (MySQL), `ON CONFLICT` (PostgreSQL/SQLite) e `MERGE` (SQL Server/Oracle/DB2, por versão).
- **Semântica de paginação por versão**
  - Diferenciar `LIMIT ... OFFSET`, `OFFSET ... FETCH`, `FETCH FIRST ... ROWS ONLY`.
- **Hints de tabela/query**
  - Controle por dialeto para `WITH (NOLOCK)`, `OPTION(...)`, `/*+ hint */`, `STRAIGHT_JOIN`.
- **`RETURNING` / `OUTPUT` / `RETURNING INTO`**
  - Tratar famílias distintas por banco.
- **Tipos e literais específicos**
  - Regras para cast, literais binários/hex e booleanos.
- **`DELETE`/`UPDATE` multi-tabela**
  - Capacidades distintas por banco/versão.
- **JSON e operadores especializados**
  - Separar suporte a `->`, `->>`, `#>`, `#>>`, `JSON_EXTRACT`, `JSON_VALUE`, `OPENJSON`.
- **Conflitos de palavras reservadas por versão**
  - Lista de keywords versionada no dialeto.
- **Colação, `NULLS FIRST/LAST` e ordenação**
  - Regras por dialeto/versão.

### Heurística prática

Se a diferença altera **validade sintática** ou **interpretação semântica**, ela deve viver no dialeto (idealmente com flag/version gate), e o parser apenas consome essas capacidades.

## Links relacionados

- [Começando rápido](getting-started.md)
- [Publicação](publishing.md)
- [Matriz SQL (feature x dialeto)](sql-compatibility-matrix.md)
- [Checklist de known gaps](known-gaps-checklist.md)
- [Wiki do GitHub](wiki/README.md)
