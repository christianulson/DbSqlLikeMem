# Features por banco e versão

> Treeview de compatibilidade por banco. As versões abaixo representam as versões simuladas aceitas pelo mock e pelo parser de dialetos.

- **MySQL**
  - **Versões simuladas**: 3, 4, 5, 8.
  - **Funcionalidades suportadas (independente da versão)**
    - Mock de conexão/ADO.NET específico do provedor.
    - Parser e execução de SQL para DDL/DML comuns.
    - Dialeto com diferenças por banco (regras de parser e compatibilidade).
    - Expressões `WHERE` (AND/OR, `IN`, `LIKE`, `IS NULL`, parâmetros).
    - `CREATE VIEW`/`CREATE OR REPLACE VIEW`.
    - `CREATE TEMPORARY TABLE` (incluindo variantes com `AS SELECT`).
    - Estratégia `INSERT ... ON DUPLICATE KEY UPDATE`.
    - Definição de schema via API fluente.
    - Seed de dados e execução de consultas com mocks compatíveis com Dapper.

- **SQL Server**
  - **Versões simuladas**: 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022.
  - **Funcionalidades suportadas (independente da versão)**
    - Mock de conexão/ADO.NET específico do provedor.
    - Parser e execução de SQL para DDL/DML comuns.
    - Dialeto com diferenças por banco (regras de parser e compatibilidade).
    - Expressões `WHERE` (AND/OR, `IN`, `LIKE`, `IS NULL`, parâmetros).
    - `CREATE VIEW`/`CREATE OR REPLACE VIEW`.
    - `CREATE TEMPORARY TABLE` (incluindo variantes com `AS SELECT`).
    - Definição de schema via API fluente.
    - Seed de dados e execução de consultas com mocks compatíveis com Dapper.

- **Oracle**
  - **Versões simuladas**: 7, 8, 9, 10, 11, 12, 18, 19, 21, 23.
  - **Funcionalidades suportadas (independente da versão)**
    - Mock de conexão/ADO.NET específico do provedor.
    - Parser e execução de SQL para DDL/DML comuns.
    - Dialeto com diferenças por banco (regras de parser e compatibilidade).
    - Expressões `WHERE` (AND/OR, `IN`, `LIKE`, `IS NULL`, parâmetros).
    - `CREATE VIEW`/`CREATE OR REPLACE VIEW`.
    - `CREATE TEMPORARY TABLE` (incluindo variantes com `AS SELECT`).
    - Definição de schema via API fluente.
    - Seed de dados e execução de consultas com mocks compatíveis com Dapper.

- **PostgreSQL (Npgsql)**
  - **Versões simuladas**: 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17.
  - **Funcionalidades suportadas (independente da versão)**
    - Mock de conexão/ADO.NET específico do provedor.
    - Parser e execução de SQL para DDL/DML comuns.
    - Dialeto com diferenças por banco (regras de parser e compatibilidade).
    - Expressões `WHERE` (AND/OR, `IN`, `LIKE`, `IS NULL`, parâmetros).
    - `CREATE VIEW`/`CREATE OR REPLACE VIEW`.
    - `CREATE TEMPORARY TABLE` (incluindo variantes com `AS SELECT`).
    - Definição de schema via API fluente.
    - Seed de dados e execução de consultas com mocks compatíveis com Dapper.


- **SQLite**
  - **Versões simuladas**: 3.
  - **Funcionalidades por versão**
    - `WITH`/CTE: disponível (>= 3).
    - `ON DUPLICATE KEY UPDATE`: não suportado (SQLite usa `ON CONFLICT`).
    - Operador null-safe `<=>`: não suportado.
    - Operadores JSON `->` e `->>`: suportados pelo parser do dialeto.

- **DB2**
  - **Versões simuladas**: 8, 9, 10, 11.
  - **Funcionalidades por versão**
    - `WITH`/CTE: disponível (>= 8).
    - `MERGE`: disponível (>= 9).
    - `FETCH FIRST`: suportado.
    - `LIMIT/OFFSET`: não suportado pelo dialeto DB2.
    - `ON DUPLICATE KEY UPDATE`: não suportado.
    - Operador null-safe `<=>`: não suportado.
    - Operadores JSON `->` e `->>`: não suportados.

## Regras candidatas para extrair do parser para os Dialects

Para deixar o parser mais fiel por banco/versão, estas regras costumam dar um bom ganho quando saem de `if` no parser e passam a ser capacidade do dialeto:

- **CTE recursiva e sintaxe de materialização**
  - Flags separadas para `WITH RECURSIVE`, `MATERIALIZED` e `NOT MATERIALIZED` (PostgreSQL/SQLite recentes).
- **UPSERT por dialeto**
  - Distinguir `ON DUPLICATE KEY UPDATE` (MySQL), `ON CONFLICT ... DO UPDATE/NOTHING` (PostgreSQL/SQLite) e `MERGE` (SQL Server/Oracle/DB2, por versão).
- **Semântica de paginação por versão**
  - Diferenciar `LIMIT ... OFFSET`, `OFFSET ... FETCH`, `FETCH FIRST ... ROWS ONLY`, e regras de obrigatoriedade de `ORDER BY` (SQL Server 2012+ em `OFFSET/FETCH`).
- **Hints de tabela/query**
  - Controle por dialeto para `WITH (NOLOCK)`, `OPTION(...)`, `/*+ hint */`, `STRAIGHT_JOIN`, etc.
- **`RETURNING` / `OUTPUT` / `RETURNING INTO`**
  - Tratar como famílias diferentes por banco: `RETURNING` (PostgreSQL), `OUTPUT` (SQL Server), `RETURNING INTO` (Oracle).
- **Tipos e literais específicos**
  - Regras para cast (`::` vs `CAST`), literais binários/hex, booleanos (`TRUE/FALSE` vs `1/0`) e notação de intervalo/data por banco.
- **`DELETE`/`UPDATE` multi-tabela**
  - Capacidades distintas para `DELETE t FROM ...`, `USING`, `UPDATE ... FROM`, alias do alvo e limitações por versão.
- **JSON e operadores especializados**
  - Separar suporte a `->`, `->>`, `#>`, `#>>`, `JSON_EXTRACT`, `JSON_VALUE`, `OPENJSON` e variantes de caminho JSON por banco.
- **Conflitos de palavras reservadas por versão**
  - Lista de keywords versionada no dialeto (evita falso positivo de parser ao evoluir versões).
- **Colação, `NULLS FIRST/LAST` e ordenação**
  - Regras de ordenação compatíveis por dialeto/versão, inclusive defaults e sintaxe permitida.

### Heurística prática

Uma boa regra de desenho é: se a diferença altera **validade sintática** ou **interpretação semântica**, ela deve viver no dialeto (idealmente com flag/version gate), e o parser apenas consome essas capacidades.
