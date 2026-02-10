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
