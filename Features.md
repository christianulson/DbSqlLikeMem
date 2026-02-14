# Features por banco e versão

> Este arquivo foi mantido por compatibilidade. A versão canônica e organizada deste conteúdo está em [`docs/providers-and-features.md`](docs/providers-and-features.md).

## Links rápidos

- [Matriz de provedores e versões](docs/providers-and-features.md#visão-geral)
- [Capacidades por dialeto](docs/providers-and-features.md#particularidades-por-banco)
- [Regras candidatas para evolução do parser](docs/providers-and-features.md#regras-candidatas-para-extrair-do-parser-para-os-dialects)

## Matriz de provedores e versões simuladas

| Banco | Projeto | Versões simuladas |
| --- | --- | --- |
| MySQL | `DbSqlLikeMem.MySql` | 3, 4, 5, 8 |
| SQL Server | `DbSqlLikeMem.SqlServer` | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| Oracle | `DbSqlLikeMem.Oracle` | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite | `DbSqlLikeMem.Sqlite` | 3 |
| DB2 | `DbSqlLikeMem.Db2` | 8, 9, 10, 11 |

## Funcionalidades por banco

### MySQL
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- `INSERT ... ON DUPLICATE KEY UPDATE`: suportado.

### SQL Server
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- Diferenças de dialeto por versão suportadas pelo provider.

### Oracle
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- Diferenças de dialeto por versão suportadas pelo provider.

### PostgreSQL (Npgsql)
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- Diferenças de dialeto por versão suportadas pelo provider.

### SQLite
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- `WITH`/CTE: disponível (>= 3).
- `ON DUPLICATE KEY UPDATE`: não suportado (SQLite usa `ON CONFLICT`).
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: suportados pelo parser do dialeto.

### DB2
- Mock de conexão/ADO.NET específico do provedor.
- Parser e execução de SQL para DDL/DML comuns.
- `WITH`/CTE: disponível (>= 8).
- `MERGE`: disponível (>= 9).
- `FETCH FIRST`: suportado.
- `LIMIT/OFFSET`: não suportado pelo dialeto DB2.
- `ON DUPLICATE KEY UPDATE`: não suportado.
- Operador null-safe `<=>`: não suportado.
- Operadores JSON `->` e `->>`: não suportados.


- Triggers em tabelas não temporárias: suportadas via `TableMock` (before/after insert, update e delete).
- Tabelas temporárias (connection/global): triggers não são executadas.

## Extensões (VS Code e Visual Studio)

As extensões agora suportam, além da geração tradicional para testes, fluxos separados para artefatos de aplicação:

- **Gerar classes de teste** (ação principal existente, com foco em classes de teste).
- **Gerar classes de modelos** (novo).
- **Gerar classes de repositório** (novo).
- **Configurar templates** via botão no topo para arquivos texto com tokens.
- **Check de consistência com status visual** para indicar ausência/divergência/sincronização dos artefatos esperados.

### Tokens de template

- `{{ClassName}}`, `{{ObjectName}}`, `{{Schema}}`, `{{ObjectType}}`, `{{DatabaseType}}`, `{{DatabaseName}}`.

> Observação: esses recursos são voltados a gerar arquivos para uso em projetos reais do usuário (não apenas arquivos de teste).
