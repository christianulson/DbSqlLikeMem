# DbSqlLikeMem

In-memory C# database engine for unit tests that emulates SQL dialects and ADO.NET behavior for **MySQL**, **SQL Server**, **Oracle**, **PostgreSQL (Npgsql)**, **SQLite**, and **DB2**.

This project lets you test data access code without a real database by using provider-specific connection mocks and a SQL parser/executor built into the library.

## 📚 Documentação por contexto

Para facilitar manutenção e leitura, a documentação principal foi separada por tema:

- [Visão geral da documentação](docs/README.md)
- [Começando rápido (instalação e uso)](docs/getting-started.md)
- [Provedores, versões e compatibilidade SQL](docs/providers-and-features.md)
- [Publicação (NuGet, VSIX e VS Code)](docs/publishing.md)
- [Guia para Wiki do GitHub](docs/wiki/README.md)

> Dica: use o `README.md` da raiz como porta de entrada e aprofunde nos links acima.

---

## Features (resumo)

- Suporte a 6 provedores: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2
- Mocks específicos por provedor (ADO.NET)
- Parser + executor SQL para DDL/DML comum
- API fluente para schema e seed de dados
- Execução amigável para Dapper
- Diferenças por dialeto/versão

Detalhes completos de compatibilidade:
- [docs/providers-and-features.md](docs/providers-and-features.md)

## Requisitos

- Bibliotecas de provider: .NET Framework 4.8, .NET 6.0 e .NET 8.0.
- Núcleo `DbSqlLikeMem`: .NET Standard 2.0 + .NET Framework 4.8, .NET 6.0 e .NET 8.0.

## Supported Providers

| Provider | Package/Project |
| --- | --- |
| MySQL | `DbSqlLikeMem.MySql` |
| SQL Server | `DbSqlLikeMem.SqlServer` |
| Oracle | `DbSqlLikeMem.Oracle` |
| PostgreSQL | `DbSqlLikeMem.Npgsql` |
| SQLite | `DbSqlLikeMem.Sqlite` |
| DB2 | `DbSqlLikeMem.Db2` |

## Exemplo de factory de provider em runtime

```csharp
using DbSqlLikeMem.Db2;
using DbSqlLikeMem.MySql;
using DbSqlLikeMem.Npgsql;
using DbSqlLikeMem.Oracle;
using DbSqlLikeMem.Sqlite;
using DbSqlLikeMem.SqlServer;

public static class DbSqlLikeMemFactory
{
    public static DbConnection Create(string provider)
    {
        return provider.ToLowerInvariant() switch
        {
            "mysql" => new MySqlConnectionMock(new MySqlDbMock()),
            "sqlserver" => new SqlServerConnectionMock(new SqlServerDbMock()),
            "oracle" => new OracleConnectionMock(new OracleDbMock()),
            "postgres" or "postgresql" or "npgsql" => new NpgsqlConnectionMock(new NpgsqlDbMock()),
            "sqlite" => new SqliteConnectionMock(new SqliteDbMock()),
            "db2" => new Db2ConnectionMock(new Db2DbMock()),
            _ => throw new ArgumentException($"Unsupported provider: {provider}")
        };
    }
}
```

## Instalação e exemplos de uso

Consulte o guia dedicado:

- [docs/getting-started.md](docs/getting-started.md)

Esse guia contém:
- referência de projeto e DLLs
- observações de NuGet/dependências
- factory de provider em runtime
- configuração de `InternalsVisibleTo`
- exemplos com SQL Server e PostgreSQL

## Testes

```bash
dotnet test src/DbSqlLikeMem.slnx
```

## Publicação

A documentação de publicação foi separada em:

- [docs/publishing.md](docs/publishing.md)

Inclui:
- publicação de pacotes no NuGet
- publicação de extensão VSIX (Visual Studio Marketplace)
- publicação de extensão VS Code (Marketplace)

## Contribuição

Contributions are welcome! If you want to help improve DbSqlLikeMem, please open an issue to discuss your idea or submit a pull request.

Áreas com alto impacto:

- Expandir compatibilidade SQL por dialeto
- Adicionar exemplos e documentação
- Melhorar desempenho e diagnósticos
- Aumentar cobertura de testes

## Estrutura de documentação para Wiki

Se quiser publicar uma wiki no GitHub com base no conteúdo local:

- veja o passo a passo em [docs/wiki/README.md](docs/wiki/README.md)
- arquivos prontos para páginas de wiki em [docs/wiki/pages](docs/wiki/pages)
