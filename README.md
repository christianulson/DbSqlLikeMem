# DbSqlLikeMem

**EN:** In-memory C# database engine for unit tests that emulates SQL dialects and ADO.NET behavior for **MySQL**, **SQL Server**, **Oracle**, **PostgreSQL (Npgsql)**, **SQLite**, and **DB2**.

**PT-BR:** Mecanismo de banco de dados em memória para testes unitários em C# que emula dialetos SQL e o comportamento de ADO.NET para **MySQL**, **SQL Server**, **Oracle**, **PostgreSQL (Npgsql)**, **SQLite** e **DB2**.

---

## 📚 Documentation by context | Documentação por contexto

**EN:** To keep maintenance and reading easier, the main documentation is split by topic:

**PT-BR:** Para facilitar a manutenção e a leitura, a documentação principal foi separada por tema:

- [Documentation overview | Visão geral da documentação](docs/README.md)
- [Getting started (installation and usage) | Começando rápido (instalação e uso)](docs/getting-started.md)
- [Providers, versions, and SQL compatibility | Provedores, versões e compatibilidade SQL](docs/providers-and-features.md)
- [AI playbook for external repository/integration tests | Playbook de IA para testes de repositório/integração](docs/ai-nuget-test-projects-playbook.md)
- [Publishing (NuGet, VSIX, and VS Code) | Publicação (NuGet, VSIX e VS Code)](docs/publishing.md)
- [GitHub Wiki guide | Guia para Wiki do GitHub](docs/wiki/README.md)

> **EN:** Use this root `README.md` as your entry point and go deeper through the links above.  
> **PT-BR:** Use este `README.md` da raiz como porta de entrada e aprofunde pelos links acima.

---

## Features (summary) | Funcionalidades (resumo)

- **EN:** Support for 6 providers: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite, and DB2.  
  **PT-BR:** Suporte a 6 provedores: MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite e DB2.
- **EN:** Provider-specific ADO.NET mocks.  
  **PT-BR:** Mocks ADO.NET específicos por provedor.
- **EN:** SQL parser + executor for common DDL/DML operations.  
  **PT-BR:** Parser + executor SQL para operações DDL/DML comuns.
- **EN:** Fluent API for schema definition and data seeding.  
  **PT-BR:** API fluente para definição de schema e seed de dados.
- **EN:** Friendly execution flow for Dapper-based tests.  
  **PT-BR:** Fluxo de execução amigável para testes com Dapper.
- **EN:** Dialect/version-specific behavior.  
  **PT-BR:** Comportamento específico por dialeto/versão.

**EN:** Full compatibility details are available here:  
**PT-BR:** Os detalhes completos de compatibilidade estão aqui:

- [docs/providers-and-features.md](docs/providers-and-features.md)

## Requirements | Requisitos

- **EN:** Provider libraries target .NET Framework 4.8, .NET 6.0, and .NET 8.0.  
  **PT-BR:** As bibliotecas de provedores têm como alvo .NET Framework 4.8, .NET 6.0 e .NET 8.0.
- **EN:** Core `DbSqlLikeMem` targets .NET Standard 2.0 plus .NET Framework 4.8, .NET 6.0, and .NET 8.0.  
  **PT-BR:** O núcleo `DbSqlLikeMem` tem como alvo .NET Standard 2.0 mais .NET Framework 4.8, .NET 6.0 e .NET 8.0.

## Supported Providers | Provedores suportados

| Provider / Provedor | Package/Project / Pacote/Projeto |
| --- | --- |
| MySQL | `DbSqlLikeMem.MySql` |
| SQL Server | `DbSqlLikeMem.SqlServer` |
| Oracle | `DbSqlLikeMem.Oracle` |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` |
| SQLite (Sqlite) | `DbSqlLikeMem.Sqlite` |
| DB2 | `DbSqlLikeMem.Db2` |

## Simulated versions by provider | Versões simuladas por provedor

| Provider / Provedor | Simulated versions / Versões simuladas |
| --- | --- |
| MySQL | 3, 4, 5, 8 |
| SQL Server | 7, 2000, 2005, 2008, 2012, 2014, 2016, 2017, 2019, 2022 |
| Oracle | 7, 8, 9, 10, 11, 12, 18, 19, 21, 23 |
| PostgreSQL (Npgsql) | 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 |
| SQLite (Sqlite) | 3 |
| DB2 | 8, 9, 10, 11 |

## Runtime provider factory example | Exemplo de factory de provider em runtime

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

## Installation and usage examples | Instalação e exemplos de uso

**EN:** See the dedicated getting-started guide:  
**PT-BR:** Consulte o guia dedicado de início rápido:

- [docs/getting-started.md](docs/getting-started.md)

**EN:** The guide includes:  
**PT-BR:** O guia inclui:

- **EN:** Project references and DLL usage.  
  **PT-BR:** Referência de projeto e uso de DLLs.
- **EN:** NuGet/dependency notes.  
  **PT-BR:** Observações de NuGet/dependências.
- **EN:** Runtime provider factory.  
  **PT-BR:** Factory de provider em runtime.
- **EN:** `InternalsVisibleTo` configuration.  
  **PT-BR:** Configuração de `InternalsVisibleTo`.
- **EN:** SQL Server and PostgreSQL examples.  
  **PT-BR:** Exemplos com SQL Server e PostgreSQL.

## Tests | Testes

```bash
dotnet test src/DbSqlLikeMem.slnx
```

## Publishing | Publicação

**EN:** Publishing documentation is available at:  
**PT-BR:** A documentação de publicação está em:

- [docs/publishing.md](docs/publishing.md)

**EN:** It includes:  
**PT-BR:** Ela inclui:

- **EN:** NuGet package publishing.  
  **PT-BR:** Publicação de pacotes no NuGet.
- **EN:** VSIX extension publishing (Visual Studio Marketplace).  
  **PT-BR:** Publicação de extensão VSIX (Visual Studio Marketplace).
- **EN:** VS Code extension publishing (Marketplace).  
  **PT-BR:** Publicação de extensão VS Code (Marketplace).

## Documentation standard (English + Portuguese) | Padrão de documentação (inglês + português)

**EN:** For open-source readability, public API documentation should be written in **two languages**:

**PT-BR:** Para melhorar a legibilidade em open source, a documentação da API pública deve ser escrita em **dois idiomas**:

- **EN:** English first (`<summary>` first sentence/paragraph in English).  
  **PT-BR:** Inglês primeiro (primeira frase/parágrafo de `<summary>` em inglês).
- **EN:** Portuguese next (second sentence/paragraph in Portuguese).  
  **PT-BR:** Português em seguida (segunda frase/parágrafo em português).

**EN:** Recommended XML doc pattern:  
**PT-BR:** Padrão recomendado de documentação XML:

```csharp
/// <summary>
/// English description.
/// Descrição em português.
/// </summary>
```

**EN:** When overriding or implementing members that already have documentation, prefer:  
**PT-BR:** Ao sobrescrever ou implementar membros que já possuem documentação, prefira:

```csharp
/// <inheritdoc/>
```

**EN:** This keeps compiler warnings visible (including `CS1591`) so missing docs are fixed instead of hidden.  
**PT-BR:** Isso mantém os avisos do compilador visíveis (incluindo `CS1591`) para que a documentação ausente seja corrigida, e não escondida.

## Contribution | Contribuição

**EN:** Contributions are welcome! If you want to improve DbSqlLikeMem, open an issue to discuss your idea or submit a pull request.

**PT-BR:** Contribuições são bem-vindas! Se você quiser melhorar o DbSqlLikeMem, abra uma issue para discutir sua ideia ou envie um pull request.

**EN:** If you want to support the project financially, use **GitHub Sponsors** ("Sponsor" button) or **Buy Me a Coffee**: https://buymeacoffee.com/chrisulson.

**PT-BR:** Se você quiser apoiar o projeto financeiramente, use o **GitHub Sponsors** (botão "Sponsor") ou o **Buy Me a Coffee**: https://buymeacoffee.com/chrisulson.

**EN:** High-impact areas:  
**PT-BR:** Áreas de alto impacto:

- **EN:** Expand SQL compatibility by dialect.  
  **PT-BR:** Expandir compatibilidade SQL por dialeto.
- **EN:** Add examples and documentation.  
  **PT-BR:** Adicionar exemplos e documentação.
- **EN:** Improve performance and diagnostics.  
  **PT-BR:** Melhorar desempenho e diagnósticos.
- **EN:** Increase test coverage.  
  **PT-BR:** Aumentar cobertura de testes.

## Documentation structure for GitHub Wiki | Estrutura de documentação para Wiki do GitHub

**EN:** If you want to publish a GitHub Wiki based on local content:  
**PT-BR:** Se você quiser publicar uma Wiki no GitHub com base no conteúdo local:

- **EN:** See the step-by-step guide in [docs/wiki/README.md](docs/wiki/README.md).  
  **PT-BR:** Veja o passo a passo em [docs/wiki/README.md](docs/wiki/README.md).
- **EN:** Ready-to-use wiki pages are available in [docs/wiki/pages](docs/wiki/pages).  
  **PT-BR:** Arquivos prontos para páginas de wiki estão em [docs/wiki/pages](docs/wiki/pages).
