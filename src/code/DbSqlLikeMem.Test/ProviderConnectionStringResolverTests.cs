namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Verifies provider connection string resolution for the database-backed test environment variables.
/// PT: Verifica a resolucao de connection string dos provedores para as variaveis de ambiente usadas nos testes com banco de dados.
/// </summary>
public sealed class ProviderConnectionStringResolverTests
{
    /// <summary>
    /// EN: Verifies the resolver prefers benchmark environment variables for each provider family.
    /// PT: Verifica se o resolvedor prefere as variaveis de ambiente de benchmark para cada familia de provedor.
    /// </summary>
    [Theory]
    [MemberData(nameof(PreferredBenchmarkConnectionStrings))]
    [Trait("Category", "Core")]
    public void TryResolve_ShouldPreferBenchmarkConnectionString(
        ProviderId provider,
        IReadOnlyDictionary<string, string?> environment,
        string expectedSourceName,
        string expectedConnectionString)
    {
        var resolved = ProviderConnectionStringResolver.TryResolve(
            provider,
            name => environment.TryGetValue(name, out var value) ? value : null,
            out var connectionString,
            out var sourceName);

        resolved.Should().BeTrue();
        sourceName.Should().Be(expectedSourceName);
        if (provider == ProviderId.Oracle)
        {
            connectionString.Should().Contain("Connection Timeout=120");
            connectionString.ToLowerInvariant().Should().Contain(expectedConnectionString.ToLowerInvariant());
            return;
        }

        connectionString.Should().Be(expectedConnectionString);
    }

    /// <summary>
    /// EN: Verifies the resolver falls back to the legacy PostgreSQL aliases when the benchmark aliases are absent.
    /// PT: Verifica se o resolvedor usa os aliases legados de PostgreSQL quando os aliases de benchmark estao ausentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryResolve_ShouldFallbackToLegacyPostgresAlias()
    {
        var environment = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["POSTGRES_CONNECTION_STRING"] = "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"
        };

        var resolved = ProviderConnectionStringResolver.TryResolve(
            ProviderId.Npgsql,
            name => environment.TryGetValue(name, out var value) ? value : null,
            out var connectionString,
            out var sourceName);

        resolved.Should().BeTrue();
        sourceName.Should().Be("POSTGRES_CONNECTION_STRING");
        connectionString.Should().Be("Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;");
    }

    /// <summary>
    /// EN: Returns benchmark connection string cases used by the provider-backed query tests.
    /// PT: Retorna os casos de connection string de benchmark usados pelos testes de query com banco.
    /// </summary>
    public static TheoryData<ProviderId, IReadOnlyDictionary<string, string?>, string, string> PreferredBenchmarkConnectionStrings => new()
    {
        {
            ProviderId.MySql,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING", "Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"),
                ("MYSQL_CONNECTION_STRING", "Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_MYSQL_CONNECTION_STRING",
            "Server=127.0.0.1;Port=13306;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"
        },
        {
            ProviderId.MariaDb,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_MARIADB_CONNECTION_STRING", "Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"),
                ("MARIADB_CONNECTION_STRING", "Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_MARIADB_CONNECTION_STRING",
            "Server=127.0.0.1;Port=13307;Database=benchmark;Uid=root;Pwd=root;Pooling=false;"
        },
        {
            ProviderId.Npgsql,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING", "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"),
                ("DBSQLLIKEMEM_BENCH_POSTGRES_CONNECTION_STRING", "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"),
                ("NPGSQL_CONNECTION_STRING", "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"),
                ("POSTGRES_CONNECTION_STRING", "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_NPGSQL_CONNECTION_STRING",
            "Host=127.0.0.1;Port=15432;Database=benchmark;Username=postgres;Password=postgres;Pooling=false;"
        },
        {
            ProviderId.SqlServer,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING", "Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;Encrypt=False;TrustServerCertificate=True;Pooling=false;"),
                ("SQLSERVER_CONNECTION_STRING", "Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;Encrypt=False;TrustServerCertificate=True;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_SQLSERVER_CONNECTION_STRING",
            "Server=127.0.0.1,11433;Database=master;User Id=sa;Password=Your_password123;Encrypt=False;TrustServerCertificate=True;Pooling=false;"
        },
        {
            ProviderId.Oracle,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING", "User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"),
                ("ORACLE_CONNECTION_STRING", "User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_ORACLE_CONNECTION_STRING",
            "User Id=benchmark;Password=benchmark;Data Source=127.0.0.1:15211/FREEPDB1;Pooling=false;"
        },
        {
            ProviderId.Db2,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING", "DATABASE=BENCH;SERVER=127.0.0.1:15000;USER_ID=db2inst1;PASSWORD=db2inst1;POOLING=False;"),
                ("DB2_CONNECTION_STRING", "DATABASE=BENCH;SERVER=127.0.0.1:15000;USER_ID=db2inst1;PASSWORD=db2inst1;POOLING=False;")),
            "DBSQLLIKEMEM_BENCH_DB2_CONNECTION_STRING",
            "DATABASE=BENCH;SERVER=127.0.0.1:15000;USER_ID=db2inst1;PASSWORD=db2inst1;POOLING=False;"
        },
        {
            ProviderId.Firebird,
            BuildEnvironment(
                ("DBSQLLIKEMEM_BENCH_FIREBIRD_CONNECTION_STRING", "User=benchmark;Password=benchmark;Database=127.0.0.1/13050:/var/lib/firebird/data/benchmark.fdb;Dialect=3;Charset=UTF8;Pooling=false;"),
                ("FIREBIRD_CONNECTION_STRING", "User=benchmark;Password=benchmark;Database=127.0.0.1/13050:/var/lib/firebird/data/benchmark.fdb;Dialect=3;Charset=UTF8;Pooling=false;")),
            "DBSQLLIKEMEM_BENCH_FIREBIRD_CONNECTION_STRING",
            "User=benchmark;Password=benchmark;Database=127.0.0.1/13050:/var/lib/firebird/data/benchmark.fdb;Dialect=3;Charset=UTF8;Pooling=false;"
        }
    };

    private static Dictionary<string, string?> BuildEnvironment(params (string Name, string Value)[] entries)
        => entries.ToDictionary(entry => entry.Name, entry => (string?)entry.Value, StringComparer.OrdinalIgnoreCase);
}
