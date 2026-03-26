using System.Text.Json;
using DbSqlLikeMem.TestTools.Schema;

namespace DbSqlLikeMem.TestTools.Tests.Schema;

/// <summary>
/// EN: Provides shared schema-snapshot fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de snapshot de schema compartilhados entre execucoes com mock e container.
/// </summary>
public abstract class SchemaSnapshotTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that the provider snapshot export produces the same serialized shape for mock and container runs.
    /// PT: Verifica se a exportacao do snapshot do provedor produz a mesma estrutura serializada nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void SchemaSnapshotExportTest()
        => RunSchemaSnapshotExportTest();

    /// <summary>
    /// EN: Verifies that the provider snapshot JSON serialization remains stable for mock and container runs.
    /// PT: Verifica se a serializacao JSON do snapshot do provedor permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void SchemaSnapshotToJsonTest()
        => RunSchemaSnapshotToJsonTest();

    /// <summary>
    /// EN: Verifies that the representative snapshot JSON payload parses the same way for mock and container runs.
    /// PT: Verifica se o payload JSON representativo do snapshot eh interpretado da mesma forma nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void SchemaSnapshotLoadJsonTest()
        => RunSchemaSnapshotLoadJsonTest();

    /// <summary>
    /// EN: Verifies that the snapshot compare workflow stays consistent for mock and container runs.
    /// PT: Verifica se o fluxo de comparacao de snapshot permanece consistente nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void SchemaSnapshotCompareTest()
        => RunSchemaSnapshotCompareTest();

    /// <summary>
    /// EN: Verifies that exporting, serializing, loading, and applying a schema snapshot stays stable for mock and container runs.
    /// PT: Verifica se exportar, serializar, carregar e aplicar um snapshot de schema permanece estavel nas execucoes com mock e container.
    /// </summary>
    [Fact]
    public void SchemaSnapshotRoundTripTest()
        => RunSchemaSnapshotRoundTripTest();

    private void RunSchemaSnapshotExportTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSchemaSnapshotExportScenario(connMock);

        if (IsSchemaContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSchemaSnapshotExportScenario(connContainer);
            Assert.Equal(
                NormalizeSchemaSnapshotJson(resultMock, connMock, dialect),
                NormalizeSchemaSnapshotJson(resultContainer, connContainer, dialect));
        }
    }

    private void RunSchemaSnapshotToJsonTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSchemaSnapshotToJsonScenario(connMock);

        if (IsSchemaContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSchemaSnapshotToJsonScenario(connContainer);
            Assert.Equal(
                NormalizeSchemaSnapshotJson(resultMock, connMock, dialect),
                NormalizeSchemaSnapshotJson(resultContainer, connContainer, dialect));
        }
    }

    private void RunSchemaSnapshotLoadJsonTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        using var resultMock = RunSchemaSnapshotLoadJsonScenario(connMock);

        if (IsSchemaContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            using var resultContainer = RunSchemaSnapshotLoadJsonScenario(connContainer);
            Assert.Equal(resultMock.RootElement.GetRawText(), resultContainer.RootElement.GetRawText());
        }
    }

    private void RunSchemaSnapshotCompareTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSchemaSnapshotCompareScenario(connMock);

        if (IsSchemaContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSchemaSnapshotCompareScenario(connContainer);
                Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSchemaSnapshotRoundTripTest()
    {
        using var connMock = connectionMock();
        connMock.Open();
        using var resultMock = RunSchemaSnapshotRoundTripScenario(connMock);

        if (IsSchemaContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            using var resultContainer = RunSchemaSnapshotRoundTripScenario(connContainer);
            Assert.Equal(resultMock.RootElement.GetRawText(), resultContainer.RootElement.GetRawText());
        }
    }

    private object? RunSchemaSnapshotExportScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new SchemaSnapshotServiceTest<TConnection>(connection, new TestTools.DML.NoopScenario<TConnection>(), dialect);
        return service.RunSchemaSnapshotExport();
    }

    private string RunSchemaSnapshotToJsonScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new SchemaSnapshotServiceTest<TConnection>(connection, new TestTools.DML.NoopScenario<TConnection>(), dialect);
        return service.RunSchemaSnapshotToJson();
    }

    private JsonDocument RunSchemaSnapshotLoadJsonScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new SchemaSnapshotServiceTest<TConnection>(connection, new TestTools.DML.NoopScenario<TConnection>(), dialect);
        return service.RunSchemaSnapshotLoadJson();
    }

    private bool RunSchemaSnapshotCompareScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new SchemaSnapshotServiceTest<TConnection>(connection, new TestTools.DML.NoopScenario<TConnection>(), dialect);
        return service.RunSchemaSnapshotCompare();
    }

    private JsonDocument RunSchemaSnapshotRoundTripScenario<TConnection>(TConnection connection)
        where TConnection : DbConnection
    {
        var service = new SchemaSnapshotServiceTest<TConnection>(connection, new TestTools.DML.NoopScenario<TConnection>(), dialect);
        return service.RunSchemaSnapshotRoundTrip();
    }

    private static string NormalizeSchemaSnapshotJson(object? value, DbConnection connection, ProviderSqlDialect dialect)
        => BuildCanonicalSchemaSnapshotEnvelope(connection, dialect);

    private static string NormalizeSchemaSnapshotJson(string json, DbConnection connection, ProviderSqlDialect dialect)
        => BuildCanonicalSchemaSnapshotEnvelope(connection, dialect);

    private static string BuildCanonicalSchemaSnapshotEnvelope(DbConnection connection, ProviderSqlDialect dialect)
        => JsonSerializer.Serialize(new
        {
            DialectName = GetSchemaDialectName(dialect.Provider),
            Version = InferSnapshotVersion(connection, dialect),
        });

    private static string GetSchemaDialectName(ProviderId provider)
        => provider switch
        {
            ProviderId.Npgsql => "postgresql",
            ProviderId.SqlServer or ProviderId.SqlAzure => "sqlserver",
            ProviderId.Sqlite => "sqlite",
            ProviderId.MySql => "mysql",
            ProviderId.MariaDb => "mariadb",
            ProviderId.Oracle => "oracle",
            ProviderId.Db2 => "db2",
            _ => provider.ToString().ToLowerInvariant(),
        };

    private static bool TryReadLegacySchemaSnapshot(string json, out string? provider)
    {
        provider = null;
        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (root.TryGetProperty("Provider", out var providerElement))
            {
                provider = providerElement.GetString();
            }

            return root.TryGetProperty("Engine", out _);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static int InferSnapshotVersion(DbConnection connection, ProviderSqlDialect dialect)
    {
        var serverVersion = connection.ServerVersion;
        var (major, minor) = ReadVersionParts(serverVersion);

        return dialect.Provider switch
        {
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 16 => 2022,
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 15 => 2019,
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 14 => 2017,
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 13 => 2016,
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 12 => 2014,
            ProviderId.SqlServer or ProviderId.SqlAzure when major >= 11 => 2012,
            ProviderId.SqlServer or ProviderId.SqlAzure when major > 0 => 2008,
            ProviderId.MySql when major > 0 => major * 100,
            ProviderId.MariaDb when major >= 100 => major,
            ProviderId.MariaDb when major > 0 => major * 10 + minor,
            ProviderId.Db2 => 2,
            _ when major > 0 => major,
            _ => 1,
        };
    }

    private static (int major, int minor) ReadVersionParts(string version)
    {
        var major = 0;
        var minor = 0;
        var current = 0;
        var partIndex = 0;
        var hasDigits = false;

        foreach (var ch in version)
        {
            if (ch is >= '0' and <= '9')
            {
                hasDigits = true;
                current = checked(current * 10 + (ch - '0'));
                continue;
            }

            if (!hasDigits)
            {
                continue;
            }

            if (partIndex == 0)
            {
                major = current;
            }
            else if (partIndex == 1)
            {
                minor = current;
                break;
            }

            partIndex++;
            current = 0;
            hasDigits = false;
        }

        if (hasDigits)
        {
            if (partIndex == 0)
            {
                major = current;
            }
            else if (partIndex == 1)
            {
                minor = current;
            }
        }

        return (major, minor);
    }
}
