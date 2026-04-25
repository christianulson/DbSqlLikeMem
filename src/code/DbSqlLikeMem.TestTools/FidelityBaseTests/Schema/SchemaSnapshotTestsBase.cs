using DbSqlLikeMem.TestTools.DML;
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
    [FidelityFact]
    public async Task SchemaSnapshotExportTest()
    {
        var result = (string?)await RunFidelityTestAsync(
            (s, a) => Task.FromResult<object?>(NormalizeSchemaSnapshotJson(s.RunSchemaSnapshotExport(), s.Repo.Cnn)));

        result.Should().NotBeNullOrWhiteSpace();
    }

    /// <summary>
    /// EN: Verifies that the provider snapshot JSON serialization remains stable for mock and container runs.
    /// PT: Verifica se a serializacao JSON do snapshot do provedor permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task SchemaSnapshotToJsonTest()
    {
        var result = (string?)await RunFidelityTestAsync(
            (s, a) => Task.FromResult<object?>(NormalizeSchemaSnapshotJson(s.RunSchemaSnapshotToJson(), s.Repo.Cnn)));

        result.Should().NotBeNullOrWhiteSpace();
    }

    /// <summary>
    /// EN: Verifies that the representative snapshot JSON payload parses the same way for mock and container runs.
    /// PT: Verifica se o payload JSON representativo do snapshot eh interpretado da mesma forma nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task SchemaSnapshotLoadJsonTest()
    {
        var result = (string?)await RunFidelityTestAsync(
            (s, a) =>
            {
                using var document = s.RunSchemaSnapshotLoadJson();
                return Task.FromResult<object?>(document.RootElement.GetRawText());
            });

        result.Should().NotBeNullOrWhiteSpace();
    }

    /// <summary>
    /// EN: Verifies that the snapshot compare workflow stays consistent for mock and container runs.
    /// PT: Verifica se o fluxo de comparacao de snapshot permanece consistente nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task SchemaSnapshotCompareTest()
    {
        var result = (bool?)await RunFidelityTestAsync(
            (s, a) => Task.FromResult<object?>(s.RunSchemaSnapshotCompare()));

        result.Should().BeTrue();
    }

    /// <summary>
    /// EN: Verifies that exporting, serializing, loading, and applying a schema snapshot stays stable for mock and container runs.
    /// PT: Verifica se exportar, serializar, carregar e aplicar um snapshot de schema permanece estavel nas execucoes com mock e container.
    /// </summary>
    [FidelityFact]
    public async Task SchemaSnapshotRoundTripTest()
    {
        var result = (string?)await RunFidelityTestAsync(
            (s, a) => Task.FromResult<object?>(NormalizeSchemaSnapshotJson(s.RunSchemaSnapshotRoundTrip(), s.Repo.Cnn)));

        result.Should().NotBeNullOrWhiteSpace();
    }

    private async Task<object?> RunFidelityTestAsync(
        Func<SchemaSnapshotServiceOpsTest, object[], Task<object?>> runTest,
        params object[] args)
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        return await testService.RunTestAsync<NoopScenario, SchemaSnapshotServiceOpsTest>(runTest, args);
    }

    private string NormalizeSchemaSnapshotJson(object? value, DbConnection connection)
        => BuildCanonicalSchemaSnapshotEnvelope(connection);

    private string NormalizeSchemaSnapshotJson(string json, DbConnection connection)
        => BuildCanonicalSchemaSnapshotEnvelope(connection);

    private string BuildCanonicalSchemaSnapshotEnvelope(DbConnection connection)
        => JsonSerializer.Serialize(new
        {
            DialectName = GetSchemaDialectName(),
            Version = InferSnapshotVersion(connection),
        });

    /// <summary>
    /// EN: Gets the canonical dialect name used in schema snapshot assertions for the current provider.
    /// PT: Obtem o nome canonico do dialect usado nas assercoes de snapshot de schema para o provedor atual.
    /// </summary>
    /// <returns>EN: The canonical schema dialect name. PT: O nome canonico do dialect de schema.</returns>
    protected virtual string GetSchemaDialectName()
        => dialect.Provider switch
        {
            ProviderId.Npgsql => "postgresql",
            ProviderId.SqlServer or ProviderId.SqlAzure => "sqlserver",
            ProviderId.Sqlite => "sqlite",
            ProviderId.MySql => "mysql",
            ProviderId.MariaDb => "mariadb",
            ProviderId.Oracle => "oracle",
            ProviderId.Db2 => "db2",
            ProviderId.Firebird => "firebird",
            _ => dialect.Provider.ToString().ToLowerInvariant(),
        };

    /// <summary>
    /// EN: Infers the schema snapshot version from the provider and the connection server version.
    /// PT: Infere a versao do snapshot de schema a partir do provedor e da versao do servidor da conexao.
    /// </summary>
    /// <param name="connection">EN: The open database connection. PT: A conexao de banco aberta.</param>
    /// <returns>EN: The inferred schema snapshot version. PT: A versao inferida do snapshot de schema.</returns>
    protected virtual int InferSnapshotVersion(DbConnection connection)
    {
        EnsureConnectionOpen(connection);

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
            ProviderId.Sqlite when major >= 100 => major / 100,
            ProviderId.Sqlite when major > 0 => major,
            ProviderId.MySql when major > 0 => major * 100,
            ProviderId.MariaDb when major >= 100 => major,
            ProviderId.MariaDb when major > 0 => major * 10 + minor,
            ProviderId.Db2 => 2,
            ProviderId.Firebird when major >= 10 => major / 10,
            _ when major > 0 => major,
            _ => 1,
        };
    }

    private static void EnsureConnectionOpen(DbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }
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

