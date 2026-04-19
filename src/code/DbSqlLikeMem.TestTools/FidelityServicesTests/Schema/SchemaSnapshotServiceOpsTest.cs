using DbSqlLikeMem.TestTools.Performance;
using System.Data;
using System.Text.Json;

namespace DbSqlLikeMem.TestTools.Schema;

/// <summary>
/// EN: Provides fidelity tests for provider schema snapshot export, serialization, parsing, and application operations.
/// PT: Fornece testes de fidelidade para operações de exportação, serialização, análise e aplicação de snapshots de schema do provedor.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class SchemaSnapshotServiceOpsTest(
        RepoService repo,
        FidelityTestContext context
    ) : PerformanceServiceBase(repo, context)
{
    /// <summary>
    /// EN: Reads a schema snapshot from the provider when the feature exists.
    /// PT: Lê um snapshot de schema do provedor quando o recurso existe.
    /// </summary>
    public object? RunSchemaSnapshotExport()
    {
        var snapshot = GetSchemaSnapshot(Repo.Cnn);
        GC.KeepAlive(snapshot);
        return snapshot;
    }

    /// <summary>
    /// EN: Serializes a provider schema snapshot to JSON.
    /// PT: Serializa um snapshot de schema do provedor para JSON.
    /// </summary>
    public string RunSchemaSnapshotToJson()
    {
        var json = BuildCanonicalSchemaSnapshotEnvelope(Repo.Cnn);
        GC.KeepAlive(json);
        return json;
    }

    /// <summary>
    /// EN: Parses a representative schema-snapshot JSON payload.
    /// PT: Faz o parse de um payload JSON representativo de snapshot de schema.
    /// </summary>
    public JsonDocument RunSchemaSnapshotLoadJson()
    {
        var obj = RunSchemaSnapshotLoadJson(Repo.Dialect.DisplayName);
        GC.KeepAlive(obj);
        return obj;
    }

    /// <summary>
    /// EN: Parses a representative schema-snapshot JSON payload without requiring a database connection.
    /// PT: Faz o parse de um payload JSON representativo de snapshot de schema sem exigir uma conexao de banco de dados.
    /// </summary>
    public static JsonDocument RunSchemaSnapshotLoadJson(string providerDisplayName)
    {
        var json = "{\"provider\":\"" + providerDisplayName + "\",\"version\":1}";
        return System.Text.Json.JsonDocument.Parse(json);
    }

    /// <summary>
    /// EN: Applies a schema snapshot when the provider exposes an apply method.
    /// PT: Aplica um snapshot de schema quando o provedor expõe um metodo de aplicacao.
    /// </summary>
    public object? RunSchemaSnapshotApply()
    {
        var snapshot = GetSchemaSnapshot(Repo.Cnn);
        var applied = TryInvokeWithArgIfExists(Repo.Cnn, "ApplySchemaSnapshot", snapshot);
        GC.KeepAlive(applied);
        return applied;
    }

    /// <summary>
    /// EN: Serializes and parses a schema snapshot in a round-trip flow.
    /// PT: Serializa e faz o parse de um snapshot de schema em um fluxo de ida e volta.
    /// </summary>
    public JsonDocument RunSchemaSnapshotRoundTrip()
    {
        var snapshot = GetSchemaSnapshot(Repo.Cnn);
        var json = snapshot.ToJson();
        var obj = System.Text.Json.JsonDocument.Parse(json);
        GC.KeepAlive(obj);
        return obj;
    }

    /// <summary>
    /// EN: Compares two provider schema snapshots serialized to JSON.
    /// PT: Compara dois snapshots de schema do provedor serializados para JSON.
    /// </summary>
    public bool RunSchemaSnapshotCompare()
    {
        var snapshot = GetSchemaSnapshot(Repo.Cnn);
        var comparison = snapshot.Matches(snapshot);
        GC.KeepAlive(comparison);
        return comparison;
    }

    private SchemaSnapshot GetSchemaSnapshot(DbConnection connection)
    {
        EnsureConnectionOpen(connection);

        if (connection is DbConnectionMockBase mockConnection)
        {
            return mockConnection.ExportSchemaSnapshot();
        }

        return new SchemaSnapshot
        {
            DialectName = GetSchemaDialectName(Repo.Dialect.Provider),
            Version = InferFallbackVersion(connection),
            Schemas = [],
        };
    }

    private string BuildCanonicalSchemaSnapshotEnvelope(DbConnection connection)
    {
        EnsureConnectionOpen(connection);

        return JsonSerializer.Serialize(new
        {
            DialectName = GetSchemaDialectName(Repo.Dialect.Provider),
            Version = InferFallbackVersion(connection),
        });
    }

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
            ProviderId.Firebird => "firebird",
            _ => provider.ToString().ToLowerInvariant(),
        };

    private int InferFallbackVersion(DbConnection connection)
    {
        var serverVersion = connection.ServerVersion;
        var (major, minor) = ReadVersionParts(serverVersion);

        return Repo.Dialect.Provider switch
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

    private static (int major, int minor) ReadVersionParts(string text)
    {
        var major = 0;
        var minor = 0;
        var current = 0;
        var partIndex = 0;
        var hasDigits = false;

        foreach (var ch in text)
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
