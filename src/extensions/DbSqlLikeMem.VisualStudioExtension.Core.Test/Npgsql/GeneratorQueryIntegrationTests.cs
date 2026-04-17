using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using Npgsql;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.Npgsql;

/// <summary>
/// EN: Verifies generator queries against a real PostgreSQL benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark PostgreSQL real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real PostgreSQL database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco PostgreSQL real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!ProviderConnectionStringResolver.TryResolve(ProviderId.Npgsql, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"gqt_{suffix}";
        var functionName = $"gqf_{suffix}";

        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        var databaseName = connection.Database;
        var definition = new ConnectionDefinition(
            "benchmark-npgsql",
            "PostgreSql",
            databaseName,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQuery(connection, $"DROP FUNCTION IF EXISTS {functionName}(integer)");
        ExecuteNonQuery(connection, $"DROP TABLE IF EXISTS {tableName}");

        try
        {
            ExecuteNonQuery(connection, $"""
CREATE TABLE {tableName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL
)
""");
            ExecuteNonQuery(connection, $"CREATE INDEX IX_{tableName}_Name ON {tableName} (Name)");
            ExecuteNonQuery(connection, $"""
CREATE FUNCTION {functionName}(base_value integer)
RETURNS integer
LANGUAGE SQL
AS 'SELECT base_value + 1';
""");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals("public", StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Function
                && x.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals("public", StringComparison.OrdinalIgnoreCase));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference("public", tableName, DatabaseObjectType.Table),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("id");
            table.Properties["Columns"].Should().Contain("id|integer|1|0|0|");
            table.Properties["Columns"].Should().Contain("name|character varying|2|0|0|");
            table.Properties["Indexes"].Should().Contain($"ix_{tableName}_name|0|name");

            var routine = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference("public", functionName, DatabaseObjectType.Function),
                CancellationToken.None);

            routine.Should().NotBeNull();
            routine!.Properties!["ReturnTypeSql"].Should().Be("integer");
            routine.Properties["BodySql"].Should().Be("base_value + 1");
            routine.Properties["Parameters"].Should().Contain("base_value|integer|1|0|0|0|");
        }
        finally
        {
            ExecuteNonQuery(connection, $"DROP FUNCTION IF EXISTS {functionName}(integer)");
            ExecuteNonQuery(connection, $"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private static void ExecuteNonQuery(NpgsqlConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static SqlDatabaseMetadataProvider CreateProvider()
        => new(new DbConnectionMetadataQueryExecutor(cs => new NpgsqlConnection(cs)));

}
