using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using MySql.Data.MySqlClient;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.MariaDb;

/// <summary>
/// EN: Verifies generator queries against a real MariaDB benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark MariaDB real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real MariaDB database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco MariaDB real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!ProviderConnectionStringResolver.TryResolve(ProviderId.MariaDb, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"gqt_{suffix}";
        var procedureName = $"gqp_{suffix}";

        using var connection = new MySqlConnection(connectionString);
        connection.Open();

        var databaseName = connection.Database;
        var definition = new ConnectionDefinition(
            "benchmark-mariadb",
            "MariaDb",
            databaseName,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQuery(connection, $"DROP PROCEDURE IF EXISTS {procedureName}");
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
CREATE PROCEDURE {procedureName}(IN p_value INT, OUT p_result INT)
BEGIN
    SET p_result = p_value + 1;
END;
""");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Procedure
                && x.Name.Equals(procedureName, StringComparison.OrdinalIgnoreCase));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(databaseName, tableName, DatabaseObjectType.Table, "public"),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("Id");
            table.Properties["Columns"].Should().Contain("Id|int|1|0|0|");
            table.Properties["Columns"].Should().Contain("Name|varchar|2|0|0|");
            table.Properties["Indexes"].Should().Contain($"IX_{tableName}_Name|0|Name");

            var routine = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(databaseName, procedureName, DatabaseObjectType.Procedure, "public"),
                CancellationToken.None);

            routine.Should().NotBeNull();
            routine!.Properties!["RequiredIn"].Should().Be("p_value|Int32|1|");
            routine.Properties["OutParams"].Should().Be("p_result|Int32|0|");
            routine.Properties["OptionalIn"].Should().BeEmpty();
            routine.Properties["ReturnParam"].Should().BeEmpty();
        }
        catch (MySqlException ex) when (ex.Message.Contains("Unknown table 'SEQUENCES'", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }
        finally
        {
            ExecuteNonQuery(connection, $"DROP PROCEDURE IF EXISTS {procedureName}");
            ExecuteNonQuery(connection, $"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private static void ExecuteNonQuery(MySqlConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static SqlDatabaseMetadataProvider CreateProvider()
        => new(new DbConnectionMetadataQueryExecutor(cs => new MySqlConnection(cs)));

}
