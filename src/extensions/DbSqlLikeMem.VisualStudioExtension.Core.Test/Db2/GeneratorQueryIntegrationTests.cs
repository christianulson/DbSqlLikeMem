#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#elif NET6_0
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#elif NET8_0
using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using DB2Connection = IBM.Data.Db2.DB2Connection;
#endif


namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.Db2;

/// <summary>
/// EN: Verifies generator queries against a real Db2 benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark Db2 real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real Db2 database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco Db2 real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!TryResolveContainerConnectionString(ProviderId.Db2, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"GQT_{suffix.ToUpperInvariant()}";
        var functionName = $"GQF_{suffix.ToUpperInvariant()}";
        var procedureName = $"GQP_{suffix.ToUpperInvariant()}";

        using var connection = new DB2Connection(connectionString);
        connection.Open();

        var schemaName = ReadCurrentSchema(connection);
        var definition = new ConnectionDefinition(
            "benchmark-db2",
            "Db2",
            schemaName,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION {functionName}(INT)");
        ExecuteNonQueryIgnoreErrors(connection, $"DROP PROCEDURE {procedureName}");
        ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE {tableName}");

        try
        {
            ExecuteNonQuery(connection, $"""
CREATE TABLE {tableName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL
)
""");
            ExecuteNonQuery(connection, $"CREATE INDEX IX_{tableName}_NAME ON {tableName} (Name)");
            ExecuteNonQuery(connection, $"CREATE OR REPLACE FUNCTION {functionName}(baseValue INT) RETURNS INT RETURN baseValue + 1");
            ExecuteNonQuery(connection, $"CREATE OR REPLACE PROCEDURE {procedureName}(IN tenantId INT, OUT tenantEcho INT) BEGIN END");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Function
                && x.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Procedure
                && x.Name.Equals(procedureName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(schemaName, tableName, DatabaseObjectType.Table),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("ID");
            table.Properties["Columns"].Should().ContainEquivalentOf("ID|");
            table.Properties["Columns"].Should().ContainEquivalentOf("NAME|");
            table.Properties["Indexes"].Should().ContainEquivalentOf($"IX_{tableName}_NAME|0|NAME");

            var function = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(schemaName, functionName, DatabaseObjectType.Function),
                CancellationToken.None);

            function.Should().NotBeNull();
            function!.Properties!["ReturnTypeSql"].Should().ContainEquivalentOf("INT");
            function.Properties["BodySql"].Should().ContainEquivalentOf("baseValue + 1");
            function.Properties["Parameters"].Should().ContainEquivalentOf("BASEVALUE|Int32|1|0|0|0|");

            var procedure = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(schemaName, procedureName, DatabaseObjectType.Procedure),
                CancellationToken.None);

            procedure.Should().NotBeNull();
            procedure!.Properties!["RequiredIn"].Should().ContainEquivalentOf("TENANTID|Int32|1|");
            procedure.Properties["OutParams"].Should().ContainEquivalentOf("TENANTECHO|Int32|1|");
            procedure.Properties["OptionalIn"].Should().BeEmpty();
            procedure.Properties["ReturnParam"].Should().BeEmpty();
        }
        finally
        {
            ExecuteNonQueryIgnoreErrors(connection, $"DROP PROCEDURE {procedureName}");
            ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION {functionName}(INT)");
            ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE {tableName}");
        }
    }

    private static string ReadCurrentSchema(DB2Connection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "VALUES CURRENT SCHEMA";
        var value = command.ExecuteScalar();
        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static void ExecuteNonQuery(DB2Connection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void ExecuteNonQueryIgnoreErrors(DB2Connection connection, string sql)
    {
        try
        {
            ExecuteNonQuery(connection, sql);
        }
        catch
        {
            // Cleanup is best-effort for benchmark schemas.
        }
    }

    private static SqlDatabaseMetadataProvider CreateProvider()
        => new(new DbConnectionMetadataQueryExecutor(cs => new DB2Connection(cs)));

}
