using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.Oracle;

/// <summary>
/// EN: Verifies generator queries against a real Oracle benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark Oracle real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real Oracle database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco Oracle real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!ProviderConnectionStringResolver.TryResolve(ProviderId.Oracle, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"GQT_{suffix.ToUpperInvariant()}";
        var functionName = $"GQF_{suffix.ToUpperInvariant()}";
        var schemaName = "BENCHMARK";

        using var connection = new OracleConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        var definition = new ConnectionDefinition(
            "benchmark-oracle",
            "Oracle",
            schemaName,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION {functionName}");
        ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE {tableName}");

        try
        {
            ExecuteNonQuery(connection, $"""
CREATE TABLE {tableName} (
    Id NUMBER(10) PRIMARY KEY,
    Name VARCHAR2(100) NOT NULL
)
""");
            ExecuteNonQuery(connection, $"CREATE INDEX IX_{tableName}_NAME ON {tableName} (Name)");
            ExecuteNonQuery(connection, $"""
CREATE OR REPLACE FUNCTION {functionName}(baseValue NUMBER)
RETURN NUMBER
IS
BEGIN
    RETURN baseValue + 1;
END;
""");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Function
                && x.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals(schemaName, StringComparison.OrdinalIgnoreCase));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(schemaName, tableName, DatabaseObjectType.Table),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("ID");
            table.Properties["Columns"].Should().Contain("ID|NUMBER|1|0|0|");
            table.Properties["Columns"].Should().Contain("NAME|VARCHAR2|2|0|0|");
            table.Properties["Indexes"].Should().Contain($"IX_{tableName}_NAME|0|NAME");

            var routine = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(schemaName, functionName, DatabaseObjectType.Function),
                CancellationToken.None);

            routine.Should().NotBeNull();
            routine!.Properties!["ReturnTypeSql"].Should().Contain("NUMBER");
            routine.Properties["BodySql"].Should().Be("baseValue + 1");
            routine.Properties["Parameters"].Should().Contain("BASEVALUE|NUMBER|1|0|0|0|");
        }
        finally
        {
            ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION {functionName}");
            ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE {tableName}");
        }
    }

    private static void ExecuteNonQuery(OracleConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void ExecuteNonQueryIgnoreErrors(OracleConnection connection, string sql)
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
        => new(new DbConnectionMetadataQueryExecutor(
            cs => new OracleConnection(cs),
            command => ((OracleCommand)command).BindByName = true,
            name => name.StartsWith(":", StringComparison.Ordinal) ? name : $":{name}",
            Console.WriteLine));

}
