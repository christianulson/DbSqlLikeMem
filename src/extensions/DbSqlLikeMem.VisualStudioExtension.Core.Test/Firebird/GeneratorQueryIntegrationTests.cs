#if NET462
using FbConnection = FirebirdSql.Data.FirebirdClient.FbConnection;
#elif NET6_0
using FbConnection = FirebirdSql.Data.FirebirdClient.FbConnection;
#elif NET8_0
using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using FbConnection = FirebirdSql.Data.FirebirdClient.FbConnection;
#endif


namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.Firebird;

/// <summary>
/// EN: Verifies generator queries against a real Firebird benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark Firebird real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real Firebird database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco Firebird real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!TryResolveContainerConnectionString(ProviderId.Firebird, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"GQT_{suffix.ToUpperInvariant()}";
        var functionName = $"GQF_{suffix.ToUpperInvariant()}";
        var procedureName = $"GQP_{suffix.ToUpperInvariant()}";

        using var connection = new FbConnection(connectionString);
        connection.Open();

        var definition = new ConnectionDefinition(
            "benchmark-firebird",
            "Firebird",
            string.Empty,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION IF EXISTS {functionName}");
        ExecuteNonQueryIgnoreErrors(connection, $"DROP PROCEDURE IF EXISTS {procedureName}");
        ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE IF EXISTS {tableName}");

        try
        {
            ExecuteNonQuery(connection, $"""
CREATE TABLE {tableName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL
)
""");
            ExecuteNonQuery(connection, $"CREATE INDEX IX_{tableName}_NAME ON {tableName} (Name)");
            ExecuteNonQuery(connection, $"CREATE FUNCTION {functionName}(baseValue INT) RETURNS INT AS BEGIN RETURN baseValue + 1; END");
            ExecuteNonQuery(connection, $"CREATE OR ALTER PROCEDURE {procedureName}(IN tenantId INT, OUT tenantEcho INT) BEGIN END");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrEmpty(x.Schema));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Function
                && x.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrEmpty(x.Schema));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Procedure
                && x.Name.Equals(procedureName, StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrEmpty(x.Schema));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(string.Empty, tableName, DatabaseObjectType.Table),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("ID");
            table.Properties["Columns"].Should().ContainEquivalentOf("ID|");
            table.Properties["Columns"].Should().ContainEquivalentOf("NAME|");
            table.Properties["Indexes"].Should().ContainEquivalentOf($"IX_{tableName}_NAME|0|NAME");

            var function = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(string.Empty, functionName, DatabaseObjectType.Function),
                CancellationToken.None);

            function.Should().NotBeNull();
            function!.Properties!["ReturnTypeSql"].Should().ContainEquivalentOf("NUMERIC");
            function.Properties["BodySql"].Should().ContainEquivalentOf("baseValue + 1");
            function.Properties["Parameters"].Should().ContainEquivalentOf("baseValue|");

            var procedure = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference(string.Empty, procedureName, DatabaseObjectType.Procedure),
                CancellationToken.None);

            procedure.Should().NotBeNull();
            procedure!.Properties!["RequiredIn"].Should().ContainEquivalentOf("tenantId|");
            procedure.Properties["OutParams"].Should().ContainEquivalentOf("tenantEcho|");
            procedure.Properties["OptionalIn"].Should().BeEmpty();
            procedure.Properties["ReturnParam"].Should().BeEmpty();
        }
        finally
        {
            ExecuteNonQueryIgnoreErrors(connection, $"DROP PROCEDURE IF EXISTS {procedureName}");
            ExecuteNonQueryIgnoreErrors(connection, $"DROP FUNCTION IF EXISTS {functionName}");
            ExecuteNonQueryIgnoreErrors(connection, $"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private static void ExecuteNonQuery(FbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void ExecuteNonQueryIgnoreErrors(FbConnection connection, string sql)
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
        => new(new DbConnectionMetadataQueryExecutor(cs => new FbConnection(cs)));

}
