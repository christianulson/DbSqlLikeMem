using DbSqlLikeMem.VisualStudioExtension.Core.Test.Tools;
using Microsoft.Data.SqlClient;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test.SqlServer;

/// <summary>
/// EN: Verifies generator queries against a real SQL Server benchmark database.
/// PT: Verifica as queries do generator contra um banco de benchmark SQL Server real.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used to record the integration test output.
/// PT: Helper de saida do xUnit usado para registrar a saida do teste de integracao.
/// </param>
public sealed class GeneratorQueryIntegrationTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the metadata provider lists and reads benchmark objects created in a real SQL Server database.
    /// PT: Verifica se o provedor de metadados lista e le objetos de benchmark criados em um banco SQL Server real.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratorQuery")]
    public async Task ListObjectsAndObjectMetadata_ShouldReadFromBenchmarkDatabase()
    {
        if (!ProviderConnectionStringResolver.TryResolve(ProviderId.SqlServer, out var connectionString))
        {
            return;
        }

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tableName = $"gqt_{suffix}";
        var functionName = $"gqf_{suffix}";
        var procedureName = $"gqp_{suffix}";

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var databaseName = connection.Database;
        var definition = new ConnectionDefinition(
            "benchmark-sqlserver",
            "SqlServer",
            databaseName,
            connectionString);

        var provider = CreateProvider();

        ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{procedureName}', N'P') IS NOT NULL DROP PROCEDURE dbo.{procedureName};");
        ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{functionName}', N'FN') IS NOT NULL DROP FUNCTION dbo.{functionName};");
        ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE dbo.{tableName};");

        try
        {
            ExecuteNonQuery(connection, $"""
CREATE TABLE dbo.{tableName} (
    Id INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL
);
""");
            ExecuteNonQuery(connection, $"CREATE INDEX IX_{tableName}_Name ON dbo.{tableName} (Name);");
            ExecuteNonQuery(connection, $"""
CREATE FUNCTION dbo.{functionName}(@baseValue INT)
RETURNS INT
AS
BEGIN
    RETURN @baseValue + 1;
END;
""");
            ExecuteNonQuery(connection, $"""
CREATE PROCEDURE dbo.{procedureName}
    @value INT,
    @result INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @result = @value + 1;
END;
""");

            var objects = await provider.ListObjectsAsync(definition, CancellationToken.None);
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Table
                && x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals("dbo", StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Function
                && x.Name.Equals(functionName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals("dbo", StringComparison.OrdinalIgnoreCase));
            objects.Should().ContainSingle(x =>
                x.Type == DatabaseObjectType.Procedure
                && x.Name.Equals(procedureName, StringComparison.OrdinalIgnoreCase)
                && x.Schema.Equals("dbo", StringComparison.OrdinalIgnoreCase));

            var table = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference("dbo", tableName, DatabaseObjectType.Table, "public"),
                CancellationToken.None);

            table.Should().NotBeNull();
            table!.Properties!["PrimaryKey"].Should().Be("Id");
            table.Properties["Columns"].Should().Contain("Id|int|1|0|0|");
            table.Properties["Columns"].Should().Contain("Name|nvarchar|2|0|0|");
            table.Properties["Indexes"].Should().Contain($"IX_{tableName}_Name|0|Name");

            var function = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference("dbo", functionName, DatabaseObjectType.Function, "public"),
                CancellationToken.None);

            function.Should().NotBeNull();
            function!.Properties!["ReturnTypeSql"].Should().Be("int");
            function.Properties["BodySql"].Should().Be("@baseValue + 1");
            function.Properties["Parameters"].Should().Contain("baseValue|int|1|0|0|0|");

            var procedure = await provider.GetObjectAsync(
                definition,
                new DatabaseObjectReference("dbo", procedureName, DatabaseObjectType.Procedure, "public"),
                CancellationToken.None);

            procedure.Should().NotBeNull();
            procedure!.Properties!["RequiredIn"].Should().Contain("value|Int32|1|");
            procedure.Properties["OutParams"].Should().Contain("result|Int32|0|");
            procedure.Properties["OptionalIn"].Should().BeEmpty();
            procedure.Properties["ReturnParam"].Should().BeEmpty();
        }
        finally
        {
            ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{procedureName}', N'P') IS NOT NULL DROP PROCEDURE dbo.{procedureName};");
            ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{functionName}', N'FN') IS NOT NULL DROP FUNCTION dbo.{functionName};");
            ExecuteNonQuery(connection, $"IF OBJECT_ID(N'dbo.{tableName}', N'U') IS NOT NULL DROP TABLE dbo.{tableName};");
        }
    }

    private static void ExecuteNonQuery(SqlConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static SqlDatabaseMetadataProvider CreateProvider()
        => new(new DbConnectionMetadataQueryExecutor(cs => new SqlConnection(cs)));

}
