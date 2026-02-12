namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public class ClassGeneratorTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [InlineData("MySql")]
    [InlineData("SqlServer")]
    [InlineData("PostgreSql")]
    [InlineData("Oracle")]
    [InlineData("Sqlite")]
    [InlineData("Db2")]
    public async Task GenerateAsync_WithoutPattern_UsesConsoleLikeRule_ForAllDatabaseTypes(string databaseType)
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"dbsql-{Guid.NewGuid():N}");

        try
        {
            var generator = new ClassGenerator();
            var request = new GenerationRequest(
                new ConnectionDefinition("1", databaseType, "ERP", "conn"),
                [new DatabaseObjectReference("dbo", "sales_order", DatabaseObjectType.Table)]);

            var config = new ConnectionMappingConfiguration(
                "1",
                new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                {
                    [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, outputDir, string.Empty)
                });

            var files = await generator.GenerateAsync(request, config, _ => "// test", TestContext.Current.CancellationToken);

            var file = Assert.Single(files);
            Assert.Equal(Path.Combine(outputDir, "SalesOrderTableFactory.cs"), file);
            Assert.True(File.Exists(file));
        }
        finally
        {
            if (Directory.Exists(outputDir))
            {
                Directory.Delete(outputDir, recursive: true);
            }
        }
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public async Task GenerateAsync_ReplacesPatternTokensIncludingDatabaseContext()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"dbsql-{Guid.NewGuid():N}");

        try
        {
            var generator = new ClassGenerator();
            var request = new GenerationRequest(
                new ConnectionDefinition("1", "PostgreSql", "Billing", "conn"),
                [new DatabaseObjectReference("public", "vw.active-customers", DatabaseObjectType.View)]);

            var config = new ConnectionMappingConfiguration(
                "1",
                new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                {
                    [DatabaseObjectType.View] = new(
                        DatabaseObjectType.View,
                        outputDir,
                        "{DatabaseType}.{DatabaseName}.{Schema}.{NamePascal}.{Type}.cs")
                });

            var files = await generator.GenerateAsync(request, config, _ => "// test", TestContext.Current.CancellationToken);

            var file = Assert.Single(files);
            Assert.Equal(
                Path.Combine(outputDir, "PostgreSql.Billing.public.VwActiveCustomers.View.cs"),
                file);
            Assert.True(File.Exists(file));
        }
        finally
        {
            if (Directory.Exists(outputDir))
            {
                Directory.Delete(outputDir, recursive: true);
            }
        }
    }
}
