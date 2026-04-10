namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies generated class files are written using the configured mapping rules.
/// PT: Verifica se os arquivos de classe gerados sao gravados usando as regras de mapeamento configuradas.
/// </summary>
public class ClassGeneratorTests
{
    /// <summary>
    /// EN: Verifies the default file name pattern across the supported database types.
    /// PT: Verifica o padrao padrao de nome de arquivo entre os tipos de banco suportados.
    /// </summary>
    [Theory]
    [Trait("Category", "ClassGenerator")]
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
    /// EN: Verifies file name tokens include namespace, database, and object metadata.
    /// PT: Verifica se os tokens do nome de arquivo incluem namespace, banco e metadados do objeto.
    /// </summary>
    [Fact]
    [Trait("Category", "ClassGenerator")]
    public async Task GenerateAsync_ReplacesPatternTokensIncludingDatabaseContextAndNamespace()
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
                        "{Namespace}.{DatabaseType}.{DatabaseName}.{Schema}.{NamePascal}.{Type}.cs",
                        "Company.Generated")
                });

            var files = await generator.GenerateAsync(request, config, _ => "// test", TestContext.Current.CancellationToken);

            var file = Assert.Single(files);
            Assert.Equal(
                Path.Combine(outputDir, "Company.Generated.PostgreSql.Billing.public.VwActiveCustomers.View.cs"),
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
