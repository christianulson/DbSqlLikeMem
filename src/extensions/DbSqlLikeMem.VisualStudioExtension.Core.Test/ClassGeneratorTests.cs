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
                [new DatabaseObjectReference("dbo", "sales_order", DatabaseObjectType.Table, "public")]);

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
                [new DatabaseObjectReference("public", "vw.active-customers", DatabaseObjectType.View, "public")]);

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

    /// <summary>
    /// EN: Verifies procedure and function objects are written in the same generation run using their routine-specific factories.
    /// PT: Verifica se objetos procedure e function sao gravados na mesma execucao de geracao usando suas factories especificas de rotina.
    /// </summary>
    [Fact]
    [Trait("Category", "ClassGenerator")]
    public async Task GenerateAsync_WritesProcedureAndFunctionFilesInTheSameRun()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"dbsql-{Guid.NewGuid():N}");

        try
        {
            var generator = new ClassGenerator();
            var request = new GenerationRequest(
                new ConnectionDefinition("1", "SqlServer", "ERP", "conn"),
                [
                    new DatabaseObjectReference(
                        "dbo",
                        "sp_update_customer",
                        DatabaseObjectType.Procedure,
                        "public",
                        new Dictionary<string, string>
                        {
                            ["RequiredIn"] = "CustomerId|Int32|1|",
                            ["OptionalIn"] = "",
                            ["OutParams"] = "RowsAffected|Int32|1|",
                            ["ReturnParam"] = "ReturnCode|Int32|0|"
                        }),
                    new DatabaseObjectReference(
                        "dbo",
                        "fn_total",
                        DatabaseObjectType.Function,
                        "public",
                        new Dictionary<string, string>
                        {
                            ["Parameters"] = "CustomerId|int|1|0|0|0|",
                            ["ReturnTypeSql"] = "int",
                            ["BodySql"] = "CustomerId + 1"
                        })
                ]);

            var config = new ConnectionMappingConfiguration(
                "1",
                new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                {
                    [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, outputDir)
                });

            var files = await generator.GenerateAsync(request, config, dbObject => $"// {dbObject.Type}:{dbObject.Name}", TestContext.Current.CancellationToken);

            var writtenFiles = files.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).ToArray();
            var expectedFiles = new[]
            {
                Path.Combine(outputDir, "FnTotalFunctionFactory.cs"),
                Path.Combine(outputDir, "SpUpdateCustomerProcedureFactory.cs")
            };

            Assert.Equal(expectedFiles, writtenFiles);
            Assert.All(writtenFiles, file => Assert.True(File.Exists(file)));
            Assert.Equal("// Function:fn_total", await File.ReadAllTextAsync(Path.Combine(outputDir, "FnTotalFunctionFactory.cs"), TestContext.Current.CancellationToken));
            Assert.Equal("// Procedure:sp_update_customer", await File.ReadAllTextAsync(Path.Combine(outputDir, "SpUpdateCustomerProcedureFactory.cs"), TestContext.Current.CancellationToken));
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
