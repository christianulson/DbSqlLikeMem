namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Regression tests focused on quality/performance sensitive flows.
/// Testes de regressão focados em fluxos sensíveis de qualidade/performance.
/// </summary>
public sealed class QualityRegressionTests
{
    /// <summary>
    /// Ensures generation honors cancellation and does not continue writing files.
    /// Garante que a geração respeita cancelamento e não continua gravando arquivos.
    /// </summary>
    [Fact]
    public async Task ClassGenerator_WhenCanceled_StopsWritingFurtherFiles()
    {
        var outputDir = Path.Combine(Path.GetTempPath(), $"dbsql-quality-{Guid.NewGuid():N}");

        try
        {
            var connection = new ConnectionDefinition("1", "SqlServer", "ERP", "conn");
            var objects = new[]
            {
                new DatabaseObjectReference("dbo", "orders", DatabaseObjectType.Table),
                new DatabaseObjectReference("dbo", "customers", DatabaseObjectType.Table)
            };

            var request = new GenerationRequest(connection, objects);
            var mapping = new ConnectionMappingConfiguration(
                "1",
                new Dictionary<DatabaseObjectType, ObjectTypeMapping>
                {
                    [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, outputDir, "{NamePascal}{Type}Factory.cs")
                });

            var generator = new ClassGenerator();
            var cts = new CancellationTokenSource();
            var callCount = 0;

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await generator.GenerateAsync(
                    request,
                    mapping,
                    _ =>
                    {
                        callCount++;
                        if (callCount == 1)
                        {
                            cts.Cancel();
                        }

                        return "// generated";
                    },
                    cts.Token);
            });

            var generatedFiles = Directory.Exists(outputDir)
                ? Directory.GetFiles(outputDir, "*.cs", SearchOption.TopDirectoryOnly)
                : Array.Empty<string>();

            Assert.True(generatedFiles.Length <= 1, "Generator must stop before writing additional files after cancellation.");
            if (generatedFiles.Length == 1)
            {
                Assert.Contains("OrdersTableFactory.cs", generatedFiles[0], StringComparison.OrdinalIgnoreCase);
            }
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
    /// Ensures snapshot reader falls back to provided reference when metadata is not present.
    /// Garante fallback para referência padrão quando metadados não estão presentes no arquivo.
    /// </summary>
    [Fact]
    public async Task GeneratedClassSnapshotReader_WhenMetadataIsMissing_UsesFallbackReference()
    {
        var file = Path.Combine(Path.GetTempPath(), $"dbsql-snapshot-fallback-{Guid.NewGuid():N}.cs");
        await File.WriteAllTextAsync(file, "public static class Dummy {}", TestContext.Current.CancellationToken);

        try
        {
            var fallback = new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                new Dictionary<string, string> { ["Columns"] = "Id|int|0|0|1||||int|" });

            var snapshot = await GeneratedClassSnapshotReader.ReadAsync(file, fallback, TestContext.Current.CancellationToken);

            Assert.Equal("dbo", snapshot.Reference.Schema);
            Assert.Equal("Orders", snapshot.Reference.Name);
            Assert.Equal(DatabaseObjectType.Table, snapshot.Reference.Type);
            Assert.NotNull(snapshot.Reference.Properties);
            Assert.True(snapshot.Reference.Properties!.ContainsKey("Columns"));
        }
        finally
        {
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }
    }

    /// <summary>
    /// Ensures consistency checker returns synchronized status when local and database properties match.
    /// Garante status sincronizado quando propriedades locais e do banco são equivalentes.
    /// </summary>
    [Fact]
    public async Task ObjectConsistencyChecker_WhenPropertiesMatch_ReturnsSynchronized()
    {
        var properties = new Dictionary<string, string>
        {
            ["Columns"] = "Id|int|0|0|1||||int|",
            ["PrimaryKey"] = "Id"
        };

        var dbObject = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table, properties);
        var local = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
            "c:/classes/Orders.cs",
            new Dictionary<string, string>(properties));

        var checker = new ObjectConsistencyChecker();
        var provider = new QualityMetadataProvider(dbObject);

        var result = await checker.CheckAsync(
            new ConnectionDefinition("1", "SqlServer", "ERP", "conn"),
            local,
            provider,
            TestContext.Current.CancellationToken);

        Assert.Equal(ObjectHealthStatus.Synchronized, result.Status);
        Assert.Null(result.Message);
    }

    private sealed class QualityMetadataProvider(DatabaseObjectReference dbObject) : IDatabaseMetadataProvider
    {
        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(
            ConnectionDefinition connection,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DatabaseObjectReference>>([dbObject]);

        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<DatabaseObjectReference?> GetObjectAsync(
            ConnectionDefinition connection,
            DatabaseObjectReference reference,
            CancellationToken cancellationToken = default)
            => Task.FromResult<DatabaseObjectReference?>(dbObject);
    }
}
