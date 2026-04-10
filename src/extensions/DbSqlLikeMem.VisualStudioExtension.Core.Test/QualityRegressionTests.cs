namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Regression tests focused on quality and performance-sensitive flows.
/// PT: Testes de regressao focados em fluxos sensiveis de qualidade e performance.
/// </summary>
public sealed class QualityRegressionTests
{
    /// <summary>
    /// EN: Ensures the shared UI database type catalog stays aligned with the core normalizer.
    /// PT: Garante que o catalogo compartilhado de tipos de banco da UI permaneça alinhado com o normalizador do core.
    /// </summary>
    [Fact]
    [Trait("Category", "QualityRegression")]
    public void DatabaseTypeNormalizer_ShouldExposeSupportedDisplayNames()
    {
        var supported = DatabaseTypeNormalizer.GetSupportedDisplayNames();

        Assert.Equal(
            ["SqlServer", "SqlAzure", "AzureSql", "PostgreSql", "MySql", "MariaDb", "Oracle", "Sqlite", "Db2", "Firebird"],
            supported);
    }

    /// <summary>
    /// EN: Ensures database object type labels stay canonical across the UI and tree builders.
    /// PT: Garante que os rótulos de tipos de objeto de banco permaneçam canônicos na UI e nos construtores de árvore.
    /// </summary>
    [Theory]
    [Trait("Category", "QualityRegression")]
    [InlineData(DatabaseObjectType.Table, "Tables")]
    [InlineData(DatabaseObjectType.View, "Views")]
    [InlineData(DatabaseObjectType.Procedure, "Procedures")]
    [InlineData(DatabaseObjectType.Function, "Functions")]
    [InlineData(DatabaseObjectType.Sequence, "Sequences")]
    public void DatabaseObjectTypeLabels_ShouldReturnCanonicalPluralLabels(DatabaseObjectType objectType, string expectedLabel)
    {
        Assert.Equal(expectedLabel, DatabaseObjectTypeLabels.GetGroupLabel(objectType));
    }

    /// <summary>
    /// EN: Ensures database type normalization keeps canonical labels and lookup keys aligned.
    /// PT: Garante que a normalizacao de tipo de banco mantenha rotulos canônicos e chaves de consulta alinhados.
    /// </summary>
    [Theory]
    [Trait("Category", "QualityRegression")]
    [InlineData(" sql-server ", "SqlServer", "sqlserver")]
    [InlineData("Azure-SQL", "SqlAzure", "azuresql")]
    [InlineData("DB2/LUW", "Db2", "db2luw")]
    [InlineData("PgSQL", "PostgreSql", "pgsql")]
    public void DatabaseTypeNormalizer_ShouldHandleLegacyAliases(string input, string expectedDisplayName, string expectedKey)
    {
        Assert.Equal(expectedDisplayName, DatabaseTypeNormalizer.NormalizeDisplayName(input));
        Assert.Equal(expectedKey, DatabaseTypeNormalizer.NormalizeKey(input));
    }

    /// <summary>
    /// EN: Ensures legacy database type aliases are normalized in the connection definition constructor.
    /// PT: Garante que aliases legados de tipo de banco sejam normalizados no construtor da definicao de conexao.
    /// </summary>
    [Theory]
    [Trait("Category", "QualityRegression")]
    [InlineData("mssql", "SqlServer")]
    [InlineData("azure-sql", "AzureSql")]
    [InlineData("pgsql", "PostgreSql")]
    [InlineData("db2/luw", "Db2")]
    [InlineData("firebirdsql", "Firebird")]
    public void ConnectionDefinition_WhenCreated_WithLegacyAliases_NormalizesDatabaseType(string databaseType, string expectedDatabaseType)
    {
        var connection = new ConnectionDefinition("1", databaseType, "ERP", "conn");

        Assert.Equal(expectedDatabaseType, connection.DatabaseType);
    }

    /// <summary>
    /// EN: Ensures generation honors cancellation and does not continue writing files.
    /// PT: Garante que a geracao respeita cancelamento e nao continua gravando arquivos.
    /// </summary>
    [Fact]
    [Trait("Category", "QualityRegression")]
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
                : [];

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
    /// EN: Ensures snapshot reader falls back to the provided reference when metadata is not present.
    /// PT: Garante fallback para a referencia fornecida quando os metadados nao estao presentes no arquivo.
    /// </summary>
    [Fact]
    [Trait("Category", "QualityRegression")]
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
    /// EN: Ensures consistency checker returns synchronized status when local and database properties match.
    /// PT: Garante que o verificador de consistencia retorne status sincronizado quando as propriedades locais e do banco coincidem.
    /// </summary>
    [Fact]
    [Trait("Category", "QualityRegression")]
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
        /// EN: Returns the single in-memory object used by the regression test provider.
        /// PT: Retorna o unico objeto em memoria usado pelo provider do teste de regressao.
        /// </summary>
        public Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(
            ConnectionDefinition connection,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DatabaseObjectReference>>([dbObject]);

        /// <summary>
        /// EN: Returns the same in-memory object for every metadata lookup.
        /// PT: Retorna o mesmo objeto em memoria para toda consulta de metadados.
        /// </summary>
        public Task<DatabaseObjectReference?> GetObjectAsync(
            ConnectionDefinition connection,
            DatabaseObjectReference reference,
            CancellationToken cancellationToken = default)
            => Task.FromResult<DatabaseObjectReference?>(dbObject);
    }
}
