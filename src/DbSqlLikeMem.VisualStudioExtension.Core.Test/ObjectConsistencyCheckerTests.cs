namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public class ObjectConsistencyCheckerTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public async Task CheckAsync_WhenObjectMissing_ReturnsMissingStatus()
    {
        var checker = new ObjectConsistencyChecker();
        var provider = new FakeMetadataProvider(null);
        var connection = new ConnectionDefinition("1", "SqlServer", "ERP", "conn");
        var local = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
            "c:/classes/Orders.cs");

        var result = await checker.CheckAsync(connection, local, provider, TestContext.Current.CancellationToken);

        Assert.Equal(ObjectHealthStatus.MissingInDatabase, result.Status);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public async Task CheckAsync_WhenPropertiesDifferent_ReturnsDifferentStatus()
    {
        var checker = new ObjectConsistencyChecker();
        var provider = new FakeMetadataProvider(new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table,
            new Dictionary<string, string> { ["Columns"] = "Id,Name" }));

        var connection = new ConnectionDefinition("1", "SqlServer", "ERP", "conn");
        var local = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
            "c:/classes/Orders.cs",
            new Dictionary<string, string> { ["Columns"] = "Id,Name,Status" });

        var result = await checker.CheckAsync(connection, local, provider, TestContext.Current.CancellationToken);

        Assert.Equal(ObjectHealthStatus.DifferentFromDatabase, result.Status);
    }

    /// <summary>
    /// EN: Ensures missing local artifacts are classified as missing when none of the expected files exist.
    /// PT: Garante que artefatos locais ausentes sejam classificados como ausentes quando nenhum dos arquivos esperados existe.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void EvaluateLocalArtifacts_WhenNothingExists_ReturnsMissingLocalArtifacts()
    {
        var checker = new ObjectConsistencyChecker();
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = checker.EvaluateLocalArtifacts(reference, "c:/classes/OrdersTests.cs", false, false, false, "missing");

        Assert.NotNull(result);
        Assert.Equal(ObjectHealthStatus.MissingLocalArtifacts, result!.Status);
    }

    /// <summary>
    /// EN: Ensures partially generated local artifacts are classified separately from metadata divergence.
    /// PT: Garante que artefatos locais gerados parcialmente sejam classificados separadamente da divergencia de metadados.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void EvaluateLocalArtifacts_WhenOnlyPartOfTheTrioExists_ReturnsIncompleteLocalArtifacts()
    {
        var checker = new ObjectConsistencyChecker();
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = checker.EvaluateLocalArtifacts(reference, "c:/classes/OrdersTests.cs", true, false, true, "partial");

        Assert.NotNull(result);
        Assert.Equal(ObjectHealthStatus.IncompleteLocalArtifacts, result!.Status);
    }

    /// <summary>
    /// EN: Ensures missing artifact kinds are exposed in deterministic class-model-repository order for UI diagnostics.
    /// PT: Garante que os tipos de artefato ausentes sejam expostos na ordem deterministica classe-modelo-repositorio para diagnosticos da UI.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void GetMissingArtifactKinds_WhenArtifactsAreMissing_ReturnsDeterministicOrder()
    {
        var checker = new ObjectConsistencyChecker();

        var missingKinds = checker.GetMissingArtifactKinds(hasPrimaryClass: false, hasModel: true, hasRepository: false);

        Assert.Equal(["class", "repository"], missingKinds);
    }

    /// <summary>
    /// EN: Ensures drifted artifact kinds are exposed in deterministic class-model-repository order for UI diagnostics.
    /// PT: Garante que os tipos de artefato divergentes sejam expostos na ordem deterministica classe-modelo-repositorio para diagnosticos da UI.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void GetDriftedArtifactKinds_WhenSnapshotsPointToDifferentObjects_ReturnsDeterministicOrder()
    {
        var checker = new ObjectConsistencyChecker();
        var expected = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);
        var classSnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "OrdersArchive", DatabaseObjectType.Table),
            "c:/classes/OrdersTests.cs");
        var modelSnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "OrdersArchive", DatabaseObjectType.Table),
            "c:/models/OrdersModel.cs");
        var repositorySnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
            "c:/repositories/OrdersRepository.cs");

        var driftedKinds = checker.GetDriftedArtifactKinds(expected, classSnapshot, modelSnapshot, repositorySnapshot);

        Assert.Equal(["class", "model"], driftedKinds);
    }

    /// <summary>
    /// EN: Ensures companion artifacts are flagged as drifted when their stored structural snapshot differs from the primary generated class.
    /// PT: Garante que artefatos complementares sejam marcados como divergentes quando o snapshot estrutural salvo difere da classe gerada principal.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void GetDriftedArtifactKinds_WhenCompanionSnapshotPropertiesDiffer_ReturnsArtifactKind()
    {
        var checker = new ObjectConsistencyChecker();
        var expected = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);
        var primarySnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                new Dictionary<string, string>
                {
                    ["Columns"] = "Id|int|0|0",
                    ["Triggers"] = "trg_orders_audit"
                }),
            "c:/classes/OrdersTests.cs",
            new Dictionary<string, string>
            {
                ["Columns"] = "Id|int|0|0",
                ["Triggers"] = "trg_orders_audit"
            });
        var modelSnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                new Dictionary<string, string>
                {
                    ["Columns"] = "Id|int|0|0",
                    ["Triggers"] = "trg_orders_audit"
                }),
            "c:/models/OrdersModel.cs",
            new Dictionary<string, string>
            {
                ["Columns"] = "Id|int|0|0",
                ["Triggers"] = "trg_orders_audit"
            });
        var repositorySnapshot = new LocalObjectSnapshot(
            new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                new Dictionary<string, string>
                {
                    ["Columns"] = "Id|int|0|0;Status|tinyint|1|1",
                    ["Triggers"] = "trg_orders_audit"
                }),
            "c:/repositories/OrdersRepository.cs",
            new Dictionary<string, string>
            {
                ["Columns"] = "Id|int|0|0;Status|tinyint|1|1",
                ["Triggers"] = "trg_orders_audit"
            });

        var driftedKinds = checker.GetDriftedArtifactKinds(expected, primarySnapshot, modelSnapshot, repositorySnapshot);

        Assert.Equal(["repository"], driftedKinds);
    }

    /// <summary>
    /// EN: Ensures snapshot drift is classified as metadata divergence once the local artifact trio exists.
    /// PT: Garante que drift de snapshot seja classificado como divergencia de metadados quando o trio local de artefatos existe.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void EvaluateArtifactDrift_WhenArtifactsPointToAnotherObject_ReturnsDifferentFromDatabase()
    {
        var checker = new ObjectConsistencyChecker();
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = checker.EvaluateArtifactDrift(reference, "c:/classes/OrdersTests.cs", ["model"], "drift");

        Assert.NotNull(result);
        Assert.Equal(ObjectHealthStatus.DifferentFromDatabase, result!.Status);
        Assert.Equal("drift", result.Message);
    }

    /// <summary>
    /// EN: Ensures metadata comparison continues only after the expected local artifact trio exists.
    /// PT: Garante que a comparacao de metadados continue apenas depois que o trio esperado de artefatos locais existir.
    /// </summary>
    [Fact]
    [Trait("Category", "ObjectConsistencyChecker")]
    public void EvaluateLocalArtifacts_WhenAllExpectedFilesExist_ReturnsNull()
    {
        var checker = new ObjectConsistencyChecker();
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = checker.EvaluateLocalArtifacts(reference, "c:/classes/OrdersTests.cs", true, true, true, "ok");

        Assert.Null(result);
    }

    private sealed class FakeMetadataProvider(DatabaseObjectReference? dbObject) : IDatabaseMetadataProvider
    {
        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<DatabaseObjectReference?> GetObjectAsync(ConnectionDefinition connection, DatabaseObjectReference reference,
            CancellationToken cancellationToken = default)
            => Task.FromResult(dbObject);

        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(ConnectionDefinition connection,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DatabaseObjectReference>>(dbObject is null ? [] : [dbObject]);
    }
}
