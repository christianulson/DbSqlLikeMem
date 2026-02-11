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
    public async Task CheckAsync_WhenObjectMissing_ReturnsMissingStatus()
    {
        var checker = new ObjectConsistencyChecker();
        var provider = new FakeMetadataProvider(null);
        var connection = new ConnectionDefinition("1", "SqlServer", "ERP", "conn");
        var local = new LocalObjectSnapshot(
            new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
            "c:/classes/Orders.cs");

        var result = await checker.CheckAsync(connection, local, provider);

        Assert.Equal(ObjectHealthStatus.MissingInDatabase, result.Status);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
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

        var result = await checker.CheckAsync(connection, local, provider);

        Assert.Equal(ObjectHealthStatus.DifferentFromDatabase, result.Status);
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
