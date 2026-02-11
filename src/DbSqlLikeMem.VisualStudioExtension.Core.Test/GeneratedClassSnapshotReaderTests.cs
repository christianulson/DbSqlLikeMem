namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class GeneratedClassSnapshotReaderTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ParsesMetadataAndCheckerDetectsDifference()
    {
        var file = Path.Combine(Path.GetTempPath(), $"dbsql-snapshot-{Guid.NewGuid():N}.cs");
        await File.WriteAllTextAsync(file, """
// DBSqlLikeMem:Schema=dbo
// DBSqlLikeMem:Object=Orders
// DBSqlLikeMem:Type=Table
// DBSqlLikeMem:Columns=Id|int|0|0|1||||int|;Name|varchar|1|1|0||||varchar|
// DBSqlLikeMem:PrimaryKey=Id
// DBSqlLikeMem:Indexes=IX_Orders_Name|0|Name
// DBSqlLikeMem:ForeignKeys=CustomerId|Customers|Id
/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public static class OrdersTableFactory {}
""");

        try
        {
            var fallback = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);
            var snapshot = await GeneratedClassSnapshotReader.ReadAsync(file, fallback);

            var dbObject = new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                new Dictionary<string, string>
                {
                    ["Columns"] = "Id|int|0|0|1||||int|;Name|varchar|1|1|0||||varchar|;Status|tinyint|2|1|0||||tinyint|",
                    ["PrimaryKey"] = "Id"
                });

            var provider = new SnapshotProvider(dbObject);
            var checker = new ObjectConsistencyChecker();
            var result = await checker.CheckAsync(
                new ConnectionDefinition("1", "MySql", "ERP", "conn"),
                snapshot,
                provider);

            Assert.Equal(ObjectHealthStatus.DifferentFromDatabase, result.Status);
        }
        finally
        {
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }
    }

    private sealed class SnapshotProvider(DatabaseObjectReference dbObject) : IDatabaseMetadataProvider
    {
        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(ConnectionDefinition connection,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DatabaseObjectReference>>([dbObject]);

        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<DatabaseObjectReference?> GetObjectAsync(ConnectionDefinition connection, DatabaseObjectReference reference,
            CancellationToken cancellationToken = default)
            => Task.FromResult<DatabaseObjectReference?>(dbObject);
    }
}
