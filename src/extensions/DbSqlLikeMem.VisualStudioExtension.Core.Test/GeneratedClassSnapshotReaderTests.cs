namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies generated artifact metadata can be read back from snapshot headers.
/// PT: Verifica se os metadados dos artefatos gerados podem ser lidos novamente dos headers de snapshot.
/// </summary>
public sealed class GeneratedClassSnapshotReaderTests
{
    /// <summary>
    /// EN: Verifies snapshot metadata survives a round trip into the consistency checker.
    /// PT: Verifica se os metadados do snapshot sobrevivem a um round trip com o verificador de consistencia.
    /// </summary>
    [Fact]
    [Trait("Category", "GeneratedClassSnapshotReader")]
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
// DBSqlLikeMem:Triggers=trg_orders_audit
// DBSqlLikeMem:RequiredIn=CustomerId|Int32|1|
// DBSqlLikeMem:Parameters=CustomerId|int|1|0|0|0|
// DBSqlLikeMem:ReturnTypeSql=int
// DBSqlLikeMem:BodySql=CustomerId
/// <summary>
/// EN: Represents the generated orders table factory used by the snapshot test.
/// PT: Representa a factory gerada da tabela Orders usada pelo teste de snapshot.
/// </summary>
public static class OrdersTableFactory {}
""", TestContext.Current.CancellationToken);

        try
        {
            var fallback = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table, "public");
            var snapshot = await GeneratedClassSnapshotReader.ReadAsync(file, fallback, TestContext.Current.CancellationToken);
            snapshot.Reference.Properties!["Triggers"].Should().Be("trg_orders_audit");
            snapshot.Reference.Properties!["RequiredIn"].Should().Be("CustomerId|Int32|1|");
            snapshot.Reference.Properties!["Parameters"].Should().Be("CustomerId|int|1|0|0|0|");

            var dbObject = new DatabaseObjectReference(
                "dbo",
                "Orders",
                DatabaseObjectType.Table,
                "public",
                new Dictionary<string, string>
                {
                    ["Columns"] = "Id|int|0|0|1||||int|;Name|varchar|1|1|0||||varchar|;Status|tinyint|2|1|0||||tinyint|",
                    ["PrimaryKey"] = "Id",
                    ["Triggers"] = "trg_orders_audit"
                });

            var provider = new SnapshotProvider(dbObject);
            var checker = new ObjectConsistencyChecker();
            var result = await checker.CheckAsync(new ConnectionDefinition("1", "MySql", "ERP", "conn"), snapshot, provider, TestContext.Current.CancellationToken);

            result.Status.Should().Be(ObjectHealthStatus.DifferentFromDatabase);
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
        /// EN: Returns the single in-memory object used by the test snapshot provider.
        /// PT: Retorna o unico objeto em memoria usado pelo provider de snapshot do teste.
        /// </summary>
        public Task<IReadOnlyCollection<DatabaseObjectReference>> ListObjectsAsync(ConnectionDefinition connection,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DatabaseObjectReference>>([dbObject]);

        /// <summary>
        /// EN: Returns the in-memory object regardless of the requested reference.
        /// PT: Retorna o objeto em memoria independentemente da referencia solicitada.
        /// </summary>
        public Task<DatabaseObjectReference?> GetObjectAsync(ConnectionDefinition connection, DatabaseObjectReference reference,
            CancellationToken cancellationToken = default)
            => Task.FromResult<DatabaseObjectReference?>(dbObject);
    }
}
