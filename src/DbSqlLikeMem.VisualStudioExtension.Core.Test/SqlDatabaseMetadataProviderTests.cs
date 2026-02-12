namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class SqlDatabaseMetadataProviderTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public async Task GetObjectAsync_ReturnsCompleteStructureMetadata()
    {
        var executor = new FakeSqlQueryExecutor();
        executor.WhenContains("FROM INFORMATION_SCHEMA.TABLES", [
            Row(("SchemaName", "dbo"), ("ObjectName", "Orders"), ("ObjectType", "Table"))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.COLUMNS", [
            Row(("ColumnName", "Id"), ("DataType", "int"), ("Ordinal", 1), ("IsNullable", "NO"), ("Extra", "auto_increment"), ("DefaultValue", null), ("CharMaxLen", null), ("NumScale", null), ("ColumnType", "int"), ("Generated", "")),
            Row(("ColumnName", "CustomerId"), ("DataType", "int"), ("Ordinal", 2), ("IsNullable", "NO"), ("Extra", ""), ("DefaultValue", null), ("CharMaxLen", null), ("NumScale", null), ("ColumnType", "int"), ("Generated", ""))
        ]);
        executor.WhenContains("INDEX_NAME='PRIMARY'", [
            Row(("ColumnName", "Id"))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.STATISTICS", [
            Row(("IndexName", "PRIMARY"), ("NonUnique", 0), ("ColumnName", "Id"), ("Seq", 1)),
            Row(("IndexName", "IX_Orders_CustomerId"), ("NonUnique", 1), ("ColumnName", "CustomerId"), ("Seq", 1))
        ]);
        executor.WhenContains("FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE", [
            Row(("ColumnName", "CustomerId"), ("RefTable", "Customers"), ("RefColumn", "Id"))
        ]);

        var provider = new SqlDatabaseMetadataProvider(executor);
        var conn = new ConnectionDefinition("1", "MySql", "ERP", "conn");
        var reference = new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table);

        var result = await provider.GetObjectAsync(conn, reference, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Contains("Id|int|1|0|1", result!.Properties!["Columns"]);
        Assert.Equal("Id", result.Properties["PrimaryKey"]);
        Assert.DoesNotContain("PRIMARY", result.Properties["Indexes"], StringComparison.OrdinalIgnoreCase);
        Assert.Contains("IX_Orders_CustomerId|0|CustomerId", result.Properties["Indexes"]);
        Assert.Equal("CustomerId|Customers|Id", result.Properties["ForeignKeys"]);
    }

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
    public void QueryFactory_SupportsConfiguredDatabases(string databaseType)
    {
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildListObjectsQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildObjectColumnsQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildPrimaryKeyQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildIndexesQuery(databaseType)));
        Assert.False(string.IsNullOrWhiteSpace(SqlMetadataQueryFactory.BuildForeignKeysQuery(databaseType)));
    }

    private static IReadOnlyDictionary<string, object?> Row(params (string Key, object? Value)[] items)
        => items.ToDictionary(x => x.Key, x => x.Value);

    private sealed class FakeSqlQueryExecutor : ISqlQueryExecutor
    {
        private readonly List<(string Contains, IReadOnlyCollection<IReadOnlyDictionary<string, object?>> Rows)> _responses = [];

        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public void WhenContains(string containsSql, IReadOnlyCollection<IReadOnlyDictionary<string, object?>> rows)
            => _responses.Add((containsSql, rows));

        /// <summary>
        /// Executes this API operation.
        /// Executa esta operação da API.
        /// </summary>
        public Task<IReadOnlyCollection<IReadOnlyDictionary<string, object?>>> QueryAsync(
            ConnectionDefinition connection,
            string sql,
            IReadOnlyDictionary<string, object?> parameters,
            CancellationToken cancellationToken = default)
        {
            var hit = _responses.FirstOrDefault(x => sql.Contains(x.Contains, StringComparison.OrdinalIgnoreCase));
            return Task.FromResult(hit.Rows ?? Array.Empty<IReadOnlyDictionary<string, object?>>());
        }
    }
}
