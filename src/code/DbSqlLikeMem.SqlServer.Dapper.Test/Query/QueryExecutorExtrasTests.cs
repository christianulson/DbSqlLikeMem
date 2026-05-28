namespace DbSqlLikeMem.SqlServer.Dapper.Test.Query;

/// <summary>
/// EN: Covers SQL Server query-executor extras against the Dapper provider.
/// PT-br: Cobre extras do executor de consultas SQL Server contra o provedor Dapper.
/// </summary>
public sealed class QueryExecutorExtrasTests(
    ITestOutputHelper helper
) : QueryExecutorExtrasTestsBase<SqlServerDbMock, SqlServerConnectionMock, SqlServerCommandMock, SqlServerQueryProvider, SqlServerTranslator>(helper)
{
    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);

    /// <inheritdoc />
    protected override SqlServerCommandMock CreateCommand(SqlServerConnectionMock connection, string sql) => new(connection) { CommandText = sql };

    /// <inheritdoc />
    protected override string PaginationBatchSql => """
SELECT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT id FROM t ORDER BY iddesc ASC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
""";

    /// <inheritdoc />
    protected override object GetTranslatorFromProvider(IQueryProvider provider)
        => typeof(SqlServerQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(provider)!;

    /// <inheritdoc />
    protected override IQueryable<QueryExecutorFoo> CreateQueryable(SqlServerConnectionMock connection)
        => connection.AsQueryable<QueryExecutorFoo>();

    /// <inheritdoc />
    protected override string TranslateSql(object translator, Expression expression)
        => ((SqlServerTranslator)translator).Translate(expression).Sql;

    /// <summary>
    /// EN: Verifies CONTAINS is emitted when SqlFunctions.Contains is used in WHERE.
    /// PT-br: Verifica se CONTAINS é emitido quando SqlFunctions.Contains é usado no WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateContainsSqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn).Where(f => SqlFunctions.Contains(f.Y, "search") > 0);
        var sql = TranslateSql(GetTranslatorFromProvider(q.Provider), q.Expression);
        sql.Should().Contain("CONTAINS(Y, @p0)");
    }

    /// <summary>
    /// EN: Verifies FREETEXT is emitted when SqlFunctions.FreeText is used in WHERE.
    /// PT-br: Verifica se FREETEXT é emitido quando SqlFunctions.FreeText é usado no WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateFreeTextSqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn).Where(f => SqlFunctions.FreeText(f.Y, "search") > 0);
        var sql = TranslateSql(GetTranslatorFromProvider(q.Provider), q.Expression);
        sql.Should().Contain("FREETEXT(Y, @p0)");
    }
}
