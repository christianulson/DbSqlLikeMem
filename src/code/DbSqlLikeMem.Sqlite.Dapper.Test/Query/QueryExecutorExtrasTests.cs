namespace DbSqlLikeMem.Sqlite.Dapper.Test.Query;

/// <summary>
/// EN: Covers SQLite query-executor extras against the Dapper provider.
/// PT-br: Cobre extras do executor de consultas SQLite contra o provedor Dapper.
/// </summary>
public sealed class QueryExecutorExtrasTests(
    ITestOutputHelper helper
) : QueryExecutorExtrasTestsBase<SqliteDbMock, SqliteConnectionMock, SqliteCommandMock, SqliteQueryProvider, SqliteTranslator>(helper)
{
    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);

    /// <inheritdoc />
    protected override SqliteCommandMock CreateCommand(SqliteConnectionMock connection, string sql) => new(connection) { CommandText = sql };

    /// <inheritdoc />
    protected override string PaginationBatchSql => """
SELECT t2.* FROM t t2 ORDER BY id DESC LIMIT 2 OFFSET 1;
SELECT * FROM t ORDER BY iddesc ASC LIMIT 2 OFFSET 1;
""";

    /// <inheritdoc />
    protected override object GetTranslatorFromProvider(IQueryProvider provider)
        => typeof(SqliteQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(provider)!;

    /// <inheritdoc />
    protected override IQueryable<QueryExecutorFoo> CreateQueryable(SqliteConnectionMock connection)
        => connection.AsQueryable<QueryExecutorFoo>();

    /// <inheritdoc />
    protected override string TranslateSql(object translator, Expression expression)
        => ((SqliteTranslator)translator).Translate(expression).Sql;

    /// <summary>
    /// EN: Verifies MATCH is emitted when SqlFunctions.Match is used in WHERE.
    /// PT-br: Verifica se MATCH é emitido quando SqlFunctions.Match é usado no WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateMatchSqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn).Where(f => SqlFunctions.Match(f.Y, "search"));
        var sql = TranslateSql(GetTranslatorFromProvider(q.Provider), q.Expression);
        sql.Should().Contain("Y MATCH @p0");
    }
}
