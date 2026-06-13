namespace DbSqlLikeMem.MySql.Dapper.Test.Query;

/// <summary>
/// EN: Covers MySQL query-executor extras against the Dapper provider.
/// PT-br: Cobre extras do executor de consultas MySQL contra o provedor Dapper.
/// </summary>
public sealed class QueryExecutorExtrasTests(
    ITestOutputHelper helper
) : QueryExecutorExtrasTestsBase<MySqlDbMock, MySqlConnectionMock, MySqlCommandMock, MySqlQueryProvider, MySqlTranslator>(helper)
{
    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db) => new(db);

    /// <inheritdoc />
    protected override MySqlCommandMock CreateCommand(MySqlConnectionMock connection, string sql) => new(connection) { CommandText = sql };

    /// <inheritdoc />
    protected override string PaginationBatchSql => """
SELECT t2.* FROM t t2 ORDER BY id DESC LIMIT 2 OFFSET 1;
SELECT * FROM t ORDER BY iddesc ASC LIMIT 2 OFFSET 1;
""";

    /// <inheritdoc />
    protected override object GetTranslatorFromProvider(IQueryProvider provider)
        => typeof(MySqlQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(provider)!;

    /// <inheritdoc />
    protected override IQueryable<QueryExecutorFoo> CreateQueryable(MySqlConnectionMock connection)
        => connection.AsQueryable<QueryExecutorFoo>();

    /// <inheritdoc />
    protected override string TranslateSql(object translator, Expression expression)
        => ((MySqlTranslator)translator).Translate(expression).Sql;

    /// <summary>
    /// EN: Verifies MATCH...AGAINST is emitted when SqlFunctions.MatchAgainst is used in WHERE.
    /// PT-br: Verifica se MATCH...AGAINST é emitido quando SqlFunctions.MatchAgainst é usado no WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateMatchAgainstSqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn).Where(f => SqlFunctions.MatchAgainst(f.Y, "search") > 0);
        var sql = TranslateSql(GetTranslatorFromProvider(q.Provider), q.Expression);
        sql.Should().Contain("MATCH(Y) AGAINST(@p0)");
    }
}
