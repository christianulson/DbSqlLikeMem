namespace DbSqlLikeMem.Npgsql.Test.Query;

/// <summary>
/// EN: Covers PostgreSQL query-executor extras against the Dapper provider.
/// PT-br: Cobre extras do executor de consultas PostgreSQL contra o provedor Dapper.
/// </summary>
public sealed class QueryExecutorExtrasTests(
    ITestOutputHelper helper
) : QueryExecutorExtrasTestsBase<NpgsqlDbMock, NpgsqlConnectionMock, NpgsqlCommandMock, NpgsqlQueryProvider, NpgsqlTranslator>(helper)
{
    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db) => new(db);

    /// <inheritdoc />
    protected override NpgsqlCommandMock CreateCommand(NpgsqlConnectionMock connection, string sql) => new(connection) { CommandText = sql };

    /// <inheritdoc />
    protected override string PaginationBatchSql => """
SELECT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT id FROM t ORDER BY iddesc ASC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
""";

    /// <inheritdoc />
    protected override object GetTranslatorFromProvider(IQueryProvider provider)
        => typeof(NpgsqlQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(provider)!;

    /// <inheritdoc />
    protected override IQueryable<QueryExecutorFoo> CreateQueryable(NpgsqlConnectionMock connection)
        => connection.AsQueryable<QueryExecutorFoo>();

    /// <inheritdoc />
    protected override string TranslateSql(object translator, Expression expression)
        => ((NpgsqlTranslator)translator).Translate(expression).Sql;

    /// <summary>
    /// EN: Verifies to_tsvector/to_tsquery with @@ is emitted when SqlFunctions.TsQuery is used in WHERE.
    /// PT-br: Verifica se to_tsvector/to_tsquery com @@ é emitido quando SqlFunctions.TsQuery é usado no WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Query")]
    public void TranslateTsQuerySqlCorrect()
    {
        using var cnn = CreateConnection(CreateDb());
        var q = CreateQueryable(cnn).Where(f => SqlFunctions.TsQuery(f.Y, "search"));
        var sql = TranslateSql(GetTranslatorFromProvider(q.Provider), q.Expression);
        sql.Should().Contain("to_tsvector(Y) @@ to_tsquery(@p0)");
    }
}
