namespace DbSqlLikeMem.Npgsql.Test.Query;

/// <summary>
/// EN: Defines the class QueryExecutorExtrasTests.
/// PT: Define a classe QueryExecutorExtrasTests.
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
SELECT t2.* FROM t t2 ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT * FROM t ORDER BY iddesc ASC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
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
}
