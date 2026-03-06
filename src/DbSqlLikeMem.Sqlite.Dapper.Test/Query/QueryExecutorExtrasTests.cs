namespace DbSqlLikeMem.Sqlite.Dapper.Test.Query;

/// <summary>
/// EN: Defines the class QueryExecutorExtrasTests.
/// PT: Define a classe QueryExecutorExtrasTests.
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
}
