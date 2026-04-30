namespace DbSqlLikeMem.MySql.Dapper.Test.Query;

/// <summary>
/// EN: Covers MySQL query-executor extras against the Dapper provider.
/// PT: Cobre extras do executor de consultas MySQL contra o provedor Dapper.
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
}
