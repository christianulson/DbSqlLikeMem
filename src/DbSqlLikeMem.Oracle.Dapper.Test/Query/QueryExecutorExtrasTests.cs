namespace DbSqlLikeMem.Oracle.Test.Query;

/// <summary>
/// EN: Defines the class QueryExecutorExtrasTests.
/// PT: Define a classe QueryExecutorExtrasTests.
/// </summary>
public sealed class QueryExecutorExtrasTests(
    ITestOutputHelper helper
) : QueryExecutorExtrasTestsBase<OracleDbMock, OracleConnectionMock, OracleCommandMock, OracleQueryProvider, OracleTranslator>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <inheritdoc />
    protected override OracleCommandMock CreateCommand(OracleConnectionMock connection, string sql) => new(connection) { CommandText = sql };

    /// <inheritdoc />
    protected override string PaginationBatchSql => """
SELECT t2.* FROM t t2 ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT * FROM t ORDER BY iddesc ASC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
""";

    /// <inheritdoc />
    protected override object GetTranslatorFromProvider(IQueryProvider provider)
        => typeof(OracleQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(provider)!;

    /// <inheritdoc />
    protected override IQueryable<QueryExecutorFoo> CreateQueryable(OracleConnectionMock connection)
        => connection.AsQueryable<QueryExecutorFoo>();

    /// <inheritdoc />
    protected override string TranslateSql(object translator, Expression expression)
        => ((OracleTranslator)translator).Translate(expression).Sql;
}
