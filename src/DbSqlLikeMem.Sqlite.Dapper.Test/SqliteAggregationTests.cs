namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for SQLite and keeps SQLite-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para SQLite e mantém cobertura específica de SQLite.
/// </summary>
public sealed class SqliteAggregationTests : AggregationHavingOrdinalTestsBase<SqliteDbMock, SqliteConnectionMock>
{
    /// <summary>
    /// EN: Initializes SQLite aggregation tests.
    /// PT: Inicializa os testes de agregação do SQLite.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public SqliteAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override SqliteDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteAggregation")]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        AssertDistinctOrderPagination("LIMIT 1 OFFSET 1");
    }
}
