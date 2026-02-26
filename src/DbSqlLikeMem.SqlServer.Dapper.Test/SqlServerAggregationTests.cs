namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for SQL Server and keeps SQL Server-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para SQL Server e mantém cobertura específica de SQL Server.
/// </summary>
public sealed class SqlServerAggregationTests : AggregationHavingOrdinalTestsBase<SqlServerDbMock, SqlServerConnectionMock>
{
    /// <summary>
    /// EN: Initializes SQL Server aggregation tests.
    /// PT: Inicializa os testes de agregação do SQL Server.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public SqlServerAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override SqlServerDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_WithPagination_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_WithPagination_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerAggregation")]
    public void Distinct_Order_WithPagination_ShouldWork()
    {
        AssertDistinctOrderPagination("OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");
    }
}
