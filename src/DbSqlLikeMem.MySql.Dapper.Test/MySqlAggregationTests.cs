namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for MySQL and keeps MySQL-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para MySQL e mantém cobertura específica de MySQL.
/// </summary>
public sealed class MySqlAggregationTests : AggregationHavingOrdinalTestsBase<MySqlDbMock, MySqlConnectionMock>
{
    /// <summary>
    /// EN: Initializes MySQL aggregation tests.
    /// PT: Inicializa os testes de agregação do MySQL.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public MySqlAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override MySqlDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAggregation")]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        AssertDistinctOrderPagination("LIMIT 1 OFFSET 1");
    }
}
