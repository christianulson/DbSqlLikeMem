namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for DB2 and keeps DB2-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para DB2 e mantém cobertura específica de DB2.
/// </summary>
public sealed class Db2AggregationTests : AggregationHavingOrdinalTestsBase<Db2DbMock, Db2ConnectionMock>
{
    /// <summary>
    /// EN: Initializes DB2 aggregation tests.
    /// PT: Inicializa os testes de agregação do DB2.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public Db2AggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override Db2DbMock CreateDb() => new();

    /// <inheritdoc />
    protected override Db2ConnectionMock CreateConnection(Db2DbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Aggregation")]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        AssertDistinctOrderPagination("LIMIT 1 OFFSET 1");
    }
}
