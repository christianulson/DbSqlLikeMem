namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for Oracle and keeps Oracle-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para Oracle e mantém cobertura específica de Oracle.
/// </summary>
public sealed class OracleAggregationTests : AggregationHavingOrdinalTestsBase<OracleDbMock, OracleConnectionMock>
{
    /// <summary>
    /// EN: Initializes Oracle aggregation tests.
    /// PT: Inicializa os testes de agregação do Oracle.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public OracleAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override OracleDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_Limit_Offset_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_Limit_Offset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleAggregation")]
    public void Distinct_Order_Limit_Offset_ShouldWork()
    {
        AssertDistinctOrderPagination("OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY");
    }
}
