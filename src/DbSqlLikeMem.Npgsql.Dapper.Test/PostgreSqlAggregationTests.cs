namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Runs shared aggregation/HAVING scenarios for PostgreSQL and keeps PostgreSQL-specific coverage.
/// PT: Executa cenários compartilhados de agregação/HAVING para PostgreSQL e mantém cobertura específica de PostgreSQL.
/// </summary>
public sealed class PostgreSqlAggregationTests : AggregationHavingOrdinalTestsBase<NpgsqlDbMock, NpgsqlConnectionMock>
{
    /// <summary>
    /// EN: Initializes PostgreSQL aggregation tests.
    /// PT: Inicializa os testes de agregação do PostgreSQL.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    public PostgreSqlAggregationTests(ITestOutputHelper helper) : base(helper)
    {
    }

    /// <inheritdoc />
    protected override NpgsqlDbMock CreateDb() => new();

    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Tests Distinct_Order_WithPagination_ShouldWork behavior.
    /// PT: Testa o comportamento de Distinct_Order_WithPagination_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlAggregation")]
    public void Distinct_Order_WithPagination_ShouldWork()
    {
        AssertDistinctOrderPagination("LIMIT 1 OFFSET 1");
    }
}
