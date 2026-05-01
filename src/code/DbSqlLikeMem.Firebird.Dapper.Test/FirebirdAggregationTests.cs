namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird aggregation and HAVING scenarios against the Firebird Dapper mock provider.
/// PT-br: Cobre cenarios de agregacao e HAVING do Firebird contra o provedor mock Dapper do Firebird.
/// </summary>
public sealed class FirebirdAggregationTests(
    ITestOutputHelper helper
) : AggregationHavingOrdinalTestsBase<FirebirdDbMock, FirebirdConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override FirebirdDbMock CreateDb() => [];

    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateConnection(FirebirdDbMock db) => new(db);

    /// <inheritdoc />
    protected override List<dynamic> Query(string sql) => Connection.Query<dynamic>(sql).ToList();

    /// <summary>
    /// EN: Verifies grouped COUNT and SUM aggregation return the expected totals.
    /// PT-br: Verifica se a contagem agrupada e a soma agregada retornam os totais esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdAggregation")]
    public void GroupBy_WithCountAndSum_ShouldWork_Test() => base.GroupBy_WithCountAndSum_ShouldWork();

    /// <summary>
    /// EN: Verifies HAVING filters out grouped rows that do not meet the aggregate threshold.
    /// PT-br: Verifica se HAVING filtra linhas agrupadas que nao atingem o limite da agregacao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdAggregation")]
    public void Having_ShouldFilterAggregates_Test() => base.Having_ShouldFilterAggregates();

    /// <summary>
    /// EN: Ensures HAVING aggregate alias can be combined with ORDER BY ordinal in grouped execution.
    /// PT-br: Garante que alias de agregação no HAVING possa ser combinado com ORDER BY ordinal na execução agrupada.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdAggregation")]
    public void Having_AggregateAlias_WithOrderByOrdinal_ShouldWork_Test() => base.Having_AggregateAlias_WithOrderByOrdinal_ShouldWork();

    /// <summary>
    /// EN: Verifies string aggregation with a custom separator returns the expected rows.
    /// PT-br: Verifica se a agregacao textual com separador customizado retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdAggregation")]
    public void StringAggregation_WithCustomSeparator_ShouldWork()
        => AssertStringAggregationWithCustomSeparator("""
SELECT userId, LIST(amount, '|') AS joined
FROM orders
GROUP BY userId
ORDER BY userId
""");

    /// <summary>
    /// EN: Verifies string aggregation with DISTINCT ignores NULL values and deduplicates text.
    /// PT-br: Verifica se a agregacao textual com DISTINCT ignora valores NULL e remove duplicidade de texto.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdAggregation")]
    public void StringAggregation_Distinct_ShouldIgnoreNullValues()
        => AssertStringAggregationDistinctIgnoresNullValues("""
SELECT LIST(val, '|') AS joined
FROM (SELECT DISTINCT val FROM textagg_data WHERE grp = 1) q
""");
}
