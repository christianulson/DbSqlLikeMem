namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class MySqlAdditionalBehaviorCoverageTests.
/// PT: Define a classe MySqlAdditionalBehaviorCoverageTests.
/// </summary>
public sealed class MySqlAdditionalBehaviorCoverageTests(
    ITestOutputHelper helper
) : AdditionalBehaviorCoverageTestsBase<MySqlDbMock, MySqlConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db) => new(db);

    /// <inheritdoc />
    protected override string DeleteWithInParameterListSql => "DELETE users WHERE id IN @ids";

    /// <summary>
    /// EN: Verifies IS NULL and IS NOT NULL predicates return the expected rows.
    /// PT: Verifica se os predicados IS NULL e IS NOT NULL retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Where_IsNull_And_IsNotNull_ShouldWork_Test() => Where_IsNull_And_IsNotNull_ShouldWork();

    /// <summary>
    /// EN: Verifies equality comparisons against NULL return no rows.
    /// PT: Verifica se comparacoes de igualdade com NULL nao retornam linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Where_EqualNull_ShouldReturnNoRows_Test() => Where_EqualNull_ShouldReturnNoRows();

    /// <summary>
    /// EN: Verifies left joins preserve left-side rows when there is no matching right-side row.
    /// PT: Verifica se left joins preservam as linhas da esquerda quando nao ha linha correspondente na direita.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void LeftJoin_ShouldPreserveLeftRows_WhenNoMatch_Test() => LeftJoin_ShouldPreserveLeftRows_WhenNoMatch();

    /// <summary>
    /// EN: Verifies mixed descending and ascending ordering is deterministic.
    /// PT: Verifica se a ordenacao mista decrescente e crescente e deterministica.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void OrderBy_Desc_ThenAsc_ShouldBeDeterministic_Test() => OrderBy_Desc_ThenAsc_ShouldBeDeterministic();

    /// <summary>
    /// EN: Verifies COUNT(*) and COUNT(column) handle null values differently as expected.
    /// PT: Verifica se COUNT(*) e COUNT(coluna) tratam valores nulos de forma diferente conforme esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls_Test() => Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls();

    /// <summary>
    /// EN: Verifies HAVING filters grouped results correctly.
    /// PT: Verifica se HAVING filtra corretamente os resultados agrupados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Having_ShouldFilterGroups_Test() => Having_ShouldFilterGroups();

    /// <summary>
    /// EN: Verifies parameter lists work correctly in IN predicates.
    /// PT: Verifica se listas de parametros funcionam corretamente em predicados IN.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Where_In_WithParameterList_ShouldWork_Test() => Where_In_WithParameterList_ShouldWork();

    /// <summary>
    /// EN: Verifies inserts map values correctly when columns are specified out of order.
    /// PT: Verifica se insercoes mapeiam valores corretamente quando as colunas sao informadas fora de ordem.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Insert_WithColumnsOutOfOrder_ShouldMapCorrectly_Test() => Insert_WithColumnsOutOfOrder_ShouldMapCorrectly();

    /// <summary>
    /// EN: Verifies deletes using an IN parameter list remove the expected rows.
    /// PT: Verifica se deletes usando uma lista de parametros em IN removem as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Delete_WithInParameterList_ShouldDeleteMatchingRows_Test() => Delete_WithInParameterList_ShouldDeleteMatchingRows();

    /// <summary>
    /// EN: Verifies update set expressions can reference the current column value correctly.
    /// PT: Verifica se expressoes SET em updates podem referenciar corretamente o valor atual da coluna.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlAdditionalBehaviorCoverage")]
    public void Update_SetExpression_ShouldUpdateRows_Test() => Update_SetExpression_ShouldUpdateRows();
}
