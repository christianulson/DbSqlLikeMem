namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class Db2AdditionalBehaviorCoverageTests.
/// PT: Define a classe Db2AdditionalBehaviorCoverageTests.
/// </summary>
public sealed class Db2AdditionalBehaviorCoverageTests(
    ITestOutputHelper helper
) : AdditionalBehaviorCoverageTestsBase<Db2DbMock, Db2ConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override Db2ConnectionMock CreateConnection(Db2DbMock db) => new(db);

    /// <summary>
    /// EN: Verifies IS NULL and IS NOT NULL predicates filter rows correctly.
    /// PT: Verifica se os predicados IS NULL e IS NOT NULL filtram as linhas corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Where_IsNull_And_IsNotNull_ShouldWork_Test() => Where_IsNull_And_IsNotNull_ShouldWork();

    /// <summary>
    /// EN: Verifies comparisons with equals null do not return rows.
    /// PT: Verifica se comparacoes com igual a null nao retornam linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Where_EqualNull_ShouldReturnNoRows_Test() => Where_EqualNull_ShouldReturnNoRows();

    /// <summary>
    /// EN: Verifies left joins preserve left-side rows when there is no match.
    /// PT: Verifica se left joins preservam as linhas do lado esquerdo quando nao ha correspondencia.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void LeftJoin_ShouldPreserveLeftRows_WhenNoMatch_Test() => LeftJoin_ShouldPreserveLeftRows_WhenNoMatch();

    /// <summary>
    /// EN: Verifies mixed descending and ascending ordering stays deterministic.
    /// PT: Verifica se a ordenacao mista descendente e ascendente permanece deterministica.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void OrderBy_Desc_ThenAsc_ShouldBeDeterministic_Test() => OrderBy_Desc_ThenAsc_ShouldBeDeterministic();

    /// <summary>
    /// EN: Verifies COUNT(*) and COUNT(column) handle null values with the expected semantics.
    /// PT: Verifica se COUNT(*) e COUNT(coluna) tratam valores nulos com a semantica esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls_Test() => Aggregation_CountStar_Vs_CountColumn_ShouldRespectNulls();

    /// <summary>
    /// EN: Verifies HAVING filters the grouped result set correctly.
    /// PT: Verifica se HAVING filtra corretamente o conjunto de resultados agrupado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Having_ShouldFilterGroups_Test() => Having_ShouldFilterGroups();

    /// <summary>
    /// EN: Verifies IN predicates with parameter lists match the expected rows.
    /// PT: Verifica se predicados IN com listas de parametros correspondem as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Where_In_WithParameterList_ShouldWork_Test() => Where_In_WithParameterList_ShouldWork();

    /// <summary>
    /// EN: Verifies inserts with columns declared out of order map values correctly.
    /// PT: Verifica se insercoes com colunas declaradas fora de ordem mapeiam os valores corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Insert_WithColumnsOutOfOrder_ShouldMapCorrectly_Test() => Insert_WithColumnsOutOfOrder_ShouldMapCorrectly();

    /// <summary>
    /// EN: Verifies deletes using IN parameter lists remove only the matching rows.
    /// PT: Verifica se exclusoes usando listas de parametros IN removem apenas as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Delete_WithInParameterList_ShouldDeleteMatchingRows_Test() => Delete_WithInParameterList_ShouldDeleteMatchingRows();

    /// <summary>
    /// EN: Verifies update set expressions modify the targeted rows correctly.
    /// PT: Verifica se expressoes SET em updates modificam corretamente as linhas alvo.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2AdditionalBehaviorCoverage")]
    public void Update_SetExpression_ShouldUpdateRows_Test() => Update_SetExpression_ShouldUpdateRows();
}
