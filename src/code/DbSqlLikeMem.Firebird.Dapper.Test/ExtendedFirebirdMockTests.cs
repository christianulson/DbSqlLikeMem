namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers extended Firebird mock scenarios shared across Dapper provider tests.
/// PT: Cobre cenarios estendidos do mock Firebird compartilhados entre testes de provedor Dapper.
/// </summary>
public sealed class ExtendedFirebirdMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<FirebirdDbMock, FirebirdConnectionMock, FirebirdMockException>(helper)
{
    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateConnection(FirebirdDbMock db) => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

    /// <summary>
    /// EN: Verifies inserts without explicit identity values receive a generated identifier.
    /// PT: Verifica se insercoes sem valor explicito de identidade recebem um identificador gerado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected when identity override is enabled.
    /// PT: Verifica se valores explicitos de identidade sao respeitados quando a sobrescrita esta habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies nullable columns accept null values.
    /// PT: Verifica se colunas anulaveis aceitam valores nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies non-nullable columns reject null values.
    /// PT: Verifica se colunas nao anulaveis rejeitam valores nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies filtering on a composite index returns the expected matching rows.
    /// PT: Verifica se o filtro em um indice composto retorna as linhas correspondentes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the expected matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the expected matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination with ordering returns the expected rows.
    /// PT: Verifica se a paginacao com distinct e ordenacao retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados apos a producao dos resultados agregados.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row throws the expected foreign-key exception.
    /// PT: Verifica se excluir uma linha pai referenciada lanca a excecao esperada de chave estrangeira.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without an explicit primary key still throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria explicita ainda lanca a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithoutPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies Dapper inserts all rows when multiple parameter sets are supplied.
    /// PT: Verifica se o Dapper insere todas as linhas quando multiplos conjuntos de parametros sao informados.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedFirebirdMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
