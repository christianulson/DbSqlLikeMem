namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class ExtendedMySqlMockTests.
/// PT: Define a classe ExtendedMySqlMockTests.
/// </summary>
public sealed class ExtendedMySqlMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<MySqlDbMock, MySqlConnectionMock, MySqlMockException>(helper)
{
    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1";

    /// <summary>
    /// EN: Verifies identity values are assigned automatically on inserts when no value is provided.
    /// PT: Verifica se valores de identidade sao atribuidos automaticamente em insercoes quando nenhum valor e informado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected only when identity override is enabled for the scenario.
    /// PT: Verifica se valores explícitos de identity são respeitados apenas quando a sobrescrita de identity está habilitada no cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values throw for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos lancam erro para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite-index filters return the expected matching rows.
    /// PT: Verifica se filtros por indice composto retornam as linhas correspondentes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the expected rows.
    /// PT: Verifica se filtros LIKE retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the expected rows.
    /// PT: Verifica se filtros IN retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination with ordering returns the expected rows.
    /// PT: Verifica se a paginacao com distinct e ordenacao retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied correctly after aggregation.
    /// PT: Verifica se filtros HAVING sao aplicados corretamente apos a agregacao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada lanca a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without an explicit primary key still throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria explicita ainda lanca a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies Dapper inserts all rows when multiple parameter sets are supplied.
    /// PT: Verifica se o Dapper insere todas as linhas quando multiplos conjuntos de parametros sao informados.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedMySqlMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
