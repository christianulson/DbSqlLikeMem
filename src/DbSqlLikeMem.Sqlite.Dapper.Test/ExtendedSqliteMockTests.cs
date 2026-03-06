namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Defines the class ExtendedSqliteMockTests.
/// PT: Define a classe ExtendedSqliteMockTests.
/// </summary>
public sealed class ExtendedSqliteMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<SqliteDbMock, SqliteConnectionMock, SqliteMockException>(helper)
{
    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1";

    /// <summary>
    /// EN: Verifies inserts without explicit identity values receive an auto-generated identifier.
    /// PT: Verifica se insercoes sem valor explicito de identidade recebem um identificador gerado automaticamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values fail for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos falham para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite index filters return only the expected rows.
    /// PT: Verifica se filtros por indice composto retornam apenas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination returns the expected ordered page of rows.
    /// PT: Verifica se a paginacao com distinct retorna a pagina ordenada esperada de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados depois que os resultados agregados sao produzidos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a parent row fails when child rows still reference it.
    /// PT: Verifica se excluir uma linha pai falha quando linhas filhas ainda a referenciam.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without a primary key still fails.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria ainda falha.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies multiple parameter sets in one insert command add all expected rows.
    /// PT: Verifica se multiplos conjuntos de parametros em um comando de insercao adicionam todas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();

    /// <summary>
    /// EN: Verifies unique composite indexes do not collide when values contain the internal separator pattern.
    /// PT: Verifica se indices compostos unicos nao colidem quando os valores contem o padrao interno de separador.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqliteMock")]
    public void UniqueCompositeIndexShouldNotCollideWhenValuesContainSeparator()
    {
        var db = new SqliteDbMock();
        var table = db.AddTable("t");
        table.AddColumn("first", DbType.String, false);
        table.AddColumn("second", DbType.String, false);
        table.CreateIndex("ux_first_second", ["first", "second"], unique: true);

        table.Add(new Dictionary<int, object?> { { 0, "A|B" }, { 1, "C" } });
        table.Add(new Dictionary<int, object?> { { 0, "A" }, { 1, "B|C" } });

        Assert.Equal(2, table.Count);
    }
}
