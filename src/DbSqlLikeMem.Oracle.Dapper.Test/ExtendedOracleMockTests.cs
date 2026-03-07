namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class ExtendedOracleMockTests.
/// PT: Define a classe ExtendedOracleMockTests.
/// </summary>
public sealed class ExtendedOracleMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<OracleDbMock, OracleConnectionMock, OracleMockException>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

    /// <summary>
    /// EN: Verifies identity values are assigned automatically on inserts when no value is provided.
    /// PT: Verifica se valores de identidade sao atribuidos automaticamente em insercoes quando nenhum valor e informado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected only when identity override is enabled for the scenario.
    /// PT: Verifica se valores explícitos de identity são respeitados apenas quando a sobrescrita de identity está habilitada no cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies Oracle-style sequence access with NEXTVAL and CURRVAL works in scalar queries and inserts.
    /// PT: Verifica se o acesso a sequence no estilo Oracle com NEXTVAL e CURRVAL funciona em consultas escalares e insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void OracleSequenceNextValAndCurrVal_ShouldWork_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 10, incrementBy: 5);
        var table = db.AddTable("orders");
        table.AddColumn("id", DbType.Int64, false);

        var first = connection.ExecuteScalar<long>("SELECT seq_orders.NEXTVAL");
        var curr = connection.ExecuteScalar<long>("SELECT seq_orders.CURRVAL");
        connection.Execute("INSERT INTO orders (id) VALUES (seq_orders.NEXTVAL)");

        Assert.Equal(10L, first);
        Assert.Equal(10L, curr);
        Assert.Equal(15L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(15, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies schema-qualified Oracle-style sequence access resolves the registered sequence.
    /// PT: Verifica se o acesso a sequence no estilo Oracle qualificado por schema resolve a sequence registrada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void OracleSchemaQualifiedSequence_ShouldWork_Test()
    {
        var db = CreateDb();
        db.CreateSchema("sales");
        using var connection = new OracleConnectionMock(db, "sales");
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 100, incrementBy: 10, schemaName: "sales");
        var table = db.AddTable("orders", schemaName: "sales");
        table.AddColumn("id", DbType.Int64, false);

        var first = connection.ExecuteScalar<long>("SELECT sales.seq_orders.NEXTVAL");
        var curr = connection.ExecuteScalar<long>("SELECT sales.seq_orders.CURRVAL");
        connection.Execute("INSERT INTO sales.orders (id) VALUES (sales.seq_orders.NEXTVAL)");

        Assert.Equal(100L, first);
        Assert.Equal(100L, curr);
        Assert.Equal(110L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence, "sales"));
        Assert.Equal(110, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values throw for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos lancam erro para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite-index filters return the expected matching rows.
    /// PT: Verifica se filtros por indice composto retornam as linhas correspondentes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the expected rows.
    /// PT: Verifica se filtros LIKE retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the expected rows.
    /// PT: Verifica se filtros IN retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination with ordering returns the expected rows.
    /// PT: Verifica se a paginacao com distinct e ordenacao retorna as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied correctly after aggregation.
    /// PT: Verifica se filtros HAVING sao aplicados corretamente apos a agregacao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada lanca a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without an explicit primary key still throws the expected exception.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria explicita ainda lanca a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies Dapper inserts all rows when multiple parameter sets are supplied.
    /// PT: Verifica se o Dapper insere todas as linhas quando multiplos conjuntos de parametros sao informados.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedOracleMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
