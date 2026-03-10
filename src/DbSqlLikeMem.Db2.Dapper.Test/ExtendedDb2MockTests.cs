namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class ExtendedDb2MockTests.
/// PT: Define a classe ExtendedDb2MockTests.
/// </summary>
public sealed class ExtendedDb2MockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<Db2DbMock, Db2ConnectionMock, Db2MockException>(helper)
{
    /// <inheritdoc />
    protected override Db2ConnectionMock CreateConnection(Db2DbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1";

    /// <summary>
    /// EN: Verifies inserts without explicit identity values receive an auto-generated identifier.
    /// PT: Verifica se insercoes sem valor explicito de identidade recebem um identificador gerado automaticamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected only when identity override is enabled for the scenario.
    /// PT: Verifica se valores explícitos de identity são respeitados apenas quando a sobrescrita de identity está habilitada no cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies DB2-style NEXT VALUE FOR and PREVIOUS VALUE FOR work in scalar queries and inserts.
    /// PT: Verifica se NEXT VALUE FOR e PREVIOUS VALUE FOR no estilo DB2 funcionam em consultas escalares e insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void Db2SequenceNextAndPreviousValueFor_ShouldWork_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 50, incrementBy: 5);
        var table = db.AddTable("orders");
        table.AddColumn("id", DbType.Int64, false);

        var first = connection.ExecuteScalar<long>("VALUES NEXT VALUE FOR seq_orders");
        var previous = connection.ExecuteScalar<long>("VALUES PREVIOUS VALUE FOR seq_orders");
        connection.Execute("INSERT INTO orders (id) VALUES (NEXT VALUE FOR seq_orders)");

        Assert.Equal(50L, first);
        Assert.Equal(50L, previous);
        Assert.Equal(55L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(55, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies schema-qualified DB2 sequences resolve correctly for NEXT VALUE FOR and PREVIOUS VALUE FOR.
    /// PT: Verifica se sequences DB2 qualificadas por schema resolvem corretamente para NEXT VALUE FOR e PREVIOUS VALUE FOR.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void Db2SchemaQualifiedSequence_ShouldWork_Test()
    {
        var db = CreateDb();
        db.CreateSchema("sales");
        using var connection = new Db2ConnectionMock(db, "sales");
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 200, incrementBy: 20, schemaName: "sales");
        var table = db.AddTable("orders", schemaName: "sales");
        table.AddColumn("id", DbType.Int64, false);

        var first = connection.ExecuteScalar<long>("VALUES NEXT VALUE FOR sales.seq_orders");
        var previous = connection.ExecuteScalar<long>("VALUES PREVIOUS VALUE FOR sales.seq_orders");
        connection.Execute("INSERT INTO sales.orders (id) VALUES (NEXT VALUE FOR sales.seq_orders)");

        Assert.Equal(200L, first);
        Assert.Equal(200L, previous);
        Assert.Equal(220L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence, "sales"));
        Assert.Equal(220, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values fail for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos falham para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite index filters return only the expected rows.
    /// PT: Verifica se filtros por indice composto retornam apenas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination returns the expected ordered page of rows.
    /// PT: Verifica se a paginacao com distinct retorna a pagina ordenada esperada de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados depois que os resultados agregados sao produzidos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a parent row fails when child rows still reference it.
    /// PT: Verifica se excluir uma linha pai falha quando linhas filhas ainda a referenciam.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without a primary key still fails.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria ainda falha.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies multiple parameter sets in one insert command add all expected rows.
    /// PT: Verifica se multiplos conjuntos de parametros em um comando de insercao adicionam todas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedDb2Mock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
