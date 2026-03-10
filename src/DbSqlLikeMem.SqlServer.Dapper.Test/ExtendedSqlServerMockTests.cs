namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Defines the class ExtendedSqlServerMockTests.
/// PT: Define a classe ExtendedSqlServerMockTests.
/// </summary>
public sealed class ExtendedSqlServerMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<SqlServerDbMock, SqlServerConnectionMock, SqlServerMockException>(helper)
{
    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

    /// <summary>
    /// EN: Verifies inserts without explicit identity values receive an auto-generated identifier.
    /// PT: Verifica se insercoes sem valor explicito de identidade recebem um identificador gerado automaticamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected only when identity override is enabled for the scenario.
    /// PT: Verifica se valores explícitos de identity são respeitados apenas quando a sobrescrita de identity está habilitada no cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies NEXT VALUE FOR reads and advances registered schema sequences during inserts.
    /// PT: Verifica se NEXT VALUE FOR le e avanca sequences registradas no schema durante insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void NextValueFor_ShouldAdvanceRegisteredSequence_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 10, incrementBy: 5);
        var table = db.AddTable("orders");
        table.AddColumn("id", DbType.Int64, false);

        connection.Execute("INSERT INTO orders (id) VALUES (NEXT VALUE FOR seq_orders)");
        connection.Execute("INSERT INTO orders (id) VALUES (NEXT VALUE FOR seq_orders)");

        Assert.Equal(10L, table[0][0]);
        Assert.Equal(15L, table[1][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(15, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies SELECT NEXT VALUE FOR advances the registered schema sequence once per scalar evaluation.
    /// PT: Verifica se SELECT NEXT VALUE FOR avanca a sequence registrada no schema uma vez por avaliacao escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void SelectNextValueFor_ShouldAdvanceRegisteredSequence_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 10, incrementBy: 5);

        var first = connection.ExecuteScalar<long>("SELECT NEXT VALUE FOR seq_orders");
        var second = connection.ExecuteScalar<long>("SELECT NEXT VALUE FOR seq_orders");

        Assert.Equal(10L, first);
        Assert.Equal(15L, second);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(15, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies schema-qualified sequences are resolved during scalar selects and inserts.
    /// PT: Verifica se sequences qualificadas por schema sao resolvidas durante selects escalares e insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void SchemaQualifiedNextValueFor_ShouldResolveRegisteredSequence_Test()
    {
        var db = CreateDb();
        db.CreateSchema("sales");
        using var connection = new SqlServerConnectionMock(db, "sales");
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 20, incrementBy: 10, schemaName: "sales");
        var table = db.AddTable("orders", schemaName: "sales");
        table.AddColumn("id", DbType.Int64, false);

        var scalar = connection.ExecuteScalar<long>("SELECT NEXT VALUE FOR sales.seq_orders");
        connection.Execute("INSERT INTO sales.orders (id) VALUES (NEXT VALUE FOR sales.seq_orders)");

        Assert.Equal(20L, scalar);
        Assert.Equal(30L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence, "sales"));
        Assert.Equal(30, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies CREATE/DROP SEQUENCE DDL registers and removes SQL Server sequences through the parser pipeline.
    /// PT: Verifica se o DDL CREATE/DROP SEQUENCE registra e remove sequences do SQL Server pelo pipeline do parser.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void CreateAndDropSequenceDdl_ShouldRegisterAndRemoveSequence_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();

        connection.Execute("CREATE SEQUENCE sales.seq_orders START WITH 10 INCREMENT BY 5");
        connection.Execute("CREATE SEQUENCE IF NOT EXISTS sales.seq_orders START WITH 999 INCREMENT BY 99");

        Assert.True(connection.TryGetSequence("seq_orders", out var created, "sales"));
        Assert.Equal(10L, created!.StartValue);
        Assert.Equal(5L, created.IncrementBy);
        Assert.Null(created.CurrentValue);

        var next = connection.ExecuteScalar<long>("SELECT NEXT VALUE FOR sales.seq_orders");
        Assert.Equal(10L, next);

        connection.Execute("DROP SEQUENCE sales.seq_orders");
        connection.Execute("DROP SEQUENCE IF EXISTS sales.seq_orders");

        Assert.False(connection.TryGetSequence("seq_orders", out _, "sales"));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            connection.ExecuteScalar<long>("SELECT NEXT VALUE FOR sales.seq_orders"));
        Assert.Contains("Sequence not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values fail for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos falham para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite index filters return only the expected rows.
    /// PT: Verifica se filtros por indice composto retornam apenas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination returns the expected ordered page of rows.
    /// PT: Verifica se a paginacao com distinct retorna a pagina ordenada esperada de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados depois que os resultados agregados sao produzidos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a parent row fails when child rows still reference it.
    /// PT: Verifica se excluir uma linha pai falha quando linhas filhas ainda a referenciam.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without a primary key still fails.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria ainda falha.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies multiple parameter sets in one insert command add all expected rows.
    /// PT: Verifica se multiplos conjuntos de parametros em um comando de insercao adicionam todas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
