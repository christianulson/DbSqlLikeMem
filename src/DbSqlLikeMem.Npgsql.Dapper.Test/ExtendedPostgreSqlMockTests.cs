namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Defines the class ExtendedPostgreSqlMockTests.
/// PT: Define a classe ExtendedPostgreSqlMockTests.
/// </summary>
public sealed class ExtendedPostgreSqlMockTests(
        ITestOutputHelper helper
    ) : ExtendedDapperProviderTestsBase<NpgsqlDbMock, NpgsqlConnectionMock, NpgsqlMockException>(helper)
{
    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db)
        => new(db);

    /// <inheritdoc />
    protected override string DistinctPaginationSql
        => "SELECT DISTINCT id FROM t ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

    /// <summary>
    /// EN: Verifies inserts without explicit identity values receive an auto-generated identifier.
    /// PT: Verifica se insercoes sem valor explicito de identidade recebem um identificador gerado automaticamente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void InsertAutoIncrementShouldAssignIdentityWhenNotSpecified_Test()
        => InsertAutoIncrementShouldAssignIdentityWhenNotSpecified();

    /// <summary>
    /// EN: Verifies explicit identity values are respected only when identity override is enabled for the scenario.
    /// PT: Verifica se valores explícitos de identity são respeitados apenas quando a sobrescrita de identity está habilitada no cenário.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled_Test()
        => InsertAutoIncrementShouldRespectExplicitIdentityWhenEnabled();

    /// <summary>
    /// EN: Verifies nextval reads and advances registered schema sequences during inserts.
    /// PT: Verifica se nextval le e avanca sequences registradas no schema durante insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void NextVal_ShouldAdvanceRegisteredSequence_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 3, incrementBy: 2);
        var table = db.AddTable("orders");
        table.AddColumn("id", DbType.Int64, false);

        connection.Execute("INSERT INTO orders (id) VALUES (nextval('seq_orders'))");
        connection.Execute("INSERT INTO orders (id) VALUES (nextval('seq_orders'))");

        Assert.Equal(3L, table[0][0]);
        Assert.Equal(5L, table[1][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(5, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies SELECT nextval advances the registered schema sequence once per scalar evaluation.
    /// PT: Verifica se SELECT nextval avanca a sequence registrada no schema uma vez por avaliacao escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void SelectNextVal_ShouldAdvanceRegisteredSequence_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 3, incrementBy: 2);

        var first = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var second = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");

        Assert.Equal(3L, first);
        Assert.Equal(5L, second);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(5, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies schema-qualified sequences are resolved during scalar selects and inserts.
    /// PT: Verifica se sequences qualificadas por schema sao resolvidas durante selects escalares e insercoes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void SchemaQualifiedNextVal_ShouldResolveRegisteredSequence_Test()
    {
        var db = CreateDb();
        db.CreateSchema("sales");
        using var connection = new NpgsqlConnectionMock(db, "sales");
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 7, incrementBy: 4, schemaName: "sales");
        var table = db.AddTable("orders", schemaName: "sales");
        table.AddColumn("id", DbType.Int64, false);

        var scalar = connection.ExecuteScalar<long>("SELECT nextval('sales.seq_orders')");
        connection.Execute("INSERT INTO sales.orders (id) VALUES (nextval('sales.seq_orders'))");

        Assert.Equal(7L, scalar);
        Assert.Equal(11L, table[0][0]);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence, "sales"));
        Assert.Equal(11, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies currval is session-local and only becomes available after nextval in the same connection.
    /// PT: Verifica se currval e local da sessao e so fica disponivel apos nextval na mesma conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void CurrVal_ShouldBeSessionLocal_Test()
    {
        var db = CreateDb();
        using var first = CreateConnection(db);
        using var second = CreateConnection(db);
        first.Open();
        second.Open();
        first.AddSequence("seq_orders", startValue: 10, incrementBy: 2);

        var firstNext = first.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var firstCurr = first.ExecuteScalar<long>("SELECT currval('seq_orders')");
        var missingCurr = Assert.Throws<InvalidOperationException>(() => second.ExecuteScalar<long>("SELECT currval('seq_orders')"));
        var secondNext = second.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var secondCurr = second.ExecuteScalar<long>("SELECT currval('seq_orders')");
        var firstCurrAfterSecond = first.ExecuteScalar<long>("SELECT currval('seq_orders')");

        Assert.Equal(10L, firstNext);
        Assert.Equal(10L, firstCurr);
        Assert.Contains("not yet defined", missingCurr.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(12L, secondNext);
        Assert.Equal(12L, secondCurr);
        Assert.Equal(10L, firstCurrAfterSecond);
    }

    /// <summary>
    /// EN: Verifies setval updates the sequence state and honors the is_called flag for the next nextval.
    /// PT: Verifica se setval atualiza o estado da sequence e respeita a flag is_called para o proximo nextval.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void SetVal_ShouldHonorIsCalledFlag_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 1, incrementBy: 1);

        var initial = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var setCalled = connection.ExecuteScalar<long>("SELECT setval('seq_orders', 20)");
        var currAfterSetCalled = connection.ExecuteScalar<long>("SELECT currval('seq_orders')");
        var nextAfterSetCalled = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var setNotCalled = connection.ExecuteScalar<long>("SELECT setval('seq_orders', 30, false)");
        var currAfterSetNotCalled = connection.ExecuteScalar<long>("SELECT currval('seq_orders')");
        var nextAfterSetNotCalled = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var currAfterNext = connection.ExecuteScalar<long>("SELECT currval('seq_orders')");

        Assert.Equal(1L, initial);
        Assert.Equal(20L, setCalled);
        Assert.Equal(20L, currAfterSetCalled);
        Assert.Equal(21L, nextAfterSetCalled);
        Assert.Equal(30L, setNotCalled);
        Assert.Equal(21L, currAfterSetNotCalled);
        Assert.Equal(30L, nextAfterSetNotCalled);
        Assert.Equal(30L, currAfterNext);
        Assert.True(connection.TryGetSequence("seq_orders", out var sequence));
        Assert.Equal(30, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Verifies lastval returns the last sequence value produced in the current session.
    /// PT: Verifica se lastval retorna o ultimo valor de sequence produzido na sessao atual.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void LastVal_ShouldReturnLastSessionSequenceValue_Test()
    {
        var db = CreateDb();
        using var first = CreateConnection(db);
        using var second = CreateConnection(db);
        first.Open();
        second.Open();
        first.AddSequence("seq_orders", startValue: 100, incrementBy: 10);

        var missing = Assert.Throws<InvalidOperationException>(() => first.ExecuteScalar<long>("SELECT lastval()"));
        var firstNext = first.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var firstLast = first.ExecuteScalar<long>("SELECT lastval()");
        var secondMissing = Assert.Throws<InvalidOperationException>(() => second.ExecuteScalar<long>("SELECT lastval()"));
        var secondNext = second.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var secondLast = second.ExecuteScalar<long>("SELECT lastval()");
        var firstLastAfterSecond = first.ExecuteScalar<long>("SELECT lastval()");

        Assert.Contains("not yet defined", missing.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(100L, firstNext);
        Assert.Equal(100L, firstLast);
        Assert.Contains("not yet defined", secondMissing.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(110L, secondNext);
        Assert.Equal(110L, secondLast);
        Assert.Equal(100L, firstLastAfterSecond);
    }

    /// <summary>
    /// EN: Verifies setval with is_called false does not change lastval until a new nextval is executed.
    /// PT: Verifica se setval com is_called false nao altera lastval ate que um novo nextval seja executado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void LastVal_ShouldRemainUnchangedAfterSetValWithIsCalledFalse_Test()
    {
        var db = CreateDb();
        using var connection = CreateConnection(db);
        connection.Open();
        connection.AddSequence("seq_orders", startValue: 1, incrementBy: 1);

        var initial = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var lastAfterInitial = connection.ExecuteScalar<long>("SELECT lastval()");
        var setResult = connection.ExecuteScalar<long>("SELECT setval('seq_orders', 40, false)");
        var lastAfterSet = connection.ExecuteScalar<long>("SELECT lastval()");
        var nextAfterSet = connection.ExecuteScalar<long>("SELECT nextval('seq_orders')");
        var lastAfterNext = connection.ExecuteScalar<long>("SELECT lastval()");

        Assert.Equal(1L, initial);
        Assert.Equal(1L, lastAfterInitial);
        Assert.Equal(40L, setResult);
        Assert.Equal(1L, lastAfterSet);
        Assert.Equal(40L, nextAfterSet);
        Assert.Equal(40L, lastAfterNext);
    }

    /// <summary>
    /// EN: Verifies inserts with null values succeed for nullable columns.
    /// PT: Verifica se insercoes com valores nulos funcionam para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void InsertNullIntoNullableColumnShouldSucceed_Test()
        => InsertNullIntoNullableColumnShouldSucceed();

    /// <summary>
    /// EN: Verifies inserts with null values fail for non-nullable columns.
    /// PT: Verifica se insercoes com valores nulos falham para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void InsertNullIntoNonNullableColumnShouldThrow_Test()
        => InsertNullIntoNonNullableColumnShouldThrow();

    /// <summary>
    /// EN: Verifies composite index filters return only the expected rows.
    /// PT: Verifica se filtros por indice composto retornam apenas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void CompositeIndexFilterShouldReturnCorrectRows_Test()
        => CompositeIndexFilterShouldReturnCorrectRows();

    /// <summary>
    /// EN: Verifies LIKE filters return the matching rows.
    /// PT: Verifica se filtros LIKE retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void LikeFilterShouldReturnMatchingRows_Test()
        => LikeFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies IN filters return the matching rows.
    /// PT: Verifica se filtros IN retornam as linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void InFilterShouldReturnMatchingRows_Test()
        => InFilterShouldReturnMatchingRows();

    /// <summary>
    /// EN: Verifies distinct pagination returns the expected ordered page of rows.
    /// PT: Verifica se a paginacao com distinct retorna a pagina ordenada esperada de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void OrderByLimitOffsetDistinctShouldReturnExpectedRows_Test()
        => OrderByLimitOffsetDistinctShouldReturnExpectedRows();

    /// <summary>
    /// EN: Verifies HAVING filters are applied after aggregation results are produced.
    /// PT: Verifica se filtros HAVING sao aplicados depois que os resultados agregados sao produzidos.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void HavingFilterShouldApplyAfterAggregation_Test()
        => HavingFilterShouldApplyAfterAggregation();

    /// <summary>
    /// EN: Verifies deleting a parent row fails when child rows still reference it.
    /// PT: Verifica se excluir uma linha pai falha quando linhas filhas ainda a referenciam.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletion_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletion();

    /// <summary>
    /// EN: Verifies deleting a referenced parent row without a primary key still fails.
    /// PT: Verifica se excluir uma linha pai referenciada sem chave primaria ainda falha.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK_Test()
        => ForeignKeyDeleteShouldThrowOnReferencedParentDeletionWithouPK();

    /// <summary>
    /// EN: Verifies multiple parameter sets in one insert command add all expected rows.
    /// PT: Verifica se multiplos conjuntos de parametros em um comando de insercao adicionam todas as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public void MultipleParameterSetsInsertShouldInsertAllRows_Test()
        => MultipleParameterSetsInsertShouldInsertAllRows();
}
