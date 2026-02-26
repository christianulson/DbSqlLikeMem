namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class MySqlWhereParserAndExecutorTests.
/// PT: Define a classe MySqlWhereParserAndExecutorTests.
/// </summary>
public sealed class MySqlWhereParserAndExecutorTests : XUnitTestBase
{
    private readonly MySqlConnectionMock _cnn;

    /// <summary>
    /// EN: Tests MySqlWhereParserAndExecutorTests behavior.
    /// PT: Testa o comportamento de MySqlWhereParserAndExecutorTests.
    /// </summary>
    public MySqlWhereParserAndExecutorTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);
        users.AddColumn("tags", DbType.String, true); // CSV-like "a,b,c"

        users.CreateIndex("ix_users_name", ["name"]);
        users.CreateIndex("ix_users_name_email", ["name", "email"]);
        users.CreateIndex("ix_users_name_include_email", ["name"], ["email"]);

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com", [3] = "a,b" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane", [2] = null, [3] = "b,c" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Bob", [2] = "bob@x.com", [3] = null });

        _cnn = new MySqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Where_IndexedEquality_ShouldUseIndexLookupMetric behavior.
    /// PT: Testa o comportamento de Where_IndexedEquality_ShouldUseIndexLookupMetric.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IndexedEquality_ShouldUseIndexLookupMetric()
    {
        var before = _cnn.Metrics.IndexLookups;
        var beforeIndexHint = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name", out var ih) ? ih : 0;
        var beforeTableHint = _cnn.Metrics.TableHints.TryGetValue("users", out var th) ? th : 0;

        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name = 'John'").ToList();

        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
        Assert.Equal(before + 1, _cnn.Metrics.IndexLookups);
        Assert.Equal(beforeIndexHint + 1, _cnn.Metrics.IndexHints["ix_users_name"]);
        Assert.Equal(beforeTableHint + 1, _cnn.Metrics.TableHints["users"]);
    }

    /// <summary>
    /// EN: Tests Where_IndexedEqualityWithParameter_ShouldUseCompositeIndexLookupMetric behavior.
    /// PT: Testa o comportamento de Where_IndexedEqualityWithParameter_ShouldUseCompositeIndexLookupMetric.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IndexedEqualityWithParameter_ShouldUseCompositeIndexLookupMetric()
    {
        var before = _cnn.Metrics.IndexLookups;
        var beforeIndexHint = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var ih) ? ih : 0;
        var beforeTableHint = _cnn.Metrics.TableHints.TryGetValue("users", out var th) ? th : 0;

        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users WHERE name = @name AND email = @email",
            new
            {
                name = "Bob",
                email = "bob@x.com"
            })
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
        Assert.Equal(before + 1, _cnn.Metrics.IndexLookups);
        Assert.Equal(beforeIndexHint + 1, _cnn.Metrics.IndexHints["ix_users_name_email"]);
        Assert.Equal(beforeTableHint + 1, _cnn.Metrics.TableHints["users"]);
    }


    /// <summary>
    /// EN: Tests Where_IndexWithIncludeCoveringProjection_ShouldExposeRequestedColumnsInIndex behavior.
    /// PT: Testa o comportamento de Where_IndexWithIncludeCoveringProjection_ShouldExposeRequestedColumnsInIndex.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IndexWithIncludeCoveringProjection_ShouldExposeRequestedColumnsInIndex()
    {
        var table = _cnn.GetTable("users");
        var idx = table.Indexes["ix_users_name_include_email"];

        var lookup = table.Lookup(idx, "John");

        Assert.NotNull(lookup);
        var idxRow = lookup!.Single().Value;
        Assert.Equal("John", idxRow["name"]);
        Assert.Equal("john@x.com", idxRow["email"]);
        Assert.Equal(1, idxRow["id"]);

        var rows = _cnn.Query<dynamic>("SELECT id, email FROM users WHERE name = 'John'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
        Assert.Equal("john@x.com", (string)rows[0].email);
    }

    /// <summary>
    /// EN: Tests Where_IndexWithoutRequestedColumn_ShouldFallbackToTableRow behavior.
    /// PT: Testa o comportamento de Where_IndexWithoutRequestedColumn_ShouldFallbackToTableRow.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IndexWithoutRequestedColumn_ShouldFallbackToTableRow()
    {
        var table = _cnn.GetTable("users");
        var idx = table.Indexes["ix_users_name_include_email"];

        var lookup = table.Lookup(idx, "John");

        Assert.NotNull(lookup);
        var idxRow = lookup!.Single().Value;
        Assert.False(idxRow.ContainsKey("tags"));

        var rows = _cnn.Query<dynamic>("SELECT tags FROM users WHERE name = 'John'").ToList();
        Assert.Single(rows);
        Assert.Equal("a,b", (string)rows[0].tags);
    }

    /// <summary>
    /// EN: Tests Where_NonIndexedPredicate_ShouldNotIncreaseIndexLookupMetric behavior.
    /// PT: Testa o comportamento de Where_NonIndexedPredicate_ShouldNotIncreaseIndexLookupMetric.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_NonIndexedPredicate_ShouldNotIncreaseIndexLookupMetric()
    {
        var before = _cnn.Metrics.IndexLookups;
        var beforeTableHint = _cnn.Metrics.TableHints.TryGetValue("users", out var th) ? th : 0;

        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1").ToList();

        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
        Assert.Equal(before, _cnn.Metrics.IndexLookups);
        Assert.Equal(beforeTableHint + 1, _cnn.Metrics.TableHints["users"]);
    }


    /// <summary>
    /// EN: Ensures FORCE INDEX FOR JOIN validates missing indexes on joined tables.
    /// PT: Garante que FORCE INDEX FOR JOIN valide índices inexistentes em tabelas de JOIN.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Join_ForceIndexForJoin_WithMissingIndex_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>(
                "SELECT u.id FROM users u JOIN users u2 FORCE INDEX FOR JOIN (ix_missing) ON u2.id = u.id WHERE u.id = 1")
            .ToList());

        Assert.Contains("FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR JOIN with existing index keeps join execution working.
    /// PT: Garante que FORCE INDEX FOR JOIN com índice existente mantenha a execução do join.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Join_ForceIndexForJoin_WithExistingIndex_ShouldExecute()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT u.id FROM users u JOIN users u2 FORCE INDEX FOR JOIN (ix_users_name) ON u2.name = u.name WHERE u.id = 1")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Ensures USE INDEX prioritizes the hinted index when available.
    /// PT: Garante que USE INDEX priorize o índice indicado quando disponível.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_UseIndex_ShouldPrioritizeHintedIndex()
    {
        var beforeName = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name", out var nameHits) ? nameHits : 0;
        var beforeComposite = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var compositeHits) ? compositeHits : 0;

        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users USE INDEX (ix_users_name) WHERE name = 'Bob' AND email = 'bob@x.com'")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
        Assert.Equal(beforeName + 1, _cnn.Metrics.IndexHints["ix_users_name"]);
        Assert.Equal(beforeComposite, _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var afterComposite) ? afterComposite : 0);
    }

    /// <summary>
    /// EN: Ensures IGNORE INDEX prevents using the ignored index.
    /// PT: Garante que IGNORE INDEX evite o uso do índice ignorado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IgnoreIndex_ShouldAvoidIgnoredIndex()
    {
        var beforeName = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name", out var nameHits) ? nameHits : 0;
        var beforeComposite = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var compositeHits) ? compositeHits : 0;

        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users IGNORE INDEX (ix_users_name_email) WHERE name = 'Bob' AND email = 'bob@x.com'")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
        Assert.Equal(beforeName + 1, _cnn.Metrics.IndexHints["ix_users_name"]);
        Assert.Equal(beforeComposite, _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var afterComposite) ? afterComposite : 0);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX with a missing index fails fast.
    /// PT: Garante que FORCE INDEX com índice inexistente falhe rapidamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndex_WithMissingIndex_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT id FROM users FORCE INDEX (ix_missing) WHERE name = 'John'").ToList());

        Assert.Contains("FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR ORDER BY validates referenced indexes even with minimal runtime scope handling.
    /// PT: Garante que FORCE INDEX FOR ORDER BY valide índices referenciados mesmo com tratamento mínimo de escopo em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForOrderBy_WithMissingIndex_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT id FROM users FORCE INDEX FOR ORDER BY (ix_missing) WHERE name = 'Bob' AND email = 'bob@x.com' ORDER BY name").ToList());

        Assert.Contains("FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures FORCE INDEX FOR GROUP BY validates referenced indexes even with minimal runtime scope handling.
    /// PT: Garante que FORCE INDEX FOR GROUP BY valide índices referenciados mesmo com tratamento mínimo de escopo em runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForGroupBy_WithMissingIndex_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT name, COUNT(*) AS c FROM users FORCE INDEX FOR GROUP BY (ix_missing) WHERE name = 'Bob' AND email = 'bob@x.com' GROUP BY name").ToList());

        Assert.Contains("FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR ORDER BY missing index is ignored when query has no ORDER BY clause.
    /// PT: Garante que FORCE INDEX FOR ORDER BY com índice inexistente seja ignorado quando a consulta não tem ORDER BY.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForOrderBy_WithoutOrderByClause_ShouldNotThrow()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users FORCE INDEX FOR ORDER BY (ix_missing) WHERE name = 'Bob' AND email = 'bob@x.com'")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR GROUP BY missing index is ignored when query has no GROUP BY clause.
    /// PT: Garante que FORCE INDEX FOR GROUP BY com índice inexistente seja ignorado quando a consulta não tem GROUP BY.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForGroupBy_WithoutGroupByClause_ShouldNotThrow()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users FORCE INDEX FOR GROUP BY (ix_missing) WHERE name = 'Bob' AND email = 'bob@x.com'")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR GROUP BY keeps default row-access planning when hinted index exists.
    /// PT: Garante que FORCE INDEX FOR GROUP BY mantenha o planejamento padrão de acesso a linhas quando o índice existe.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForGroupBy_WithExistingIndex_ShouldKeepDefaultPlanning()
    {
        var beforeComposite = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var compositeHits) ? compositeHits : 0;

        var rows = _cnn.Query<dynamic>(
            "SELECT name, COUNT(*) AS c FROM users FORCE INDEX FOR GROUP BY (ix_users_name) WHERE name = 'Bob' AND email = 'bob@x.com' GROUP BY name")
            .ToList();

        Assert.Single(rows);
        Assert.Equal("Bob", (string)rows[0].name);
        Assert.Equal(1L, (long)rows[0].c);
        Assert.Equal(beforeComposite + 1, _cnn.Metrics.IndexHints["ix_users_name_email"]);
    }

    /// <summary>
    /// EN: Ensures FORCE INDEX FOR ORDER BY keeps default row-access planning when hinted index exists.
    /// PT: Garante que FORCE INDEX FOR ORDER BY mantenha o planejamento padrão de acesso a linhas quando o índice existe.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForceIndexForOrderBy_WithExistingIndex_ShouldKeepDefaultPlanning()
    {
        var beforeComposite = _cnn.Metrics.IndexHints.TryGetValue("ix_users_name_email", out var compositeHits) ? compositeHits : 0;

        var rows = _cnn.Query<dynamic>(
            "SELECT id FROM users FORCE INDEX FOR ORDER BY (ix_users_name) WHERE name = 'Bob' AND email = 'bob@x.com' ORDER BY name")
            .ToList();

        Assert.Single(rows);
        Assert.Equal(3, (int)rows[0].id);
        Assert.Equal(beforeComposite + 1, _cnn.Metrics.IndexHints["ix_users_name_email"]);
    }

    /// <summary>
    /// EN: Ensures PRIMARY hint resolves to a primary-key-equivalent index in execution.
    /// PT: Garante que hint PRIMARY resolva para um índice equivalente à chave primária na execução.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_ForcePrimaryHint_ShouldUsePrimaryKeyEquivalentIndex()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddPrimaryKeyIndexes("id");
        users.CreateIndex("ix_users_pk", ["id"], unique: true);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        using var cnn = new MySqlConnectionMock(db);
        cnn.Open();

        var beforePk = cnn.Metrics.IndexHints.TryGetValue("ix_users_pk", out var pkHits) ? pkHits : 0;

        var rows = cnn.Query<dynamic>("SELECT name FROM users FORCE INDEX (PRIMARY) WHERE id = 1").ToList();

        Assert.Single(rows);
        Assert.Equal("John", (string)rows[0].name);
        Assert.Equal(beforePk + 1, cnn.Metrics.IndexHints["ix_users_pk"]);
    }

    /// <summary>
    /// EN: Ensures ORDER BY ordinal position works for projected columns.
    /// PT: Garante que ORDER BY por posição ordinal funcione para colunas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void OrderBy_OrdinalPosition_ShouldSortBySelectedColumn()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT name, id FROM users ORDER BY 2 DESC")
            .ToList();

        Assert.Equal([3, 2, 1], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Ensures ORDER BY alias works for expression projections.
    /// PT: Garante que ORDER BY por alias funcione para projeções com expressão.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void OrderBy_AliasFromExpression_ShouldSortCorrectly()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id + 10 AS score, id FROM users ORDER BY score DESC")
            .ToList();

        Assert.Equal([3, 2, 1], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Ensures ORDER BY alias works when alias differs from base column name.
    /// PT: Garante que ORDER BY por alias funcione quando o alias difere do nome da coluna base.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void OrderBy_AliasDifferentFromColumnName_ShouldSortCorrectly()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT name AS username, id FROM users ORDER BY username DESC")
            .ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]); // John, Jane, Bob
    }

    /// <summary>
    /// EN: Ensures ORDER BY function expression works in execution.
    /// PT: Garante que ORDER BY com expressão de função funcione na execução.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void OrderBy_FunctionExpression_ShouldSortCorrectly()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT id, name FROM users ORDER BY LOWER(name) ASC")
            .ToList();

        Assert.Equal([3, 2, 1], [.. rows.Select(r => (int)r.id)]); // Bob, Jane, John
    }

    /// <summary>
    /// EN: Ensures ORDER BY qualified column name works against projected rows.
    /// PT: Garante que ORDER BY com nome de coluna qualificado funcione contra linhas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void OrderBy_QualifiedColumnName_ShouldSortCorrectly()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT u.id, u.name FROM users u ORDER BY u.name DESC")
            .ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.id)]); // John, Jane, Bob
    }

    /// <summary>
    /// EN: Ensures GROUP BY ordinal position groups by the projected column.
    /// PT: Garante que GROUP BY por posição ordinal agrupe pela coluna projetada.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void GroupBy_OrdinalPosition_ShouldGroupBySelectedColumn()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT name, COUNT(*) AS c FROM users GROUP BY 1 ORDER BY name")
            .ToList();

        Assert.Equal(3, rows.Count);
        Assert.Equal("Bob", (string)rows[0].name);
        Assert.Equal(1L, (long)rows[0].c);
    }

    /// <summary>
    /// EN: Ensures GROUP BY ordinal out of range throws invalid operation.
    /// PT: Garante que GROUP BY ordinal fora do intervalo lance invalid operation.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void GroupBy_OrdinalOutOfRange_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT name, COUNT(*) AS c FROM users GROUP BY 3").ToList());

        Assert.Contains("GROUP BY ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures HAVING can filter grouped rows by aggregate alias.
    /// PT: Garante que HAVING filtre linhas agrupadas por alias de agregação.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Having_AggregateAlias_ShouldFilterGroupedRows()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT name, SUM(id) AS total FROM users GROUP BY name HAVING total >= 2 ORDER BY 2 DESC")
            .ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal("Bob", (string)rows[0].name);
        Assert.Equal(3L, (long)rows[0].total);
        Assert.Equal("Jane", (string)rows[1].name);
        Assert.Equal(2L, (long)rows[1].total);
    }

    /// <summary>
    /// EN: Ensures GROUP BY + HAVING + ORDER BY supports alias filtering and ordinal ordering together.
    /// PT: Garante que GROUP BY + HAVING + ORDER BY suporte filtro por alias e ordenação ordinal em conjunto.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void GroupByHavingOrderBy_AliasAndOrdinal_ShouldWorkTogether()
    {
        var rows = _cnn.Query<dynamic>(
            "SELECT name, COUNT(*) AS c FROM users GROUP BY name HAVING c > 0 ORDER BY 1 ASC")
            .ToList();

        Assert.Equal(["Bob", "Jane", "John"], [.. rows.Select(r => (string)r.name)]);
    }

    /// <summary>
    /// EN: Ensures invalid HAVING alias references fail with a clear validation error.
    /// PT: Garante que referências inválidas de alias no HAVING falhem com erro de validação claro.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Having_InvalidAlias_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT name, COUNT(*) AS c FROM users GROUP BY name HAVING missing_alias > 0").ToList());

        Assert.Contains("HAVING reference", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures invalid ORDER BY ordinal remains validated in grouped queries with HAVING.
    /// PT: Garante que ordinal inválido em ORDER BY continue validado em queries agrupadas com HAVING.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void GroupByHaving_OrderByOrdinalOutOfRange_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>("SELECT name, COUNT(*) AS c FROM users GROUP BY name HAVING c > 0 ORDER BY 3").ToList());

        Assert.Contains("ORDER BY ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Where_IN_ShouldFilter behavior.
    /// PT: Testa o comportamento de Where_IN_ShouldFilter.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IN_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id IN (1,3)").ToList();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => (int)r.id == 1);
        Assert.Contains(rows, r => (int)r.id == 3);
    }

    /// <summary>
    /// EN: Tests Where_IsNotNull_ShouldFilter behavior.
    /// PT: Testa o comportamento de Where_IsNotNull_ShouldFilter.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_IsNotNull_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE email IS NOT NULL").ToList();
        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// EN: Tests Where_Operators_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_Operators_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_Operators_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id >= 2 AND id <= 3").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id != 2").ToList();
        Assert.Equal([1, 3], [.. rows2.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Tests Where_Like_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_Like_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_Like_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name LIKE '%oh%'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Tests Where_FindInSet_ShouldWork behavior.
    /// PT: Testa o comportamento de Where_FindInSet_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_FindInSet_ShouldWork()
    {
        // FIND_IN_SET('b', tags) -> John(a,b) e Jane(b,c)
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE FIND_IN_SET('b', tags)").ToList();
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Tests Where_AND_ShouldBeCaseInsensitive_InRealLife behavior.
    /// PT: Testa o comportamento de Where_AND_ShouldBeCaseInsensitive_InRealLife.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlWhereParserAndExecutor")]
    public void Where_AND_ShouldBeCaseInsensitive_InRealLife()
    {
        // esse teste é pra pegar o bug clássico: split só em " AND " / " and "
        // Se falhar, você sabe o que arrumar: split por regex com IgnoreCase.
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 aNd name = 'John'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
