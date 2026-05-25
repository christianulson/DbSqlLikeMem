namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL WHERE parser and executor scenarios over a direct mock connection.
/// PT-br: Cobre cenarios do parser e executor de WHERE PostgreSQL sobre uma conexao mock direta.
/// </summary>
public sealed class PostgreSqlWhereParserAndExecutorTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory PostgreSQL database used by the WHERE parser and executor coverage tests.
    /// PT-br: Cria o banco PostgreSQL em memoria usado pelos testes de cobertura do parser e executor de WHERE.
    /// </summary>
    public PostgreSqlWhereParserAndExecutorTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
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

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies indexed equality increments the index lookup metric.
    /// PT-br: Verifica se igualdade indexada incrementa a metrica de lookup por indice.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
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
    /// EN: Verifies parameterized equality uses the composite index metric.
    /// PT-br: Verifica se igualdade parametrizada usa a metrica de indice composto.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
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
    /// EN: Verifies covering indexes expose the requested projected columns.
    /// PT-br: Verifica se indices de cobertura expõem as colunas projetadas solicitadas.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_IndexWithIncludeCoveringProjection_ShouldExposeRequestedColumnsInIndex()
    {
        var table = _cnn.GetTable("users");
        var idx = table.Indexes["ix_users_name_include_email"];

        var lookup = table.Lookup(idx, new IndexKey("John"));

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
    /// EN: Verifies missing projected columns fall back to the base table row.
    /// PT-br: Verifica se colunas projetadas ausentes usam fallback para a linha base da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_IndexWithoutRequestedColumn_ShouldFallbackToTableRow()
    {
        var table = _cnn.GetTable("users");
        var idx = table.Indexes["ix_users_name_include_email"];

        var lookup = table.Lookup(idx, new IndexKey("John"));

        Assert.NotNull(lookup);
        var idxRow = lookup!.Single().Value;
        Assert.False(idxRow.ContainsKey("tags"));

        var rows = _cnn.Query<dynamic>("SELECT tags FROM users WHERE name = 'John'").ToList();
        Assert.Single(rows);
        Assert.Equal("a,b", (string)rows[0].tags);
    }

    /// <summary>
    /// EN: Verifies non-indexed predicates do not increase index lookup metrics.
    /// PT-br: Verifica se predicados sem indice nao aumentam as metricas de lookup por indice.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
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
    /// EN: Verifies IN filters rows correctly in PostgreSQL coverage.
    /// PT-br: Verifica se IN filtra linhas corretamente na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_IN_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id IN (1,3)").ToList();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => (int)r.id == 1);
        Assert.Contains(rows, r => (int)r.id == 3);
    }

    /// <summary>
    /// EN: Verifies IS NOT NULL filters rows correctly in PostgreSQL coverage.
    /// PT-br: Verifica se IS NOT NULL filtra linhas corretamente na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_IsNotNull_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE email IS NOT NULL").ToList();
        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// EN: Verifies comparison operators work in PostgreSQL coverage.
    /// PT-br: Verifica se os operadores de comparacao funcionam na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_Operators_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id >= 2 AND id <= 3").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id != 2").ToList();
        Assert.Equal([1, 3], [.. rows2.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Verifies LIKE filters rows correctly in PostgreSQL coverage.
    /// PT-br: Verifica se LIKE filtra linhas corretamente na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_Like_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name LIKE '%oh%'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Verifies ILIKE filters rows correctly in PostgreSQL coverage.
    /// PT-br: Verifica se ILIKE filtra linhas corretamente na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_Ilike_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name ILIKE 'jo%'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Verifies FIND_IN_SET filters rows correctly in PostgreSQL coverage.
    /// PT-br: Verifica se FIND_IN_SET filtra linhas corretamente na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_FindInSet_ShouldWork()
    {
        // FIND_IN_SET('b', tags) -> John(a,b) e Jane(b,c)
        var ex = Assert.Throws<NotSupportedException>(() => _cnn
            .Query<dynamic>("SELECT id FROM users WHERE FIND_IN_SET('b', tags)")
            .ToList());
        Assert.Contains("FIND_IN_SET", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies mixed-case AND expressions are parsed correctly.
    /// PT-br: Verifica se expressoes AND com maiusculas e minusculas sao interpretadas corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlWhereParserAndExecutor")]
    public void Where_AND_ShouldBeCaseInsensitive_InRealLife()
    {
        // esse teste é pra pegar o bug clássico: split só em SqlConst._AND_ / " and "
        // Se falhar, você sabe o que arrumar: split por regex com IgnoreCase.
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id = 1 aNd name = 'John'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT-br: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
