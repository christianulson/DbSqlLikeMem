namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers SQL Server WHERE parser and executor scenarios over a direct mock connection.
/// PT-br: Cobre cenarios do parser e executor de WHERE SQL Server sobre uma conexao mock direta.
/// </summary>
public sealed class SqlServerWhereParserAndExecutorTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory SQL Server database used by the WHERE parser and executor coverage tests.
    /// PT-br: Cria o banco SQL Server em memoria usado pelos testes de cobertura do parser e executor de WHERE.
    /// </summary>
    public SqlServerWhereParserAndExecutorTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
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

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Verifies indexed equality predicates update index lookup metrics.
    /// PT-br: Verifica se predicados de igualdade indexada atualizam as metricas de busca por indice.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
    /// EN: Verifies parameterized indexed equality predicates update composite index lookup metrics.
    /// PT-br: Verifica se predicados de igualdade indexada parametrizados atualizam as metricas de busca por indice composto.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
    /// EN: Verifies covering indexes expose the requested columns.
    /// PT-br: Verifica se indices de cobertura expõem as colunas solicitadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
    /// EN: Verifies missing indexed columns fall back to the table row.
    /// PT-br: Verifica se colunas ausentes no indice voltam para a linha da tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
    /// PT-br: Verifica se predicados nao indexados nao aumentam as metricas de busca por indice.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
    /// EN: Verifies IN filters rows as expected.
    /// PT-br: Verifica se IN filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
    public void Where_IN_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id IN (1,3)").ToList();
        Assert.Equal(2, rows.Count);
        Assert.Contains(rows, r => (int)r.id == 1);
        Assert.Contains(rows, r => (int)r.id == 3);
    }

    /// <summary>
    /// EN: Verifies IS NOT NULL filters rows as expected.
    /// PT-br: Verifica se IS NOT NULL filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
    public void Where_IsNotNull_ShouldFilter()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE email IS NOT NULL").ToList();
        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// EN: Verifies comparison operators return the expected rows.
    /// PT-br: Verifica se operadores de comparacao retornam as linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
    public void Where_Operators_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE id >= 2 AND id <= 3").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id).OrderBy(_ => _)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id != 2").ToList();
        Assert.Equal([1, 3], [.. rows2.Select(r => (int)r.id).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Verifies LIKE filters rows as expected.
    /// PT-br: Verifica se LIKE filtra as linhas como esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
    public void Where_Like_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id FROM users WHERE name LIKE '%oh%'").ToList();
        Assert.Single(rows);
        Assert.Equal(1, (int)rows[0].id);
    }

    /// <summary>
    /// EN: Verifies SQL Server rejects the MySQL-only <c>FIND_IN_SET</c> function.
    /// PT-br: Verifica se o SQL Server rejeita a funcao <c>FIND_IN_SET</c>, que eh especifica do MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
    public void Where_FindInSet_ShouldThrowNotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            _cnn.Query<dynamic>("SELECT id FROM users WHERE FIND_IN_SET('b', tags)").ToList());
    }

    /// <summary>
    /// EN: Verifies mixed-case AND is parsed as a logical conjunction.
    /// PT-br: Verifica se AND em maiusculas e minusculas mistas e interpretado como conjuncao logica.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerWhereParserAndExecutor")]
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
