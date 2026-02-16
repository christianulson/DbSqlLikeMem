namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class Db2WhereParserAndExecutorTests : XUnitTestBase
{
    private readonly Db2ConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Db2WhereParserAndExecutorTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new Db2DbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("email", DbType.String, true);
        users.AddColumn("tags", DbType.String, true); // CSV-like "a,b,c"

        users.CreateIndex(new IndexDef("ix_users_name", ["name"]));
        users.CreateIndex(new IndexDef("ix_users_name_email", ["name", "email"]));

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com", [3] = "a,b" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane", [2] = null, [3] = "b,c" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Bob", [2] = "bob@x.com", [3] = null });

        _cnn = new Db2ConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests Where_IndexedEquality_ShouldUseIndexLookupMetric behavior.
    /// PT: Testa o comportamento de Where_IndexedEquality_ShouldUseIndexLookupMetric.
    /// </summary>
    [Fact]
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
    /// EN: Tests Where_NonIndexedPredicate_ShouldNotIncreaseIndexLookupMetric behavior.
    /// PT: Testa o comportamento de Where_NonIndexedPredicate_ShouldNotIncreaseIndexLookupMetric.
    /// </summary>
    [Fact]
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
    /// EN: Tests Where_IN_ShouldFilter behavior.
    /// PT: Testa o comportamento de Where_IN_ShouldFilter.
    /// </summary>
    [Fact]
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
