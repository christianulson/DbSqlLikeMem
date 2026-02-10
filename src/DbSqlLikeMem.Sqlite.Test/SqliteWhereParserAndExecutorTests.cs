namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqliteWhereParserAndExecutorTests : XUnitTestBase
{
    private readonly SqliteConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public SqliteWhereParserAndExecutorTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqliteDbMock();
        var users = db.AddTable("users");
        users.Columns["id"] = new(0, DbType.Int32, false);
        users.Columns["name"] = new(1, DbType.String, false);
        users.Columns["email"] = new(2, DbType.String, true);
        users.Columns["tags"] = new(3, DbType.String, true); // CSV-like "a,b,c"

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "john@x.com", [3] = "a,b" });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane", [2] = null, [3] = "b,c" });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Bob", [2] = "bob@x.com", [3] = null });

        _cnn = new SqliteConnectionMock(db);
        _cnn.Open();
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
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id).OrderBy(_=>_)]);

        var rows2 = _cnn.Query<dynamic>("SELECT id FROM users WHERE id != 2").ToList();
        Assert.Equal([1, 3], [.. rows2.Select(r => (int)r.id).OrderBy(_=>_)]);
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
        Assert.Equal([1, 2], [.. rows.Select(r => (int)r.id).OrderBy(_=>_)]);
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
