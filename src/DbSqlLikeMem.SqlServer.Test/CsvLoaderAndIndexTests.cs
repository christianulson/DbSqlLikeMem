namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class CsvLoaderAndIndexTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests CsvLoader_ShouldLoadRows_ByColumnName behavior.
    /// PT: Testa o comportamento de CsvLoader_ShouldLoadRows_ByColumnName.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void CsvLoader_ShouldLoadRows_ByColumnName()
    {
        var db = new SqlServerDbMock();
        var tb = db.AddTable("users");
        tb.Columns["id"] = new(0, DbType.Int32, false);
        tb.Columns["name"] = new(1, DbType.String, false);

        using var cnn = new SqlServerConnectionMock(db);

        var tmp = Path.GetTempFileName();
        File.WriteAllText(tmp,
            "id,name\n" +
            "1,John\n" +
            "2,Jane\n");

        db.LoadCsv(tmp, "users");

        Assert.Equal(2, cnn.GetTable("users").Count);
        Assert.Equal("John", cnn.GetTable("users")[0][1]);
    }

    /// <summary>
    /// EN: Tests GetColumn_ShouldThrow_UnknownColumn behavior.
    /// PT: Testa o comportamento de GetColumn_ShouldThrow_UnknownColumn.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void GetColumn_ShouldThrow_UnknownColumn()
    {
        var db = new SqlServerDbMock();
        var tb = db.AddTable("users");
        tb.Columns["id"] = new(0, DbType.Int32, false);

        var ex = Assert.Throws<SqlServerMockException>(() => tb.GetColumn("nope"));
        Assert.Equal(1054, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests Index_Lookup_ShouldReturnRowPositions behavior.
    /// PT: Testa o comportamento de Index_Lookup_ShouldReturnRowPositions.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Index_Lookup_ShouldReturnRowPositions()
    {
        var db = new SqlServerDbMock();
        var tb = db.AddTable("users");
        tb.Columns["id"] = new(0, DbType.Int32, false);
        tb.Columns["name"] = new(1, DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane" });

        var idxDef = new IndexDef("ix_name", ["name"]);
        tb.CreateIndex(idxDef);

        var ix = tb.Lookup(idxDef, "John" );
        Assert.Equal([0, 1], [.. ix!.Order()]);
    }

    /// <summary>
    /// EN: Tests BackupRestore_ShouldRollbackData behavior.
    /// PT: Testa o comportamento de BackupRestore_ShouldRollbackData.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void BackupRestore_ShouldRollbackData()
    {
        var db = new SqlServerDbMock();
        var tb = db.AddTable("users");
        tb.Columns["id"] = new(0, DbType.Int32, false);
        tb.Columns["name"] = new(1, DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        tb.Backup();
        tb.UpdateRowColumn(0, 1, "Hacked");
        tb.Restore();

        Assert.Equal("John", tb[0][1]);
    }
}
