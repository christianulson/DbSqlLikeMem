namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Shared CsvLoader and index behavior tests executed by provider-specific derived classes.
/// PT: Testes compartilhados de CsvLoader e índices executados por classes derivadas de cada provedor.
/// </summary>
public abstract class CsvLoaderAndIndexTestBase<TDbMock, TSqlMockException>(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
    where TDbMock : DbMock
    where TSqlMockException : SqlMockException
{
    protected abstract TDbMock CreateDb();

    /// <summary>
    /// EN: Tests CsvLoader_ShouldLoadRows_ByColumnName behavior.
    /// PT: Testa o comportamento de CsvLoader_ShouldLoadRows_ByColumnName.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void CsvLoader_ShouldLoadRows_ByColumnName()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        var tmp = Path.GetTempFileName();
        File.WriteAllText(tmp,
            "id,name\n" +
            "1,John\n" +
            "2,Jane\n");

        db.LoadCsv(tmp, "users");

        Assert.Equal(2, db.GetTable("users").Count);
        Assert.Equal("John", tb[0][1]);
    }

    /// <summary>
    /// EN: Tests GetColumn_ShouldThrow_UnknownColumn behavior.
    /// PT: Testa o comportamento de GetColumn_ShouldThrow_UnknownColumn.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void GetColumn_ShouldThrow_UnknownColumn()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);

        var ex = Assert.Throws<TSqlMockException>(() => tb.GetColumn("nope"));
        Assert.Equal(1054, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests Index_Lookup_ShouldReturnRowPositions behavior.
    /// PT: Testa o comportamento de Index_Lookup_ShouldReturnRowPositions.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void Index_Lookup_ShouldReturnRowPositions()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "John" });
        tb.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane" });

        var idxDef = tb.CreateIndex("ix_name", ["name"]);

        var ix = tb.Lookup(idxDef, "John");
        Assert.Equal([0, 1], [.. ix!.Select(_ => _.Key).OrderBy(_ => _)]);
    }

    /// <summary>
    /// EN: Tests BackupRestore_ShouldRollbackData behavior.
    /// PT: Testa o comportamento de BackupRestore_ShouldRollbackData.
    /// </summary>
    [Fact]
    [Trait("Category", "CsvLoaderAndIndex")]
    public void BackupRestore_ShouldRollbackData()
    {
        var db = CreateDb();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("name", DbType.String, false);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John" });

        tb.Backup();
        tb.UpdateRowColumn(0, 1, "Hacked");
        tb.Restore();

        Assert.Equal("John", tb[0][1]);
    }
}
