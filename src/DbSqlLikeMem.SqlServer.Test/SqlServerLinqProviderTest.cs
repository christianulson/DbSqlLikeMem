namespace DbSqlLikeMem.SqlServer.Test;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqlServerLinqProviderTest
{
#pragma warning disable CA1812
    private sealed class User
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public string Name { get; set; } = "";
    }
#pragma warning restore CA1812

    /// <summary>
    /// EN: Tests LinqProvider_ShouldQueryWhereAndReturnRows behavior.
    /// PT: Testa o comportamento de LinqProvider_ShouldQueryWhereAndReturnRows.
    /// </summary>
    [Fact]
    public void LinqProvider_ShouldQueryWhereAndReturnRows()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });

        using var cnn = new SqlServerConnectionMock(db);

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("B", list[0].Name);
    }

}
