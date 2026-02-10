namespace DbSqlLikeMem.Oracle.Test;
/// <summary>
/// EN: Defines the class OracleLinqProviderTest.
/// PT: Define o(a) class OracleLinqProviderTest.
/// </summary>
public sealed class OracleLinqProviderTest
{
#pragma warning disable CA1812
    private sealed class User
    {
        /// <summary>
        /// EN: Provides details for Id.
        /// PT: Fornece detalhes de Id.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Provides details for Name.
        /// PT: Fornece detalhes de Name.
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
        var db = new OracleDbMock();
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });

        using var cnn = new OracleConnectionMock(db);

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("B", list[0].Name);
    }

}
