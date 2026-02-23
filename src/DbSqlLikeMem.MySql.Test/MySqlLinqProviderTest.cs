namespace DbSqlLikeMem.MySql.Test;
/// <summary>
/// EN: Defines the class MySqlLinqProviderTest.
/// PT: Define a classe MySqlLinqProviderTest.
/// </summary>
public sealed class MySqlLinqProviderTest
{
#pragma warning disable CA1812
    private sealed class User
    {
        /// <summary>
        /// EN: Gets or sets Id.
        /// PT: Obtém ou define Id.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// EN: Gets or sets Name.
        /// PT: Obtém ou define Name.
        /// </summary>
        public string Name { get; set; } = "";
    }
#pragma warning restore CA1812

    /// <summary>
    /// EN: Tests LinqProvider_ShouldQueryWhereAndReturnRows behavior.
    /// PT: Testa o comportamento de LinqProvider_ShouldQueryWhereAndReturnRows.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlLinqProviderTest")]
    public void LinqProvider_ShouldQueryWhereAndReturnRows()
    {
        var db = new MySqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });

        using var cnn = new MySqlConnectionMock(db);

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("B", list[0].Name);
    }

}
