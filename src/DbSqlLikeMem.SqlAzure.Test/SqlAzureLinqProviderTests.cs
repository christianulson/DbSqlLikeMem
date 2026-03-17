namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Defines SQL Azure LINQ provider smoke tests.
/// PT: Define testes smoke do provider LINQ de SQL Azure.
/// </summary>
public sealed class SqlAzureLinqProviderTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
#pragma warning disable CA1812
    private sealed class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
#pragma warning restore CA1812

    /// <summary>
    /// EN: Ensures SQL Azure LINQ query with explicit table name returns expected rows.
    /// PT: Garante que consulta LINQ SQL Azure com nome de tabela explícito retorne as linhas esperadas.
    /// </summary>
    [Fact]
    public void LinqProvider_ShouldQueryWhereAndReturnRows()
    {
        var db = new SqlAzureDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });

        using var cnn = new SqlAzureConnectionMock(db);

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("B", list[0].Name);
    }

    /// <summary>
    /// EN: Ensures SQL Azure LINQ extension uses CLR type name as default table name.
    /// PT: Garante que a extensão LINQ SQL Azure use o nome do tipo CLR como nome de tabela padrão.
    /// </summary>
    [Fact]
    public void LinqProvider_DefaultTableName_ShouldUseEntityTypeName()
    {
        var db = new SqlAzureDbMock();
        var t = db.AddTable("User");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 11 }, { 1, "Ana" } });

        using var cnn = new SqlAzureConnectionMock(db);

        var list = cnn.AsQueryable<User>()
                      .Where(u => u.Id == 11)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("Ana", list[0].Name);
    }
}
