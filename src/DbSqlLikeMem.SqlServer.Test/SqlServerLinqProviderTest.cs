using FluentAssertions;

namespace DbSqlLikeMem.SqlServer.Test;
/// <summary>
/// EN: Verifies LINQ query translation and provider metadata behavior for SQL Server connections.
/// PT: Verifica a traducao de consultas LINQ e o comportamento de metadados do provedor para conexoes SQL Server.
/// </summary>
public sealed class SqlServerLinqProviderTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
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
    /// EN: Verifies LINQ queries filter rows and project the expected SQL Server results.
    /// PT: Verifica se consultas LINQ filtram linhas e projetam os resultados esperados no SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerLinqProviderTest")]
    public void LinqProvider_ShouldQueryWhereAndReturnRows()
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });

        using var cnn = new SqlServerConnectionMock(db);

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        list.Should().ContainSingle();
        list[0].Name.Should().Be("B");
    }

}
