using System.Linq.Expressions;

namespace DbSqlLikeMem.MySql.Test;
/// <summary>
/// EN: Defines the class MySqlLinqProviderTest.
/// PT: Define a classe MySqlLinqProviderTest.
/// </summary>
public sealed class MySqlLinqProviderTest(
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

    private static MySqlConnectionMock CreateUsersConnection()
    {
        var db = new MySqlDbMock();
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" } });
        return new MySqlConnectionMock(db);
    }

    /// <summary>
    /// EN: Tests LinqProvider_ShouldQueryWhereAndReturnRows behavior.
    /// PT: Testa o comportamento de LinqProvider_ShouldQueryWhereAndReturnRows.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlLinqProviderTest")]
    public void LinqProvider_ShouldQueryWhereAndReturnRows()
    {
        using var cnn = CreateUsersConnection();

        var list = cnn.AsQueryable<User>("users")
                      .Where(u => u.Id == 2)
                      .ToList();

        Assert.Single(list);
        Assert.Equal("B", list[0].Name);
    }

    /// <summary>
    /// EN: Verifies the non-generic CreateQuery path keeps provider metadata and can be enumerated.
    /// PT: Verifica se o caminho não genérico de CreateQuery mantém os metadados do provedor e pode ser enumerado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlLinqProviderTest")]
    public void LinqProvider_CreateQueryNonGeneric_ShouldPreserveTableName()
    {
        using var cnn = CreateUsersConnection();
        var queryable = cnn.AsQueryable<User>("users");
        var provider = new MySqlQueryProvider(cnn);

        var recreated = provider.CreateQuery(queryable.Expression);

        recreated.Should().BeOfType<MySqlQueryable<User>>();
        ((MySqlQueryable<User>)recreated).TableName.Should().Be("users");
        recreated.Cast<User>().Should().HaveCount(2);
    }

    /// <summary>
    /// EN: Verifies Execute delegates translated scalar expressions to the in-memory query executor.
    /// PT: Verifica se Execute delega expressões escalares traduzidas ao executor de consulta em memória.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlLinqProviderTest")]
    public void LinqProvider_ExecuteScalar_ShouldReturnTranslatedResult()
    {
        using var cnn = CreateUsersConnection();

        var count = cnn.AsQueryable<User>("users").Count();

        Assert.Equal(2, count);
    }

    /// <summary>
    /// EN: Verifies CreateQuery rejects expressions that do not expose a table source.
    /// PT: Verifica se CreateQuery rejeita expressões que não expõem uma origem de tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlLinqProviderTest")]
    public void LinqProvider_CreateQueryWithoutTableSource_ShouldThrow()
    {
        using var cnn = CreateUsersConnection();
        var provider = new MySqlQueryProvider(cnn);
        Expression<Func<int>> expression = () => 1;

        Assert.Throws<InvalidOperationException>(() => provider.CreateQuery<int>(expression.Body));
    }

}
