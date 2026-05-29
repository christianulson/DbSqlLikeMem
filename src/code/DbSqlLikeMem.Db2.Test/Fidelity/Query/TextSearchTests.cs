using DbSqlLikeMem.Db2.TestTools;

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for CONTAINS full-text search semantics.
/// PT-br: Executa testes de fidelidade Db2 para a semantica de busca em texto completo CONTAINS.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    /// <summary>
    /// EN: Creates the Db2 mock connection used by this test suite.
    /// PT-br: Cria a conexao mock Db2 usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static Db2ConnectionMock CreateOpenConnection()
    {
        var db = new Db2DbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new Db2ConnectionMock(db);
        cnn.Open();

        using var seed = new Db2CommandMock(cnn)
        {
            CommandText = """
                INSERT INTO Documents (Id, Title, Body) VALUES
                    (1, 'C# Programming', 'Learn C# and .NET for building modern applications'),
                    (2, 'Java Basics', 'Java is a popular language for enterprise development'),
                    (3, 'Python Data Science', 'Python with pandas and numpy for data analysis'),
                    (4, 'JavaScript Web', 'Build web applications with JavaScript and Node.js'),
                    (5, 'C# Advanced', 'Deep dive into C# patterns, LINQ, and async programming');
                """
        };
        seed.ExecuteNonQuery();

        return cnn;
    }

    /// <summary>
    /// EN: Verifies CONTAINS returns no rows for a non-matching query.
    /// PT-br: Verifica que CONTAINS nao retorna linhas para consulta sem correspondencia.
    /// </summary>
    [FidelityFact]
    public void Contains_NonMatchingQuery_ShouldReturnNoRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'ruby') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Empty(ids);
    }

    /// <summary>
    /// EN: Verifies CONTAINS matches rows containing the search term.
    /// PT-br: Verifica que CONTAINS encontra linhas contendo o termo de busca.
    /// </summary>
    [FidelityFact]
    public void Contains_MatchingQuery_ShouldReturnMatchingRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C#') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Equal(1, ids[0]);
        Assert.Equal(5, ids[1]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with multiple terms matches rows containing all terms.
    /// PT-br: Verifica que CONTAINS com multiplos termos encontra linhas contendo todos os termos.
    /// </summary>
    [FidelityFact]
    public void Contains_MultipleTerms_ShouldMatchAllTerms()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C# AND Advanced') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(5, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with prefix wildcard matches words starting with prefix.
    /// PT-br: Verifica que CONTAINS com curinga de prefixo encontra palavras comecando com prefixo.
    /// </summary>
    [FidelityFact]
    public void Contains_PrefixWildcard_ShouldMatchPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'Java*') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(2, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with phrase search matches exact phrase.
    /// PT-br: Verifica que CONTAINS com busca por frase encontra frase exata.
    /// </summary>
    [FidelityFact]
    public void Contains_PhraseSearch_ShouldMatchExactPhrase()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Body, 'data analysis') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS across multiple columns searches all targets.
    /// PT-br: Verifica que CONTAINS em multiplas colunas busca em todos os alvos.
    /// </summary>
    [FidelityFact]
    public void Contains_MultipleColumns_ShouldSearchAllTargets()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new Db2CommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS((Title, Body), 'pandas') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }
}
