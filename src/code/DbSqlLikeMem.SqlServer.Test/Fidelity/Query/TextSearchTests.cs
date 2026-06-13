using DbSqlLikeMem.SqlServer.TestTools;
using Microsoft.Data.SqlClient;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQL Server fidelity tests for CONTAINS and FREETEXT full-text search semantics.
/// PT-br: Executa testes de fidelidade SQL Server para a semantica de busca em texto completo CONTAINS e FREETEXT.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    /// <summary>
    /// EN: Creates the SQL Server mock connection used by this test suite.
    /// PT-br: Cria a conexao mock SQL Server usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static SqlServerConnectionMock CreateOpenConnection()
    {
        var db = new SqlServerDbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new SqlServerConnectionMock(db);
        cnn.Open();

        using var seed = new SqlServerCommandMock(cnn)
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
        using var cmd = new SqlServerCommandMock(cnn)
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
        using var cmd = new SqlServerCommandMock(cnn)
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
        using var cmd = new SqlServerCommandMock(cnn)
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
    /// EN: Verifies CONTAINS with prefix term searches for words starting with the prefix.
    /// PT-br: Verifica que CONTAINS com termo prefixo busca palavras comecando com o prefixo.
    /// </summary>
    [FidelityFact]
    public void Contains_PrefixTerm_ShouldMatchPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C#*') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Contains(1, ids);
        Assert.Contains(5, ids);
    }

    /// <summary>
    /// EN: Verifies FREETEXT returns matching rows for a natural language query.
    /// PT-br: Verifica que FREETEXT retorna linhas correspondentes para consulta em linguagem natural.
    /// </summary>
    [FidelityFact]
    public void Freetext_MatchingQuery_ShouldReturnMatchingRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE FREETEXT(Title, 'data analysis') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Contains(3, ids);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with parameterized query text works end-to-end.
    /// PT-br: Verifica que CONTAINS com texto de consulta parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void Contains_ParameterizedQuery_ShouldWork()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, @query) = 1 ORDER BY Id"
        };
        cmd.Parameters.Add(new SqlParameter("@query", "JavaScript"));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(4, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS searches across multiple columns (Title and Body).
    /// PT-br: Verifica que CONTAINS busca em multiplas colunas (Title e Body).
    /// </summary>
    [FidelityFact]
    public void Contains_MultipleColumns_ShouldSearchAllTargets()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
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

    /// <summary>
    /// EN: Verifies CONTAINS with NEAR proximity term.
    /// PT-br: Verifica que CONTAINS com termo de proximidade NEAR.
    /// </summary>
    [FidelityFact]
    public void Contains_NearTerm_ShouldMatchProximity()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Body, 'NEAR((C#, .NET))') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies FREETEXT matches across multiple columns.
    /// PT-br: Verifica que FREETEXT encontra correspondencias em multiplas colunas.
    /// </summary>
    [FidelityFact]
    public void Freetext_MultipleColumns_ShouldSearchAllTargets()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE FREETEXT((Title, Body), 'JavaScript') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(4, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with FORMSOF inflectional search.
    /// PT-br: Verifica que CONTAINS com busca flexional FORMSOF.
    /// </summary>
    [FidelityFact]
    public void Contains_FormsOfInflectional_ShouldMatchInflections()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Body, 'FORMSOF(INFLECTIONAL, building)') = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Contains(1, ids);
        Assert.Contains(4, ids);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with a parameterized LIMIT works with full-text filter.
    /// PT-br: Verifica que CONTAINS com LIMIT parametrizado funciona com filtro de texto completo.
    /// </summary>
    [FidelityFact]
    public void Contains_WithParameterizedLimit_ShouldReturnTopMatched()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = """
                SELECT Id FROM Documents
                WHERE CONTAINS(Title, @query) = 1
                ORDER BY Id
                OFFSET 0 ROWS FETCH NEXT @limit ROWS ONLY
                """
        };
        cmd.Parameters.Add(new SqlParameter("@query", "C#"));
        cmd.Parameters.Add(new SqlParameter("@limit", 1));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }
}
