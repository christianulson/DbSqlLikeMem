using DbSqlLikeMem.Oracle.TestTools;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for CONTAINS full-text search semantics.
/// PT-br: Executa testes de fidelidade Oracle para a semantica de busca em texto completo CONTAINS.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    /// <summary>
    /// EN: Creates the Oracle mock connection used by this test suite.
    /// PT-br: Cria a conexao mock Oracle usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static OracleConnectionMock CreateOpenConnection()
    {
        var db = new OracleDbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new OracleConnectionMock(db);
        cnn.Open();

        using var seed = new OracleCommandMock(cnn)
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
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'ruby') > 0 ORDER BY Id"
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
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C#') > 0 ORDER BY Id"
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
    /// EN: Verifies CONTAINS with AND operator matches rows containing all terms.
    /// PT-br: Verifica que CONTAINS com operador AND encontra linhas contendo todos os termos.
    /// </summary>
    [FidelityFact]
    public void Contains_AndOperator_ShouldMatchAllTerms()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C# AND Advanced') > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(5, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with fuzzy operator (~) matches similar terms.
    /// PT-br: Verifica que CONTAINS com operador fuzzy (~) encontra termos semelhantes.
    /// </summary>
    [FidelityFact]
    public void Contains_FuzzyOperator_ShouldMatchSimilarTerms()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'fuzzy(program, 60)') > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with wildcard term matches prefixes.
    /// PT-br: Verifica que CONTAINS com curinga encontra prefixos.
    /// </summary>
    [FidelityFact]
    public void Contains_WildcardPrefix_ShouldMatchPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'Java%') > 0 ORDER BY Id"
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
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Body, '{data analysis}') > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with parameterized query text works end-to-end.
    /// PT-br: Verifica que CONTAINS com texto de consulta parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void Contains_ParameterizedQuery_ShouldWork()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Body, :query) > 0 ORDER BY Id"
        };
        cmd.Parameters.Add(new OracleParameter("query", "enterprise"));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(2, ids[0]);
    }

    /// <summary>
    /// EN: Verifies CONTAINS with within operator searches a specific section.
    /// PT-br: Verifica que CONTAINS com operador within busca em secao especifica.
    /// </summary>
    [FidelityFact]
    public void Contains_WithinOperator_ShouldSearchSection()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE CONTAINS(Title, 'C# WITHIN Title') > 0 ORDER BY Id"
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
    /// EN: Verifies CONTAINS score is returned when used in select list.
    /// PT-br: Verifica que o score do CONTAINS e retornado quando usado na lista de selecao.
    /// </summary>
    [FidelityFact]
    public void Contains_ScoreInSelect_ShouldReturnPositiveScore()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id, CONTAINS(Title, 'C#') AS score FROM Documents ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var rowScores = new Dictionary<int, double>();
        while (reader.Read())
        {
            var id = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var score = reader.GetValue(1) is DBNull ? 0.0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            rowScores[id] = score;
        }

        Assert.True(rowScores[1] > 0, "Document 1 has 'C#' in Title");
        Assert.True(rowScores[5] > 0, "Document 5 has 'C#' in Title");
        Assert.True(rowScores[2] == 0, "Document 2 has no match");
    }

    /// <summary>
    /// EN: Verifies CONTAINS with parameterized LIMIT works end-to-end.
    /// PT-br: Verifica que CONTAINS com LIMIT parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void Contains_WithParameterizedLimit_ShouldReturnTopMatched()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = """
                SELECT Id FROM Documents
                WHERE CONTAINS(Title, :query) > 0
                ORDER BY Id
                OFFSET 0 ROWS FETCH NEXT :limit ROWS ONLY
                """
        };
        cmd.Parameters.Add(new OracleParameter("query", "C#"));
        cmd.Parameters.Add(new OracleParameter("limit", 1));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }
}
