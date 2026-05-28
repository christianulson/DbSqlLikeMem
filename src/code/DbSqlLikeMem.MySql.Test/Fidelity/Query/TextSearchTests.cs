using DbSqlLikeMem.MySql.TestTools;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for MATCH ... AGAINST full-text search semantics.
/// PT-br: Executa testes de fidelidade MySQL para a semantica de busca em texto completo MATCH ... AGAINST.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    /// <summary>
    /// EN: Creates the MySQL mock connection used by this test suite.
    /// PT-br: Cria a conexao mock MySQL usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static MySqlConnectionMock CreateOpenConnection()
    {
        var db = new MySqlDbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new MySqlConnectionMock(db);
        cnn.Open();

        using var seed = new MySqlCommandMock(cnn)
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
    /// EN: Verifies MATCH ... AGAINST returns score zero for non-matching query.
    /// PT-br: Verifica que MATCH ... AGAINST retorna score zero para consulta sem correspondencia.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_NonMatchingQuery_ShouldReturnZeroScore()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id, MATCH(Title, Body) AGAINST ('ruby') AS score FROM Documents ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var scores = new List<double>();
        while (reader.Read())
        {
            var score = reader.GetValue(1);
            scores.Add(score is DBNull ? 0.0 : Convert.ToDouble(score, CultureInfo.InvariantCulture));
        }

        Assert.All(scores, s => Assert.Equal(0.0, s));
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST in natural language mode returns positive scores for matching rows.
    /// PT-br: Verifica que MATCH ... AGAINST em modo linguagem natural retorna scores positivos para linhas correspondentes.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_NaturalLanguageMode_ShouldScoreMatchingRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id, MATCH(Title, Body) AGAINST ('C#') AS score FROM Documents ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var rowScores = new Dictionary<int, double>();
        while (reader.Read())
        {
            var id = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var score = reader.GetValue(1) is DBNull ? 0.0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            rowScores[id] = score;
        }

        Assert.True(rowScores[1] > 0, "Document 1 has 'C#' in both Title and Body");
        Assert.True(rowScores[5] > 0, "Document 5 has 'C#' in Title");
        Assert.True(rowScores[2] == 0, "Document 2 has no match");
        Assert.True(rowScores[3] == 0, "Document 3 has no match");
        Assert.True(rowScores[4] == 0, "Document 4 has no match");
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST in boolean mode with required (+) and prohibited (-) operators.
    /// PT-br: Verifica MATCH ... AGAINST em modo booleano com operadores requerido (+) e proibido (-).
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_BooleanModeRequiredAndProhibited_ShouldFilterCorrectly()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('+C# -advanced' IN BOOLEAN MODE) > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST score is higher for rows with more term matches.
    /// PT-br: Verifica que o score de MATCH ... AGAINST e maior para linhas com mais correspondencias.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_MoreMatches_ShouldHaveHigherScore()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id, MATCH(Title, Body) AGAINST ('C# programming') AS score FROM Documents ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var rowScores = new Dictionary<int, double>();
        while (reader.Read())
        {
            var id = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            var score = reader.GetValue(1) is DBNull ? 0.0 : Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture);
            rowScores[id] = score;
        }

        Assert.True(rowScores[1] > rowScores[5], "Doc 1 has both 'C#' and 'programming'; Doc 5 only 'C#'");
        Assert.True(rowScores[5] > 0, "Doc 5 has 'C#' in Title");
    }

    /// <summary>
    /// EN: Verifies ORDER BY MATCH ... AGAINST score descending prioritizes most relevant rows.
    /// PT-br: Verifica que ORDER BY com score de MATCH ... AGAINST descendente prioriza linhas mais relevantes.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_OrderByScoreDesc_ShouldPrioritizeRelevance()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents ORDER BY MATCH(Title, Body) AGAINST ('C# programming') DESC, Id ASC"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(1, ids[0]);
        Assert.Equal(5, ids[1]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST with phrase search in boolean mode.
    /// PT-br: Verifica MATCH ... AGAINST com busca por frase entre aspas em modo booleano.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_PhraseSearch_ShouldMatchExactPhrase()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('\"data analysis\"' IN BOOLEAN MODE) > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST with prefix wildcard (term*) finds words starting with the prefix.
    /// PT-br: Verifica MATCH ... AGAINST com curinga de prefixo (term*) encontra palavras comecando com o prefixo.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_PrefixWildcard_ShouldMatchWordPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('app*' IN BOOLEAN MODE) > 0 ORDER BY Id"
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
    /// EN: Verifies MATCH ... AGAINST across multiple columns flattens content for scoring.
    /// PT-br: Verifica que MATCH ... AGAINST em multiplas colunas achata o conteudo para scoring.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_MultipleColumns_ShouldSearchAllTargets()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('pandas' IN BOOLEAN MODE) > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST with parameterized query text works end-to-end.
    /// PT-br: Verifica que MATCH ... AGAINST com texto de consulta parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_ParameterizedQuery_ShouldWork()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST (@query IN BOOLEAN MODE) > 0 ORDER BY Id"
        };
        cmd.Parameters.Add(new MySqlParameter("@query", "enterprise"));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(2, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST alias in SELECT list is usable in ORDER BY.
    /// PT-br: Verifica que alias de MATCH ... AGAINST na lista SELECT e usavel no ORDER BY.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_AliasInSelect_ShouldBeUsableInOrderBy()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = """
                SELECT Id, MATCH(Title, Body) AGAINST ('C#') AS score
                FROM Documents
                ORDER BY score DESC, Id ASC
                """
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(1, ids[0]);
        Assert.Equal(5, ids[1]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST in WITH QUERY EXPANSION mode is parsed and executed.
    /// PT-br: Verifica que MATCH ... AGAINST no modo WITH QUERY EXPANSION e analisado e executado.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_WithQueryExpansion_ShouldParseAndExecute()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('C#' WITH QUERY EXPANSION) > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(ids.Count >= 2, "Should find at least documents with C# in content");
        Assert.Contains(1, ids);
        Assert.Contains(5, ids);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION is parsed and executed.
    /// PT-br: Verifica que MATCH ... AGAINST IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION e analisado e executado.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_NaturalLanguageWithQueryExpansion_ShouldParseAndExecute()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE MATCH(Title, Body) AGAINST ('JavaScript' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION) > 0 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(4, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH ... AGAINST with parameterized LIMIT works end-to-end.
    /// PT-br: Verifica que MATCH ... AGAINST com LIMIT parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void MatchAgainst_WithParameterizedLimit_ShouldReturnTopMatched()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = """
                SELECT Id, MATCH(Title, Body) AGAINST (@query IN BOOLEAN MODE) AS score
                FROM Documents
                ORDER BY score DESC, Id ASC
                LIMIT @limit
                """
        };
        cmd.Parameters.Add(new MySqlParameter("@query", "C# programming .NET"));
        cmd.Parameters.Add(new MySqlParameter("@limit", 2));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Equal(1, ids[0]);
    }
}
