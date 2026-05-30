using DbSqlLikeMem.Npgsql.TestTools;
using Npgsql;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for full-text search with @@ operator.
/// PT-br: Executa testes de fidelidade PostgreSQL para busca em texto completo com operador @@.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    /// <summary>
    /// EN: Creates the PostgreSQL mock connection used by this test suite.
    /// PT-br: Cria a conexao mock PostgreSQL usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static NpgsqlConnectionMock CreateOpenConnection()
    {
        var db = new NpgsqlDbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        using var seed = new NpgsqlCommandMock(cnn)
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
    /// EN: Verifies @@ operator returns false for non-matching query.
    /// PT-br: Verifica que o operador @@ retorna falso para consulta sem correspondencia.
    /// </summary>
    [FidelityFact]
    public void TsQuery_NonMatchingQuery_ShouldReturnNoRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'ruby') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Empty(ids);
    }

    /// <summary>
    /// EN: Verifies @@ operator matches rows containing the search term.
    /// PT-br: Verifica que o operador @@ encontra linhas contendo o termo de busca.
    /// </summary>
    [FidelityFact]
    public void TsQuery_MatchingQuery_ShouldReturnMatchingRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'C#') ORDER BY Id"
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
    /// EN: Verifies @@ with AND operator (&amp;) matches rows containing all terms.
    /// PT-br: Verifica que @@ com operador AND (&amp;) encontra linhas com todos os termos.
    /// </summary>
    [FidelityFact]
    public void TsQuery_AndOperator_ShouldMatchAllTerms()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'C# & Advanced') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(5, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ with OR operator (|) matches rows containing any term.
    /// PT-br: Verifica que @@ com operador OR (|) encontra linhas com qualquer termo.
    /// </summary>
    [FidelityFact]
    public void TsQuery_OrOperator_ShouldMatchAnyTerm()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'Java | Python') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Contains(2, ids);
        Assert.Contains(3, ids);
    }

    /// <summary>
    /// EN: Verifies @@ with prefix matching using to_tsquery prefix operator.
    /// PT-br: Verifica que @@ com correspondencia de prefixo usando operador de prefixo to_tsquery.
    /// </summary>
    [FidelityFact]
    public void TsQuery_PrefixMatch_ShouldMatchPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'Jav:*') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        // Mock sem stemming: "Jav:*" prefix match "Java" e "JavaScript"
        // PostgreSQL real com stemming poderia retornar apenas 1
        Assert.Equal(2, ids.Count);
        Assert.Equal(2, ids[0]);
        Assert.Equal(4, ids[1]);
    }

    /// <summary>
    /// EN: Verifies @@ with phrase search matches exact phrase.
    /// PT-br: Verifica que @@ com busca por frase encontra frase exata.
    /// </summary>
    [FidelityFact]
    public void TsQuery_PhraseSearch_ShouldMatchExactPhrase()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Body) @@ to_tsquery('english', 'data <-> analysis') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ with plainto_tsquery matches rows for natural language query.
    /// PT-br: Verifica que @@ com plainto_tsquery encontra linhas para consulta em linguagem natural.
    /// </summary>
    [FidelityFact]
    public void TsQuery_PlainToTsQuery_ShouldMatchNaturalLanguage()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Body) @@ plainto_tsquery('english', 'data analysis') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ with parameterized query text works end-to-end.
    /// PT-br: Verifica que @@ com texto de consulta parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void TsQuery_ParameterizedQuery_ShouldWork()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Body) @@ to_tsquery('english', @query) ORDER BY Id"
        };
        cmd.Parameters.Add(new NpgsqlParameter("@query", "enterprise"));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(2, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ across multiple columns searches all targets.
    /// PT-br: Verifica que @@ em multiplas colunas busca em todos os alvos.
    /// </summary>
    [FidelityFact]
    public void TsQuery_MultipleColumns_ShouldSearchAllTargets()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title || ' ' || COALESCE(Body, '')) @@ to_tsquery('english', 'pandas') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ with negation operator (!) excludes matching term.
    /// PT-br: Verifica que @@ com operador de negacao (!) exclui termo correspondente.
    /// </summary>
    [FidelityFact]
    public void TsQuery_NegationOperator_ShouldExcludeTerm()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE to_tsvector('english', Title) @@ to_tsquery('english', 'C# & !Advanced') ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies @@ with parameterized LIMIT works end-to-end.
    /// PT-br: Verifica que @@ com LIMIT parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void TsQuery_WithParameterizedLimit_ShouldReturnTopMatched()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new NpgsqlCommandMock(cnn)
        {
            CommandText = """
                SELECT Id FROM Documents
                WHERE to_tsvector('english', Title) @@ to_tsquery('english', @query)
                ORDER BY Id
                LIMIT @limit
                """
        };
        cmd.Parameters.Add(new NpgsqlParameter("@query", "C#"));
        cmd.Parameters.Add(new NpgsqlParameter("@limit", 1));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }
}
