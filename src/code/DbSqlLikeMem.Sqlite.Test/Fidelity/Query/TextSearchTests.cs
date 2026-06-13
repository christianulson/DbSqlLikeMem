using DbSqlLikeMem.Sqlite.TestTools;
using Microsoft.Data.Sqlite;

namespace DbSqlLikeMem.Sqlite.Test.Fidelity.Query;

/// <summary>
/// EN: Runs SQLite fidelity tests for MATCH full-text search operator.
/// PT-br: Executa testes de fidelidade SQLite para o operador de busca em texto completo MATCH.
/// </summary>
public sealed class TextSearchTests : XUnitTestBase
{
    private static readonly int _bootstrap = InitializeBootstrap();

    private static int InitializeBootstrap()
    {
        Sqlite.Test.SqliteBootstrap.Initialize();
        return 0;
    }

    /// <summary>
    /// EN: Creates the SQLite mock connection used by this test suite.
    /// PT-br: Cria a conexao mock SQLite usada por esta suite de testes.
    /// </summary>
    public TextSearchTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    private static SqliteConnectionMock CreateOpenConnection()
    {
        var db = new SqliteDbMock();
        db.AddTable("Documents", [
            new("Id", DbType.Int32, false),
            new("Title", DbType.String, false),
            new("Body", DbType.String, true)
        ]);
        var cnn = new SqliteConnectionMock(db);
        cnn.Open();

        using var seed = new SqliteCommandMock(cnn)
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
    /// EN: Verifies MATCH returns no rows for a non-matching query.
    /// PT-br: Verifica que MATCH nao retorna linhas para consulta sem correspondencia.
    /// </summary>
    [FidelityFact]
    public void Match_NonMatchingQuery_ShouldReturnNoRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH 'ruby' ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Empty(ids);
    }

    /// <summary>
    /// EN: Verifies MATCH returns rows matching the search term.
    /// PT-br: Verifica que MATCH retorna linhas correspondentes ao termo de busca.
    /// </summary>
    [FidelityFact]
    public void Match_MatchingQuery_ShouldReturnMatchingRows()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH 'C#' ORDER BY Id"
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
    /// EN: Verifies MATCH with prefix wildcard matches words starting with prefix.
    /// PT-br: Verifica que MATCH com curinga de prefixo encontra palavras comecando com prefixo.
    /// </summary>
    [FidelityFact]
    public void Match_PrefixWildcard_ShouldMatchPrefix()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH 'Jav*' ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Equal(2, ids.Count);
        Assert.Contains(2, ids);
        Assert.Contains(4, ids);
    }

    /// <summary>
    /// EN: Verifies MATCH with phrase search matches exact phrase.
    /// PT-br: Verifica que MATCH com busca por frase encontra frase exata.
    /// </summary>
    [FidelityFact]
    public void Match_PhraseSearch_ShouldMatchExactPhrase()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Body MATCH '\"data analysis\"' ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(3, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH with NEAR operator searches for proximity.
    /// PT-br: Verifica que MATCH com operador NEAR busca por proximidade.
    /// </summary>
    [FidelityFact]
    public void Match_NearOperator_ShouldMatchProximity()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Body MATCH 'NEAR(C# .NET)' ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH with NOT operator excludes terms.
    /// PT-br: Verifica que MATCH com operador NOT exclui termos.
    /// </summary>
    [FidelityFact]
    public void Match_NotOperator_ShouldExcludeTerm()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH 'C# NOT Advanced' ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH with OR operator matches any term.
    /// PT-br: Verifica que MATCH com operador OR encontra qualquer termo.
    /// </summary>
    [FidelityFact]
    public void Match_OrOperator_ShouldMatchAnyTerm()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH 'Java OR Python' ORDER BY Id"
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
    /// EN: Verifies MATCH with parameterized query text works end-to-end.
    /// PT-br: Verifica que MATCH com texto de consulta parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void Match_ParameterizedQuery_ShouldWork()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM Documents WHERE Title MATCH @query ORDER BY Id"
        };
        cmd.Parameters.Add(new SqliteParameter("@query", "JavaScript"));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(4, ids[0]);
    }

    /// <summary>
    /// EN: Verifies MATCH with column expression and parameterized LIMIT works end-to-end.
    /// PT-br: Verifica que MATCH com expressao de coluna e LIMIT parametrizado funciona ponta a ponta.
    /// </summary>
    [FidelityFact]
    public void Match_WithParameterizedLimit_ShouldReturnTopMatched()
    {
        using var cnn = CreateOpenConnection();
        using var cmd = new SqliteCommandMock(cnn)
        {
            CommandText = """
                SELECT Id FROM Documents
                WHERE Title MATCH @query
                ORDER BY Id
                LIMIT @limit
                """
        };
        cmd.Parameters.Add(new SqliteParameter("@query", "C#"));
        cmd.Parameters.Add(new SqliteParameter("@limit", 1));

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.Single(ids);
        Assert.Equal(1, ids[0]);
    }
}
