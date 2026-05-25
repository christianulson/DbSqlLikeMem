namespace DbSqlLikeMem.Firebird.Test.Views;

/// <summary>
/// EN: Covers Firebird CREATE VIEW parsing scenarios in the mock dialect.
/// PT-br: Cobre cenarios de parsing de CREATE VIEW no dialeto simulado Firebird.
/// </summary>
public sealed class FirebirdCreateViewParserTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies CREATE VIEW followed by SELECT is parsed as two statements.
    /// PT-br: Verifica se CREATE VIEW seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        const string sql = @"
CREATE VIEW v_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM v_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, db, dialect).ToList();

        Assert.Collection(queries,
            query =>
            {
                var createView = Assert.IsType<SqlCreateViewQuery>(query);
                Assert.Equal("v_users", createView.Table?.Name);
                Assert.False(createView.OrReplace);
                Assert.NotNull(createView.Select);
                Assert.Contains("users", createView.Select.Table!.Name, StringComparison.OrdinalIgnoreCase);
            },
            query => Assert.IsType<SqlSelectQuery>(query));
    }

    /// <summary>
    /// EN: Verifies explicit view column names are captured by the parser.
    /// PT-br: Verifica se nomes explicitos de colunas da view sao capturados pelo parser.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        const string sql = "CREATE VIEW v_users (a,b) AS SELECT id, name FROM users;";

        var query = SqlQueryParser.Parse(sql, db, dialect);

        var createView = Assert.IsType<SqlCreateViewQuery>(query);
        Assert.Equal(["a", "b"], createView.ColumnNames);
    }

    /// <summary>
    /// EN: Verifies CREATE OR ALTER VIEW parses as a replacing view definition.
    /// PT-br: Verifica se CREATE OR ALTER VIEW faz parse como uma definicao de view substituivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void Parse_CreateOrAlterView_ShouldSetReplaceFlag()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        const string sql = "CREATE OR ALTER VIEW v_users AS SELECT id FROM users;";

        var query = SqlQueryParser.Parse(sql, db, dialect);

        var createView = Assert.IsType<SqlCreateViewQuery>(query);
        Assert.True(createView.OrReplace);
        Assert.Equal("v_users", createView.Table?.Name);
    }

    /// <summary>
    /// EN: Verifies RECREATE VIEW parses as a replacing view definition.
    /// PT-br: Verifica se RECREATE VIEW faz parse como uma definicao de view substituivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void Parse_RecreateView_ShouldSetReplaceFlag()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        const string sql = "RECREATE VIEW v_users AS SELECT id FROM users;";

        var query = SqlQueryParser.Parse(sql, db, dialect);

        var createView = Assert.IsType<SqlCreateViewQuery>(query);
        Assert.True(createView.OrReplace);
        Assert.Equal("v_users", createView.Table?.Name);
    }

    /// <summary>
    /// EN: Verifies DROP VIEW IF EXISTS parses successfully.
    /// PT-br: Verifica se DROP VIEW IF EXISTS faz parse com sucesso.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void Parse_DropView_IfExists_ShouldWork()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var query = SqlQueryParser.Parse("DROP VIEW IF EXISTS v_users;", db, dialect);

        var dropView = Assert.IsType<SqlDropViewQuery>(query);
        Assert.True(dropView.IfExists);
        Assert.Equal("v_users", dropView.Table?.Name);
    }
}
