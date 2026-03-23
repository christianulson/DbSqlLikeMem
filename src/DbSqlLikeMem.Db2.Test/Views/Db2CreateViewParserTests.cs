namespace DbSqlLikeMem.Db2.Test.Views;

/// <summary>
/// EN: Covers CREATE VIEW parsing scenarios in the Db2 dialect.
/// PT: Cobre cenarios de parsing de CREATE VIEW no dialeto Db2.
/// </summary>
public sealed class Db2CreateViewParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that CREATE VIEW followed by SELECT is parsed as two statements.
    /// PT: Verifica se CREATE VIEW seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements(int version)
    {
        const string sql = @"
CREATE VIEW v_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM v_users;
";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList();
        Assert.Equal(2, q.Count);

        Assert.IsType<SqlCreateViewQuery>(q[0]);
        Assert.IsType<SqlSelectQuery>(q[1]);

        var cv = (SqlCreateViewQuery)q[0];
        Assert.Equal("v_users", cv.Table?.Name);
        Assert.False(cv.OrReplace);
        Assert.NotNull(cv.Select);
        Assert.Contains("users", cv.Select.Table?.Name, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies that CREATE OR REPLACE VIEW sets the replace flag.
    /// PT: Verifica se CREATE OR REPLACE VIEW define a flag de replace.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateOrReplaceView_ShouldSetFlag(int version)
    {
        const string sql = "CREATE OR REPLACE VIEW v AS SELECT id FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.True(cv.OrReplace);
        Assert.Equal("v", cv.Table?.Name);
    }

    /// <summary>
    /// EN: Verifies that explicit view column names are captured.
    /// PT: Verifica se nomes explicitos de colunas da view sao capturados.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames(int version)
    {
        const string sql = "CREATE VIEW v (a,b) AS SELECT id, name FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.Equal(["a", "b"], cv.ColumnNames);
    }

    /// <summary>
    /// EN: Verifies that CREATE VIEW with a simple identifier parses.
    /// PT: Verifica se CREATE VIEW com identificador simples faz parse.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithBackticks_ShouldFail_ByDb2Spec(int version)
    {
        const string sql = "CREATE VIEW `v` AS SELECT `id` FROM `users`;";
        Assert.ThrowsAny<Exception>(() => SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList());
    }

    /// <summary>
    /// EN: Verifies that IF NOT EXISTS is rejected for CREATE VIEW.
    /// PT: Verifica se IF NOT EXISTS e rejeitado em CREATE VIEW.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_IfNotExists_ShouldBeRejected_ByDb2Spec(int version)
    {
        const string sql = "CREATE VIEW IF NOT EXISTS v AS SELECT 1;";
        Assert.ThrowsAny<Exception>(() => SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList());
    }

    /// <summary>
    /// EN: Verifies that extra tokens after DROP VIEW raise an error.
    /// PT: Verifica se tokens extras apos DROP VIEW geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithUnexpectedContinuation_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW v_users EXTRA";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that an extra statement after the CREATE VIEW body raises an error.
    /// PT: Verifica se uma instrucao extra apos o corpo de CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users AS SELECT id FROM users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that a CREATE VIEW body missing after AS raises an error.
    /// PT: Verifica se um corpo ausente apos AS em CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users AS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that DROP VIEW without a name raises an error.
    /// PT: Verifica se DROP VIEW sem nome gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithoutName_ShouldThrow(int version)
    {
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse("DROP VIEW ;", new Db2Dialect(version)));
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse("DROP VIEW IF EXISTS ;", new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that an empty CREATE VIEW column list raises an error.
    /// PT: Verifica se uma lista vazia de colunas em CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithEmptyColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users () AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that a trailing comma in the view column list raises an error.
    /// PT: Verifica se uma virgula final na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id,) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that a leading comma in the view column list raises an error.
    /// PT: Verifica se uma virgula inicial na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (,id) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that an unclosed view column list raises an error.
    /// PT: Verifica se uma lista de colunas de view nao fechada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnclosedColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that missing commas between view columns raise an error.
    /// PT: Verifica se virgulas ausentes entre colunas da view geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id name) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP VIEW raise an error.
    /// PT: Verifica se instrucoes extras apos DROP VIEW geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW v_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP VIEW IF EXISTS raise an error.
    /// PT: Verifica se instrucoes extras apos DROP VIEW IF EXISTS geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropViewIfExists_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW IF EXISTS v_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that a double comma in the view column list raises an error.
    /// PT: Verifica se uma virgula dupla na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id,,name) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Verifies that an unclosed column list before AS raises an error.
    /// PT: Verifica se uma lista de colunas nao fechada antes de AS gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnclosedColumnListBeforeAs_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }
}
