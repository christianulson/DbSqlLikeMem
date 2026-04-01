using FluentAssertions;

namespace DbSqlLikeMem.MySql.Test.Views;

/// <summary>
/// EN: Covers CREATE VIEW parsing scenarios in the MySql dialect.
/// PT: Cobre cenarios de parsing de CREATE VIEW no dialeto MySql.
/// </summary>
public sealed class MySqlCreateViewParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that CREATE VIEW followed by SELECT is parsed as two statements.
    /// PT: Verifica se CREATE VIEW seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = @"
CREATE VIEW v_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM v_users;
";
        var q = SqlQueryParser.ParseMulti(sql, db, d).ToList();
        q.Should().HaveCount(2);

        q[0].Should().BeOfType<SqlCreateViewQuery>();
        q[1].Should().BeOfType<SqlSelectQuery>();

        var cv = q[0].Should().BeOfType<SqlCreateViewQuery>().Which;
        cv.Table?.Name.Should().Be("v_users");
        cv.OrReplace.Should().BeFalse();
        cv.Select.Should().NotBeNull();
        cv.Select!.Table?.Name.Should().Contain("users");
    }

    /// <summary>
    /// EN: Verifies that CREATE OR REPLACE VIEW sets the replace flag.
    /// PT: Verifica se CREATE OR REPLACE VIEW define a flag de replace.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateOrReplaceView_ShouldSetFlag(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE OR REPLACE VIEW v AS SELECT id FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, db, d).Single();
        var cv = q.Should().BeOfType<SqlCreateViewQuery>().Which;
        cv.OrReplace.Should().BeTrue();
        cv.Table?.Name.Should().Be("v");
    }

    /// <summary>
    /// EN: Verifies that explicit view column names are captured.
    /// PT: Verifica se nomes explicitos de colunas da view sao capturados.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v (a,b) AS SELECT id, name FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, db, d).Single();
        var cv = q.Should().BeOfType<SqlCreateViewQuery>().Which;
        cv.ColumnNames.Should().Equal(["a", "b"]);
    }

    /// <summary>
    /// EN: Verifies that backtick-delimited identifiers are parsed.
    /// PT: Verifica se identificadores delimitados por backticks sao parsed.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithBackticks_ShouldWork(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW `v` AS SELECT `id` FROM `users`;";
        var q = SqlQueryParser.ParseMulti(sql, db, d).Single();
        var cv = q.Should().BeOfType<SqlCreateViewQuery>().Which;
        cv.Table?.Name.Should().Be("v");
    }

    /// <summary>
    /// EN: Verifies that IF NOT EXISTS is rejected for CREATE VIEW.
    /// PT: Verifica se IF NOT EXISTS e rejeitado em CREATE VIEW.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_IfNotExists_ShouldBeRejected_ByMySqlSpec(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW IF NOT EXISTS v AS SELECT 1;";
        Action act = () => SqlQueryParser.ParseMulti(sql, db, d).ToList();
        act.Should().Throw<Exception>();
    }

    /// <summary>
    /// EN: Verifies that extra tokens after DROP VIEW raise an error.
    /// PT: Verifica se tokens extras apos DROP VIEW geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_DropView_WithUnexpectedContinuation_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP VIEW v_users EXTRA";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an extra statement after the CREATE VIEW body raises an error.
    /// PT: Verifica se uma instrucao extra apos o corpo de CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users AS SELECT id FROM users; SELECT 1";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a CREATE VIEW body missing after AS raises an error.
    /// PT: Verifica se um corpo ausente apos AS em CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users AS ;";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that DROP VIEW without a name raises an error.
    /// PT: Verifica se DROP VIEW sem nome gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_DropView_WithoutName_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        Action dropView = () => SqlQueryParser.Parse("DROP VIEW ;", db, d);
        Action dropViewIfExists = () => SqlQueryParser.Parse("DROP VIEW IF EXISTS ;", db, d);
        dropView.Should().Throw<InvalidOperationException>();
        dropViewIfExists.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an empty CREATE VIEW column list raises an error.
    /// PT: Verifica se uma lista vazia de colunas em CREATE VIEW gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithEmptyColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users () AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a trailing comma in the view column list raises an error.
    /// PT: Verifica se uma virgula final na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (id,) AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a leading comma in the view column list raises an error.
    /// PT: Verifica se uma virgula inicial na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (,id) AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an unclosed view column list raises an error.
    /// PT: Verifica se uma lista de colunas de view nao fechada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithUnclosedColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (id";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that missing commas between view columns raise an error.
    /// PT: Verifica se virgulas ausentes entre colunas da view geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (id name) AS SELECT id, name FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP VIEW raise an error.
    /// PT: Verifica se instrucoes extras apos DROP VIEW geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_DropView_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP VIEW v_users; SELECT 1";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP VIEW IF EXISTS raise an error.
    /// PT: Verifica se instrucoes extras apos DROP VIEW IF EXISTS geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_DropViewIfExists_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP VIEW IF EXISTS v_users; SELECT 1";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a double comma in the view column list raises an error.
    /// PT: Verifica se uma virgula dupla na lista de colunas da view gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (id,,name) AS SELECT id, name FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an unclosed column list before AS raises an error.
    /// PT: Verifica se uma lista de colunas nao fechada antes de AS gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataMySqlVersion]
    public void Parse_CreateView_WithUnclosedColumnListBeforeAs_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE VIEW v_users (id AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }
}
