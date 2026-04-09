namespace DbSqlLikeMem.MySql.Test.TemporaryTable;

/// <summary>
/// EN: Covers CREATE TEMPORARY TABLE parsing scenarios in the MySql dialect.
/// PT: Cobre cenarios de parsing de CREATE TEMPORARY TABLE no dialeto MySql.
/// </summary>
public sealed class MySqlTemporaryTableParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that CREATE TEMPORARY TABLE followed by SELECT is parsed as two statements.
    /// PT: Verifica se CREATE TEMPORARY TABLE seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, db, d).ToList();

        // TDD contract: the parser must accept the batch and produce 2 statements.
        queries.Should().HaveCount(2);

        queries[0].RawSql.Should().Contain("CREATE TEMPORARY TABLE");

        var select2 = queries[1].Should().BeOfType<SqlSelectQuery>().Subject;
        select2.Table.Should().NotBeNull();
        select2.Table!.Name.Should().BeEquivalentTo("tmp_users");
    }

    /// <summary>
    /// EN: Provides CREATE TEMPORARY TABLE statement variants.
    /// PT: Fornece variantes de instrucao CREATE TEMPORARY TABLE.
    /// </summary>
    public static IEnumerable<object[]> CreateTempTableStatements()
    {
        yield return new object[]
        {
            // IF NOT EXISTS
            "CREATE TEMPORARY TABLE IF NOT EXISTS tmp_users AS SELECT id FROM users",
        };

        yield return new object[]
        {
            // explicit column list
            "CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50)) AS SELECT id, name FROM users",
        };

        yield return new object[]
        {
            // backticks + multiline select
            @"CREATE TEMPORARY TABLE `tmp_users` AS
SELECT `id`, `name`
FROM `users`
WHERE `tenantid` = 10",
        };
    }

    /// <summary>
    /// EN: Verifies that supported CREATE TEMPORARY TABLE variants parse successfully.
    /// PT: Verifica se variantes suportadas de CREATE TEMPORARY TABLE sao parsed com sucesso.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataByMySqlVersion(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, db, d);
        q.Should().NotBeNull();
        q.RawSql.Should().Contain(SqlConst.CREATE);
        q.RawSql.Should().Contain(SqlConst.TEMPORARY);
    }

    /// <summary>
    /// EN: Verifies that CREATE GLOBAL TEMPORARY TABLE is parsed with global scope.
    /// PT: Verifica se CREATE GLOBAL TEMPORARY TABLE e parsed com escopo global.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_ShouldAccept_GlobalTemporaryTable(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var q = SqlQueryParser.Parse("CREATE GLOBAL TEMPORARY TABLE tmp_users AS SELECT id FROM users", db, d)
            .Should().BeOfType<SqlCreateTemporaryTableQuery>().Subject;

        q.Scope.Should().Be(TemporaryTableScope.Global);
    }

    /// <summary>
    /// EN: Verifies that CREATE OR REPLACE TABLE is rejected for temporary tables.
    /// PT: Verifica se CREATE OR REPLACE TABLE e rejeitado para tabelas temporarias.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateOrReplaceTable_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE OR REPLACE TABLE tmp_users AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<NotSupportedException>();
    }

    /// <summary>
    /// EN: Verifies that extra statements after a temporary table body raise an error.
    /// PT: Verifica se instrucoes extras apos o corpo da tabela temporaria geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS SELECT id FROM users; SELECT 1";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a missing SELECT body after AS raises an error.
    /// PT: Verifica se um corpo SELECT ausente apos AS gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS ;";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an empty temporary table column list raises an error.
    /// PT: Verifica se uma lista vazia de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users () AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a trailing comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula final na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,) AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a leading comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula inicial na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (,id INT) AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that an unclosed temporary table column list raises an error.
    /// PT: Verifica se uma lista de colunas da tabela temporaria nao fechada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that missing commas between temporary table columns raise an error.
    /// PT: Verifica se virgulas ausentes entre colunas da tabela temporaria geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT name VARCHAR(50)) AS SELECT id, name FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a missing comma after a typed column raises an error.
    /// PT: Verifica se uma virgula ausente apos uma coluna tipada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50) age INT) AS SELECT id, name, age FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that a double comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula dupla na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,,name VARCHAR(50)) AS SELECT id, name FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that IF EXISTS is rejected for CREATE TEMPORARY TABLE.
    /// PT: Verifica se IF EXISTS e rejeitado em CREATE TEMPORARY TABLE.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE IF EXISTS tmp_users AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that DROP TABLE IF EXISTS parses the table name.
    /// PT: Verifica se DROP TABLE IF EXISTS faz o parse do nome da tabela.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_ShouldAccept_DropTable_IfExists(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var q = SqlQueryParser.Parse("DROP TABLE IF EXISTS tmp_users", db, d)
            .Should().BeOfType<SqlDropTableQuery>().Subject;

        q.IfExists.Should().BeTrue();
        q.Table.Should().NotBeNull();
        q.Table!.Name.Should().BeEquivalentTo("tmp_users");
    }

    /// <summary>
    /// EN: Verifies that DROP GLOBAL TEMPORARY TABLE IF EXISTS parses as a global temporary drop.
    /// PT: Verifica se DROP GLOBAL TEMPORARY TABLE IF EXISTS e parsed como uma remocao temporaria global.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var q = SqlQueryParser.Parse("DROP GLOBAL TEMPORARY TABLE IF EXISTS tmp_users", db, d)
            .Should().BeOfType<SqlDropTableQuery>().Subject;

        q.IfExists.Should().BeTrue();
        q.Temporary.Should().BeTrue();
        q.Scope.Should().Be(TemporaryTableScope.Global);
    }

    /// <summary>
    /// EN: Verifies that DROP GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se DROP GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP GLOBAL TABLE IF EXISTS tmp_users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }


    /// <summary>
    /// EN: Verifies that DROP TABLE without a name raises an error.
    /// PT: Verifica se DROP TABLE sem nome gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_DropTable_WithoutName_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP TABLE IF EXISTS ;";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP TABLE raise an error.
    /// PT: Verifica se instrucoes extras apos DROP TABLE geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "DROP TABLE IF EXISTS tmp_users; SELECT 1";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// EN: Verifies that CREATE GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se CREATE GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataMySqlVersion]
    public void Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        const string sql = "CREATE GLOBAL TABLE tmp_users AS SELECT id FROM users";
        Action act = () => SqlQueryParser.Parse(sql, db, d);
        act.Should().Throw<InvalidOperationException>();
    }
}

