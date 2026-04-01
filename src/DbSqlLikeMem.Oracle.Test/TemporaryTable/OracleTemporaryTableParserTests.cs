namespace DbSqlLikeMem.Oracle.Test.TemporaryTable;

/// <summary>
/// EN: Covers CREATE TEMPORARY TABLE parsing scenarios in the Oracle dialect.
/// PT: Cobre cenarios de parsing de CREATE TEMPORARY TABLE no dialeto Oracle.
/// </summary>
public sealed class OracleTemporaryTableParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that CREATE TEMPORARY TABLE followed by SELECT is parsed as two statements.
    /// PT: Verifica se CREATE TEMPORARY TABLE seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, db, d).ToList();

        // TDD contract: the parser must accept the batch and produce 2 statements.
        Assert.Equal(2, queries.Count);

        Assert.Contains("CREATE TEMPORARY TABLE", queries[0].RawSql, StringComparison.OrdinalIgnoreCase);

        var select2 = Assert.IsType<SqlSelectQuery>(queries[1]);
        Assert.NotNull(select2.Table);
        Assert.Equal("tmp_users", select2.Table!.Name, ignoreCase: true);
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
            @"CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name
FROM users
WHERE tenantid = 10",
        };
    }

    /// <summary>
    /// EN: Verifies that supported CREATE TEMPORARY TABLE variants parse successfully.
    /// PT: Verifica se variantes suportadas de CREATE TEMPORARY TABLE sao parsed com sucesso.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataByOracleVersion(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, db, d);
        Assert.NotNull(q);
        Assert.Contains(SqlConst.CREATE, q.RawSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(SqlConst.TEMPORARY, q.RawSql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies that GLOBAL TEMPORARY TABLE syntax is accepted and mapped to global scope.
    /// PT: Verifica se a sintaxe GLOBAL TEMPORARY TABLE é aceita e mapeada para escopo global.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_ShouldAccept_GlobalTemporaryTable(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var q = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE GLOBAL TEMPORARY TABLE tmp_users AS SELECT id FROM users", db, d));

        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }

    /// <summary>
    /// EN: Verifies that CREATE OR REPLACE TABLE is rejected for temporary tables.
    /// PT: Verifica se CREATE OR REPLACE TABLE e rejeitado para tabelas temporarias.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateOrReplaceTable_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE OR REPLACE TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that DROP TABLE IF EXISTS parses the table name.
    /// PT: Verifica se DROP TABLE IF EXISTS faz o parse do nome da tabela.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_ShouldAccept_DropTable_IfExists(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP TABLE IF EXISTS tmp_users", db, d));

        Assert.True(q.IfExists);
        Assert.NotNull(q.Table);
        Assert.Equal("tmp_users", q.Table!.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Verifies that DROP GLOBAL TEMPORARY TABLE IF EXISTS parses as a global temporary drop.
    /// PT: Verifica se DROP GLOBAL TEMPORARY TABLE IF EXISTS e parsed como uma remocao temporaria global.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP GLOBAL TEMPORARY TABLE IF EXISTS tmp_users", db, d));

        Assert.True(q.IfExists);
        Assert.True(q.Temporary);
        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }

    /// <summary>
    /// EN: Verifies that DROP GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se DROP GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "DROP GLOBAL TABLE IF EXISTS tmp_users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }


    /// <summary>
    /// EN: Verifies that DROP TABLE without a name raises an error.
    /// PT: Verifica se DROP TABLE sem nome gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_DropTable_WithoutName_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "DROP TABLE IF EXISTS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that extra statements after DROP TABLE raise an error.
    /// PT: Verifica se instrucoes extras apos DROP TABLE geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "DROP TABLE IF EXISTS tmp_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that CREATE GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se CREATE GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE GLOBAL TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that extra statements after a temporary table body raise an error.
    /// PT: Verifica se instrucoes extras apos o corpo da tabela temporaria geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS SELECT id FROM users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that a missing SELECT body after AS raises an error.
    /// PT: Verifica se um corpo SELECT ausente apos AS gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that an empty temporary table column list raises an error.
    /// PT: Verifica se uma lista vazia de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users () AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that a trailing comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula final na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that a leading comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula inicial na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (,id INT) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that an unclosed temporary table column list raises an error.
    /// PT: Verifica se uma lista de colunas da tabela temporaria nao fechada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that missing commas between temporary table columns raise an error.
    /// PT: Verifica se virgulas ausentes entre colunas da tabela temporaria geram erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that a missing comma after a typed column raises an error.
    /// PT: Verifica se uma virgula ausente apos uma coluna tipada gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50) age INT) AS SELECT id, name, age FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that a double comma in the temporary table column list raises an error.
    /// PT: Verifica se uma virgula dupla na lista de colunas da tabela temporaria gera erro.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,,name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies that IF EXISTS is rejected for CREATE TEMPORARY TABLE.
    /// PT: Verifica se IF EXISTS e rejeitado em CREATE TEMPORARY TABLE.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataOracleVersion]
    public void Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE IF EXISTS tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }
}

