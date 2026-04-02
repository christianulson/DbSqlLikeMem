namespace DbSqlLikeMem.SqlServer.Test.TemporaryTable;

/// <summary>
/// EN: Covers temporary table parser rules for the SqlServer dialect.
/// PT: Cobre as regras de parser de tabelas temporarias para o dialeto SqlServer.
/// </summary>
public sealed class SqlServerTemporaryTableParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies CREATE TEMPORARY TABLE followed by SELECT is parsed as a multi-statement batch.
    /// PT: Verifica se CREATE TEMPORARY TABLE seguido de SELECT e parseado como um lote com multiplas instrucoes.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
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
    /// EN: Provides test data for CreateTempTableStatements.
    /// PT: Fornece dados de teste para CreateTempTableStatements.
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
    /// EN: Verifies temporary table creation variants are accepted.
    /// PT: Verifica se variantes de criacao de tabela temporaria sao aceitas.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataBySqlServerVersion(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, db, d);
        Assert.NotNull(q);
        Assert.Contains(SqlConst.CREATE, q.RawSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(SqlConst.TEMPORARY, q.RawSql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies hash-based temporary table scopes are recognized.
    /// PT: Verifica se escopos de tabelas temporarias baseados em hash sao reconhecidos.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_ShouldRecognize_HashTempTableScopes(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));

        var localTemp = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE TABLE #tmp_users AS SELECT id FROM users", db, d));
        Assert.Equal(TemporaryTableScope.Connection, localTemp.Scope);

        var globalTemp = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE TABLE ##tmp_users AS SELECT id FROM users", db, d));
        Assert.Equal(TemporaryTableScope.Global, globalTemp.Scope);
    }

    /// <summary>
    /// EN: Verifies CREATE OR REPLACE TABLE is rejected.
    /// PT: Verifica se CREATE OR REPLACE TABLE e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateOrReplaceTable_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE OR REPLACE TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies unexpected statements after CREATE TEMPORARY TABLE body are rejected.
    /// PT: Verifica se instrucoes inesperadas apos o corpo de CREATE TEMPORARY TABLE sao rejeitadas.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS SELECT id FROM users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies missing CREATE TEMPORARY TABLE body after AS is rejected.
    /// PT: Verifica se a ausencia do corpo de CREATE TEMPORARY TABLE apos AS e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies an empty column list is rejected.
    /// PT: Verifica se uma lista de colunas vazia e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users () AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies a trailing comma in the column list is rejected.
    /// PT: Verifica se uma virgula no final da lista de colunas e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies a leading comma in the column list is rejected.
    /// PT: Verifica se uma virgula no inicio da lista de colunas e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (,id INT) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies an unclosed column list is rejected.
    /// PT: Verifica se uma lista de colunas sem fechamento e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies a missing comma between columns is rejected.
    /// PT: Verifica se a ausencia de virgula entre colunas e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies a missing comma after a parenthesized type is rejected.
    /// PT: Verifica se a ausencia de virgula apos um tipo entre parenteses e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50) age INT) AS SELECT id, name, age FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies a double comma in the column list is rejected.
    /// PT: Verifica se uma virgula dupla na lista de colunas e rejeitada.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,,name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies IF EXISTS is rejected when IF NOT EXISTS is required.
    /// PT: Verifica se IF EXISTS e rejeitado quando IF NOT EXISTS e obrigatorio.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE TEMPORARY TABLE IF EXISTS tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies DROP TABLE IF EXISTS is accepted.
    /// PT: Verifica se DROP TABLE IF EXISTS e aceito.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_ShouldAccept_DropTable_IfExists(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP TABLE IF EXISTS tmp_users", db, d));

        Assert.True(q.IfExists);
        Assert.NotNull(q.Table);
        Assert.Equal("tmp_users", q.Table!.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Verifies DROP GLOBAL TEMPORARY TABLE IF EXISTS is accepted.
    /// PT: Verifica se DROP GLOBAL TEMPORARY TABLE IF EXISTS e aceito.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP GLOBAL TEMPORARY TABLE IF EXISTS tmp_users", db, d));

        Assert.True(q.IfExists);
        Assert.True(q.Temporary);
        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }

    /// <summary>
    /// EN: Verifies DROP GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se DROP GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "DROP GLOBAL TABLE IF EXISTS tmp_users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }


    /// <summary>
    /// EN: Verifies DROP TABLE without a table name is rejected.
    /// PT: Verifica se DROP TABLE sem nome de tabela e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_DropTable_WithoutName_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "DROP TABLE IF EXISTS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies unexpected statements after DROP TABLE are rejected.
    /// PT: Verifica se instrucoes inesperadas apos DROP TABLE sao rejeitadas.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "DROP TABLE IF EXISTS tmp_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies CREATE GLOBAL TABLE without TEMPORARY is rejected.
    /// PT: Verifica se CREATE GLOBAL TABLE sem TEMPORARY e rejeitado.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        var d = Get(version, v => new SqlServerDialect(v));
        var db = Get(version, v => new SqlServerDbMock(v));
        const string sql = "CREATE GLOBAL TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));
    }
}

