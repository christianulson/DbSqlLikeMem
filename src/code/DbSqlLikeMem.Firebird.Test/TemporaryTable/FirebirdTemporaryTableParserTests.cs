namespace DbSqlLikeMem.Firebird.Test.TemporaryTable;

/// <summary>
/// EN: Covers Firebird temporary table parsing scenarios in the mock dialect.
/// PT-br: Cobre cenarios de parsing de tabela temporaria Firebird no dialeto simulado.
/// </summary>
public sealed class FirebirdTemporaryTableParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies CREATE TEMPORARY TABLE followed by SELECT is parsed as two statements.
    /// PT-br: Verifica se CREATE TEMPORARY TABLE seguido de SELECT e parsed como duas instrucoes.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, db, dialect).ToList();

        Assert.Collection(queries,
            query => Assert.Contains("CREATE TEMPORARY TABLE", query.RawSql, StringComparison.OrdinalIgnoreCase),
            query =>
            {
                var select2 = Assert.IsType<SqlSelectQuery>(query);
                Assert.NotNull(select2.Table);
                Assert.Equal("tmp_users", select2.Table!.Name, ignoreCase: true);
            });
    }

    /// <summary>
    /// EN: Verifies supported CREATE TEMPORARY TABLE variants parse successfully.
    /// PT-br: Verifica se variantes suportadas de CREATE TEMPORARY TABLE sao parsed com sucesso.
    /// </summary>
    [Theory]
    [InlineData("CREATE TEMPORARY TABLE IF NOT EXISTS tmp_users AS SELECT id FROM users")]
    [InlineData("CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50)) AS SELECT id, name FROM users")]
    [InlineData("""
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name
FROM users
WHERE tenantid = 10
""")]
    [Trait("Category", "TemporaryTable")]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql)
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);

        var q = SqlQueryParser.Parse(sql, db, dialect);

        var temp = Assert.IsType<SqlCreateTemporaryTableQuery>(q);
        Assert.True(temp.Temporary);
        Assert.Equal(TemporaryTableScope.Connection, temp.Scope);
        Assert.NotNull(temp.AsSelect);
    }

    /// <summary>
    /// EN: Verifies DROP TEMPORARY TABLE IF EXISTS parses as a temporary drop.
    /// PT-br: Verifica se DROP TEMPORARY TABLE IF EXISTS e parsed como uma remocao temporaria.
    /// </summary>
    [Fact]
    [Trait("Category", "TemporaryTable")]
    public void Parse_ShouldAccept_DropTemporaryTable_IfExists()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);
        var db = new FirebirdDbMock(FirebirdDbVersions.Default);
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP TEMPORARY TABLE IF EXISTS tmp_users", db, dialect));

        Assert.True(q.IfExists);
        Assert.True(q.Temporary);
        Assert.Equal(TemporaryTableScope.Connection, q.Scope);
        Assert.NotNull(q.Table);
        Assert.Equal("tmp_users", q.Table!.Name, ignoreCase: true);
    }
}
