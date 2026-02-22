namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers statement splitting edge cases that are hotspots in parser execution.
/// PT: Cobre cenários de divisão de statements que são hotspots na execução do parser.
/// </summary>
public sealed class SqlQueryParserSplitStatementsTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures semicolons inside string literals, parentheses and backtick identifiers do not split statements.
    /// PT: Garante que ponto e vírgula dentro de strings, parênteses e identificadores com crase não dividam statements.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void SplitStatementsTopLevel_ShouldIgnoreSemicolonsInsideNestedContexts(int version)
    {
        var dialect = new MySqlDialect(version);
        var sql = "SELECT ';' AS txt, CONCAT('a;', 'b') AS c FROM `semi;table`; SELECT 2;";

        var parts = SqlQueryParser.SplitStatementsTopLevel(sql, dialect);

        Assert.Equal(2, parts.Count);
        Assert.Equal("SELECT ';' AS txt, CONCAT('a;', 'b') AS c FROM `semi;table`", parts[0]);
        Assert.Equal("SELECT 2", parts[1]);
    }

    /// <summary>
    /// EN: Ensures parser keeps INSERT ... SELECT ... ON DUPLICATE KEY UPDATE as a single statement boundary.
    /// PT: Garante que o parser mantenha INSERT ... SELECT ... ON DUPLICATE KEY UPDATE como um único statement.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsertSelectWithOnDuplicate_ShouldRemainSingleStatement(int version)
    {
        var dialect = new MySqlDialect(version);
        var sql = "INSERT INTO users (Id, Name)\n"
                + "SELECT Id, Name FROM users_archive\n"
                + "ON DUPLICATE KEY UPDATE Name = VALUES(Name);\n"
                + "SELECT COUNT(*) FROM users;";

        var parts = SqlQueryParser.SplitStatementsTopLevel(sql, dialect);

        Assert.Equal(2, parts.Count);
        var insertAst = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(parts[0], dialect));

        Assert.NotNull(insertAst.InsertSelect);
        Assert.True(insertAst.HasOnDuplicateKeyUpdate);
    }
}
