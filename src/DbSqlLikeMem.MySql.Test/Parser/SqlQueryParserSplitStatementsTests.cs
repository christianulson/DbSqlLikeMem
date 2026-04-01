using FluentAssertions;

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
        var dialect = Get(version, v => new MySqlDialect(v));
        var sql = "SELECT ';' AS txt, CONCAT('a;', 'b') AS c FROM `semi;table`; SELECT 2;";

        var parts = SqlStatementSplitter.SplitStatementsTopLevel(sql, dialect);

        parts.Should().HaveCount(2);
        parts[0].Should().Be("SELECT ';' AS txt, CONCAT('a;', 'b') AS c FROM `semi;table`");
        parts[1].Should().Be("SELECT 2");
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
        var d = Get(version, v => new MySqlDialect(v));
        var db = Get(version, v => new MySqlDbMock(v));
        var sql = "INSERT INTO users (Id, Name)\n"
                + "SELECT Id, Name FROM users_archive\n"
                + "ON DUPLICATE KEY UPDATE Name = VALUES(Name);\n"
                + "SELECT COUNT(*) FROM users;";

        var parts = SqlStatementSplitter.SplitStatementsTopLevel(sql, d);

        parts.Should().HaveCount(2);
        var insertAst = SqlQueryParser.Parse(parts[0], db, d).Should().BeOfType<SqlInsertQuery>().Which;

        insertAst.InsertSelect.Should().NotBeNull();
        insertAst.HasOnDuplicateKeyUpdate.Should().BeTrue();
    }
}
