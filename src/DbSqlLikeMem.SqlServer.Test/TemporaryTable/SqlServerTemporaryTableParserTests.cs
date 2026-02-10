namespace DbSqlLikeMem.SqlServer.Test.TemporaryTable;

public sealed class SqlServerTemporaryTableParserTests
{
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).ToList();

        // TDD contract: the parser must accept the batch and produce 2 statements.
        Assert.Equal(2, queries.Count);

        Assert.Contains("CREATE TEMPORARY TABLE", queries[0].RawSql, StringComparison.OrdinalIgnoreCase);

        var select2 = Assert.IsType<SqlSelectQuery>(queries[1]);
        Assert.NotNull(select2.Table);
        Assert.Equal("tmp_users", select2.Table!.Name, ignoreCase: true);
    }

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

    [Theory]
    [MemberDataBySqlServerVersion(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, new SqlServerDialect(version));
        Assert.NotNull(q);
        Assert.Contains("CREATE", q.RawSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TEMPORARY", q.RawSql, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberDataSqlServerVersion]
    public void Parse_ShouldRecognize_HashTempTableScopes(int version)
    {
        var dialect = new SqlServerDialect(version);

        var localTemp = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE TABLE #tmp_users AS SELECT id FROM users", dialect));
        Assert.Equal(TemporaryTableScope.Connection, localTemp.Scope);

        var globalTemp = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE TABLE ##tmp_users AS SELECT id FROM users", dialect));
        Assert.Equal(TemporaryTableScope.Global, globalTemp.Scope);
    }
}
