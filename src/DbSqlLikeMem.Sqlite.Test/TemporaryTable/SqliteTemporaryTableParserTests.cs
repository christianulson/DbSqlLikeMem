namespace DbSqlLikeMem.Sqlite.Test.TemporaryTable;

/// <summary>
/// EN: Defines the class SqliteTemporaryTableParserTests.
/// PT: Define a classe SqliteTemporaryTableParserTests.
/// </summary>
public sealed class SqliteTemporaryTableParserTests
{
    /// <summary>
    /// EN: Tests ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect behavior.
    /// PT: Testa o comportamento de ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqliteVersion]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, new SqliteDialect(version)).ToList();

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
            @"CREATE TEMPORARY TABLE `tmp_users` AS
SELECT `id`, `name`
FROM `users`
WHERE `tenantid` = 10",
        };
    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_CreateTemporaryTable_Variants behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_CreateTemporaryTable_Variants.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataBySqliteVersion(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, new SqliteDialect(version));
        Assert.NotNull(q);
        Assert.Contains("CREATE", q.RawSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TEMPORARY", q.RawSql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_GlobalTemporaryTable behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_GlobalTemporaryTable.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataSqliteVersion]
    public void Parse_ShouldAccept_GlobalTemporaryTable(int version)
    {
        var dialect = new SqliteDialect(version);
        var q = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE GLOBAL TEMPORARY TABLE tmp_users AS SELECT id FROM users", dialect));

        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }
}
