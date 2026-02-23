namespace DbSqlLikeMem.Db2.Test.TemporaryTable;

/// <summary>
/// EN: Defines the class Db2TemporaryTableParserTests.
/// PT: Define a classe Db2TemporaryTableParserTests.
/// </summary>
public sealed class Db2TemporaryTableParserTests
{
    /// <summary>
    /// EN: Tests ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect behavior.
    /// PT: Testa o comportamento de ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void ParseMulti_ShouldAccept_CreateTemporaryTable_AsSelect_FollowedBySelect(int version)
    {
        const string sql = @"
CREATE TEMPORARY TABLE tmp_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM tmp_users;
";

        var queries = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList();

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

    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_CreateTemporaryTable_Variants behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_CreateTemporaryTable_Variants.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataByDb2Version(nameof(CreateTempTableStatements))]
    public void Parse_ShouldAccept_CreateTemporaryTable_Variants(string sql, int version)
    {
        // TDD contract: these statements must parse without throwing.
        var q = SqlQueryParser.Parse(sql, new Db2Dialect(version));
        Assert.NotNull(q);
        Assert.Contains("CREATE", q.RawSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TEMPORARY", q.RawSql, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Tests Parse_ShouldReject_Backticks_ByDb2Spec behavior.
    /// PT: Testa o comportamento de Parse_ShouldReject_Backticks_ByDb2Spec.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_ShouldReject_Backticks_ByDb2Spec(int version)
    {
        const string sql = @"CREATE TEMPORARY TABLE `tmp_users` AS
SELECT `id`, `name`
FROM `users`
WHERE `tenantid` = 10";

        Assert.ThrowsAny<Exception>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_GlobalTemporaryTable behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_GlobalTemporaryTable.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_ShouldAccept_GlobalTemporaryTable(int version)
    {
        var dialect = new Db2Dialect(version);
        var q = Assert.IsType<SqlCreateTemporaryTableQuery>(
            SqlQueryParser.Parse("CREATE GLOBAL TEMPORARY TABLE tmp_users AS SELECT id FROM users", dialect));

        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }
}
