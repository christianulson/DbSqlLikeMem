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

    /// <summary>
    /// EN: Tests Parse_CreateOrReplaceTable_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateOrReplaceTable_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateOrReplaceTable_ShouldThrow(int version)
    {
        const string sql = "CREATE OR REPLACE TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_DropTable_IfExists behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_DropTable_IfExists.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_ShouldAccept_DropTable_IfExists(int version)
    {
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP TABLE IF EXISTS tmp_users", new Db2Dialect(version)));

        Assert.True(q.IfExists);
        Assert.NotNull(q.Table);
        Assert.Equal("tmp_users", q.Table!.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Tests Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists behavior.
    /// PT: Testa o comportamento de Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_ShouldAccept_DropGlobalTemporaryTable_IfExists(int version)
    {
        var q = Assert.IsType<SqlDropTableQuery>(
            SqlQueryParser.Parse("DROP GLOBAL TEMPORARY TABLE IF EXISTS tmp_users", new Db2Dialect(version)));

        Assert.True(q.IfExists);
        Assert.True(q.Temporary);
        Assert.Equal(TemporaryTableScope.Global, q.Scope);
    }

    /// <summary>
    /// EN: Tests Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_DropGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        const string sql = "DROP GLOBAL TABLE IF EXISTS tmp_users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }


    /// <summary>
    /// EN: Tests Parse_DropTable_WithoutName_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropTable_WithoutName_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_DropTable_WithoutName_ShouldThrow(int version)
    {
        const string sql = "DROP TABLE IF EXISTS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_DropTable_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        const string sql = "DROP TABLE IF EXISTS tmp_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateGlobalTable_WithoutTemporaryKeyword_ShouldThrow(int version)
    {
        const string sql = "CREATE GLOBAL TABLE tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS SELECT id FROM users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users AS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithEmptyColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users () AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (,id INT) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithUnclosedColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithMissingCommaAfterParenthesizedType_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT, name VARCHAR(50) age INT) AS SELECT id, name, age FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE tmp_users (id INT,,name VARCHAR(50)) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "TemporaryTable")]
    [MemberDataDb2Version]
    public void Parse_CreateTemporaryTable_WithIfExistsInsteadOfIfNotExists_ShouldThrow(int version)
    {
        const string sql = "CREATE TEMPORARY TABLE IF EXISTS tmp_users AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }
}

