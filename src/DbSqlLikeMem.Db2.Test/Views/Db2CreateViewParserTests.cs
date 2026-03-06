namespace DbSqlLikeMem.Db2.Test.Views;

/// <summary>
/// EN: Defines the class Db2CreateViewParserTests.
/// PT: Define a classe Db2CreateViewParserTests.
/// </summary>
public sealed class Db2CreateViewParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements behavior.
    /// PT: Testa o comportamento de ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements(int version)
    {
        const string sql = @"
CREATE VIEW v_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM v_users;
";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList();
        Assert.Equal(2, q.Count);

        Assert.IsType<SqlCreateViewQuery>(q[0]);
        Assert.IsType<SqlSelectQuery>(q[1]);

        var cv = (SqlCreateViewQuery)q[0];
        Assert.Equal("v_users", cv.Table?.Name);
        Assert.False(cv.OrReplace);
        Assert.NotNull(cv.Select);
        Assert.Contains("users", cv.Select.Table?.Name, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests Parse_CreateOrReplaceView_ShouldSetFlag behavior.
    /// PT: Testa o comportamento de Parse_CreateOrReplaceView_ShouldSetFlag.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateOrReplaceView_ShouldSetFlag(int version)
    {
        const string sql = "CREATE OR REPLACE VIEW v AS SELECT id FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.True(cv.OrReplace);
        Assert.Equal("v", cv.Table?.Name);
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames(int version)
    {
        const string sql = "CREATE VIEW v (a,b) AS SELECT id, name FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.Equal(["a", "b"], cv.ColumnNames);
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithBackticks_ShouldWork behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithBackticks_ShouldWork.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithBackticks_ShouldFail_ByDb2Spec(int version)
    {
        const string sql = "CREATE VIEW `v` AS SELECT `id` FROM `users`;";
        Assert.ThrowsAny<Exception>(() => SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList());
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_IfNotExists_ShouldBeRejected_ByDb2Spec behavior.
    /// PT: Testa o comportamento de Parse_CreateView_IfNotExists_ShouldBeRejected_ByDb2Spec.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_IfNotExists_ShouldBeRejected_ByDb2Spec(int version)
    {
        const string sql = "CREATE VIEW IF NOT EXISTS v AS SELECT 1;";
        Assert.ThrowsAny<Exception>(() => SqlQueryParser.ParseMulti(sql, new Db2Dialect(version)).ToList());
    }

    /// <summary>
    /// EN: Tests Parse_DropView_WithUnexpectedContinuation_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropView_WithUnexpectedContinuation_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithUnexpectedContinuation_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW v_users EXTRA";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithUnexpectedSecondStatementInBody_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithUnexpectedSecondStatementInBody_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnexpectedSecondStatementInBody_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users AS SELECT id FROM users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithMissingBodyAfterAs_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithMissingBodyAfterAs_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithMissingBodyAfterAs_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users AS ;";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_DropView_WithoutName_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropView_WithoutName_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithoutName_ShouldThrow(int version)
    {
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse("DROP VIEW ;", new Db2Dialect(version)));
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse("DROP VIEW IF EXISTS ;", new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithEmptyColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithEmptyColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithEmptyColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users () AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithTrailingCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithTrailingCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithTrailingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id,) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithLeadingCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithLeadingCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithLeadingCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (,id) AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithUnclosedColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithUnclosedColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnclosedColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithMissingCommaBetweenColumns_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithMissingCommaBetweenColumns_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithMissingCommaBetweenColumns_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id name) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_DropView_WithUnexpectedSecondStatement_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropView_WithUnexpectedSecondStatement_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropView_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW v_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_DropViewIfExists_WithUnexpectedSecondStatement_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_DropViewIfExists_WithUnexpectedSecondStatement_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_DropViewIfExists_WithUnexpectedSecondStatement_ShouldThrow(int version)
    {
        const string sql = "DROP VIEW IF EXISTS v_users; SELECT 1";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithDoubleCommaInColumnList_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithDoubleCommaInColumnList_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithDoubleCommaInColumnList_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id,,name) AS SELECT id, name FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests Parse_CreateView_WithUnclosedColumnListBeforeAs_ShouldThrow behavior.
    /// PT: Testa o comportamento de Parse_CreateView_WithUnclosedColumnListBeforeAs_ShouldThrow.
    /// </summary>
    [Theory]
    [Trait("Category", "Views")]
    [MemberDataDb2Version]
    public void Parse_CreateView_WithUnclosedColumnListBeforeAs_ShouldThrow(int version)
    {
        const string sql = "CREATE VIEW v_users (id AS SELECT id FROM users";
        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }
}
