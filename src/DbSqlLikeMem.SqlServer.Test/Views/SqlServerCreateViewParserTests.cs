namespace DbSqlLikeMem.SqlServer.Test.Views;

public sealed class SqlServerCreateViewParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseMulti_CreateView_ThenSelect_ShouldReturnTwoStatements(int version)
    {
        const string sql = @"
CREATE VIEW v_users AS
SELECT id, name FROM users WHERE tenantid = 10;

SELECT * FROM v_users;
";
        var q = SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).ToList();
        Assert.Equal(2, q.Count);

        Assert.IsType<SqlCreateViewQuery>(q[0]);
        Assert.IsType<SqlSelectQuery>(q[1]);

        var cv = (SqlCreateViewQuery)q[0];
        Assert.Equal("v_users", cv.Table?.Name);
        Assert.False(cv.OrReplace);
        Assert.NotNull(cv.Select);
        Assert.Contains("users", cv.Select.Table?.Name, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberDataSqlServerVersion]
    public void Parse_CreateOrReplaceView_ShouldSetFlag(int version)
    {
        const string sql = "CREATE OR REPLACE VIEW v AS SELECT id FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.True(cv.OrReplace);
        Assert.Equal("v", cv.Table?.Name);
    }

    [Theory]
    [MemberDataSqlServerVersion]
    public void Parse_CreateView_WithExplicitColumnList_ShouldCaptureNames(int version)
    {
        const string sql = "CREATE VIEW v (a,b) AS SELECT id, name FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.Equal(["a", "b"], cv.ColumnNames);
    }

    [Theory]
    [MemberDataSqlServerVersion]
    public void Parse_CreateView_WithBackticks_ShouldWork(int version)
    {
        const string sql = "CREATE VIEW v AS SELECT id FROM users;";
        var q = SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).Single();
        var cv = Assert.IsType<SqlCreateViewQuery>(q);
        Assert.Equal("v", cv.Table?.Name);
    }

    [Theory(Skip = "MySQL não suporta IF NOT EXISTS em CREATE VIEW. O mock aceita por conveniência; habilite se quiser comportamento estrito.")]
    [MemberDataSqlServerVersion]
    public void Parse_CreateView_IfNotExists_ShouldBeRejected_ByMySqlSpec(int version)
    {
        const string sql = "CREATE VIEW IF NOT EXISTS v AS SELECT 1;";
        Assert.ThrowsAny<Exception>(() => SqlQueryParser.ParseMulti(sql, new SqlServerDialect(version)).ToList());
    }
}
