namespace DbSqlLikeMem.SqlServer.Dapper.Test;

public sealed class SqlServerFunctionHotspotCoverageTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    public SqlServerFunctionHotspotCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("fn_data");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("name", DbType.String, false);
        t.AddColumn("email", DbType.String, true);
        t.AddColumn("payload", DbType.String, true);
        t.AddColumn("created", DbType.DateTime, false);

        t.Add(new Dictionary<int, object?>
        {
            [0] = 1,
            [1] = "John",
            [2] = null,
            [3] = "{\"a\":{\"b\":42}}",
            [4] = new DateTime(2020, 1, 1)
        });

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void Cast_And_TryCast_ShouldFollowExpectedFallbacks()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT CAST('abc' AS INT) AS cast_value, TRY_CAST('abc' AS INT) AS try_cast_value");

        Assert.Equal(0, (int)row.cast_value);
        Assert.Null((object?)row.try_cast_value);
    }

    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void OpenJson_ConcatWs_And_DateAdd_ShouldBeEvaluated()
    {
        var row = _cnn.QuerySingle<dynamic>(@"
SELECT
    OPENJSON(payload) AS json_text,
    CONCAT_WS('-', name, email, 'end') AS joined,
    DATEADD(DAY, 2, created) AS plus_two_days
FROM fn_data
WHERE id = 1");

        Assert.Equal("{\"a\":{\"b\":42}}", (string)row.json_text);
        Assert.Equal("John-end", (string)row.joined);
        Assert.Equal(new DateTime(2020, 1, 3), (DateTime)row.plus_two_days);
    }

    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
