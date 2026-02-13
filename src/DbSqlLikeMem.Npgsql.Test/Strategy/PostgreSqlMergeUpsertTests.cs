namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Covers PostgreSQL MERGE upsert behavior in dialect versions that support it.
/// PT: Cobre o comportamento de upsert com MERGE no PostgreSQL em versões de dialeto com suporte.
/// </summary>
public sealed class PostgreSqlMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    [Theory]
    [MemberDataNpgsqlVersion]
    public void Merge_ShouldFollowDialectVersionSupport(int version)
    {
        var db = new NpgsqlDbMock(version);

        if (version < NpgsqlDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("MERGE INTO users t USING (SELECT 1 AS Id) s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET Name = 'x'", db.Dialect));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = SqlQueryParser.Parse(
            "MERGE INTO users t USING (SELECT 1 AS Id) s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET Name = 'x'",
            db.Dialect);

        Assert.IsType<SqlMergeQuery>(parsed);
    }

    [Fact]
    public void Merge_ShouldUpdate_WhenMatched()
    {
        var db = new NpgsqlDbMock(NpgsqlDialect.MergeMinVersion);
        var t = db.AddTable("users");
        t.Columns["Id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["Name"] = new ColumnDef(1, DbType.String, false);
        t.PrimaryKeyIndexes.Add(0);
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users AS target
USING (SELECT 1 AS Id, 'NEW' AS Name) AS src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name);";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }
}
