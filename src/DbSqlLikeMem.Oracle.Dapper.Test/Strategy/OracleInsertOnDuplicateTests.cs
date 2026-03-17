namespace DbSqlLikeMem.Oracle.Test.Strategy;

/// <summary>
/// EN: Defines the class OracleMergeUpsertTests.
/// PT: Define a classe OracleMergeUpsertTests.
/// </summary>
public sealed class OracleMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Merge_ShouldInsert_WhenNotMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldInsert_WhenNotMatched.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "Strategy")]
    public void Merge_ShouldInsert_WhenNotMatched(int version)
    {
        var db = new OracleDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        using var cnn = new OracleConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users target
USING (SELECT 1 AS Id, 'A' AS Name FROM DUAL) src
ON (target.Id = src.Id)
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)";

        if (version < OracleDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute(sql));
            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Tests Merge_ShouldUpdate_WhenMatched behavior.
    /// PT: Testa o comportamento de Merge_ShouldUpdate_WhenMatched.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "Strategy")]
    public void Merge_ShouldUpdate_WhenMatched(int version)
    {
        var db = new OracleDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        using var cnn = new OracleConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users target
USING (SELECT 1 AS Id, 'NEW' AS Name FROM DUAL) src
ON (target.Id = src.Id)
WHEN MATCHED THEN
    UPDATE SET target.Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)";

        if (version < OracleDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute(sql));
            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Tests Merge_SourceAliasWithoutAs_ShouldResolveSourceColumns behavior.
    /// PT: Testa o comportamento de Merge_SourceAliasWithoutAs_ShouldResolveSourceColumns.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "Strategy")]
    public void Merge_SourceAliasWithoutAs_ShouldResolveSourceColumns(int version)
    {
        var db = new OracleDbMock(version);
        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        using var cnn = new OracleConnectionMock(db);
        cnn.Open();

        const string sql = @"
MERGE INTO users target
USING (SELECT 11 AS Id, 'OraNoAs' AS Name FROM DUAL) s
ON (target.Id = s.Id)
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (s.Id, s.Name)";

        if (version < OracleDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute(sql));
            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(11, (int)t[0][0]!);
        Assert.Equal("OraNoAs", (string)t[0][1]!);
    }
}
