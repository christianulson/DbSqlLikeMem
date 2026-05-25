namespace DbSqlLikeMem.Oracle.Test.Strategy;

/// <summary>
/// EN: Covers Oracle MERGE-based upsert scenarios against the Dapper provider.
/// PT-br: Cobre cenarios de upsert baseado em MERGE do Oracle contra o provedor Dapper.
/// </summary>
public sealed class OracleMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies MERGE inserts rows when the source row is not matched.
    /// PT-br: Verifica se MERGE insere linhas quando a linha de origem nao e correspondida.
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
            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("A", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Verifies MERGE updates rows when the source row is matched.
    /// PT-br: Verifica se MERGE atualiza linhas quando a linha de origem e correspondida.
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
            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Verifies MERGE resolves source columns when the alias omits AS.
    /// PT-br: Verifica se MERGE resolve as colunas de origem quando o alias omite AS.
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
            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(11, (int)t[0][0]!);
        Assert.Equal("OraNoAs", (string)t[0][1]!);
    }
}
