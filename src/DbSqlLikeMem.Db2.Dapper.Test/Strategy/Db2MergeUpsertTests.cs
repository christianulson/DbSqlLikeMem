namespace DbSqlLikeMem.Db2.Dapper.Test.Strategy;

/// <summary>
/// EN: Covers DB2 MERGE upsert behavior in the command mock pipeline.
/// PT: Cobre o comportamento de upsert com merge no pipeline do comando simulado DB2.
/// </summary>
public sealed class Db2MergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures MERGE parsing follows DB2 dialect version support.
    /// PT: Garante que o parse de merge siga o suporte por versão do dialeto DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataDb2Version]
    public void Merge_ShouldFollowDialectVersionSupport(int version)
    {
        var db = new Db2DbMock(version);

        if (version < Db2Dialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("MERGE INTO users t USING (SELECT 1 AS Id) s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET t.Id = s.Id", db.Dialect));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = SqlQueryParser.Parse(
            "MERGE INTO users t USING (SELECT 1 AS Id) s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET t.Id = s.Id",
            db.Dialect);

        Assert.IsType<SqlMergeQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures MERGE updates an existing row when the ON condition matches.
    /// PT: Garante que merge atualize uma linha existente quando a condição ON é satisfeita.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataDb2Version]
    public void Merge_ShouldUpdate_WhenMatched(int version)
    {
        var db = new Db2DbMock(version);
        using var cnn = new Db2ConnectionMock(db);
        cnn.Open();

        if (version < Db2Dialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
                MERGE INTO users target
                USING (SELECT 1 AS Id, 'NEW' AS Name) src
                ON target.Id = src.Id
                WHEN MATCHED THEN
                    UPDATE SET target.Name = src.Name
                WHEN NOT MATCHED THEN
                    INSERT (Id, Name) VALUES (src.Id, src.Name)
                """));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        const string sql = @"
MERGE INTO users target
USING (SELECT 1 AS Id, 'NEW' AS Name) src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET target.Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Ensures MERGE resolves source alias without AS for insert path.
    /// PT: Garante que o merge resolva alias da fonte sem AS no caminho de inserção.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataDb2Version]
    public void Merge_SourceAliasWithoutAs_ShouldInsert_WhenNotMatched(int version)
    {
        var db = new Db2DbMock(version);

        using var cnn = new Db2ConnectionMock(db);
        cnn.Open();

        if (version < Db2Dialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
                MERGE INTO users target
                USING (SELECT 7 AS Id, 'Db2NoAs' AS Name) s
                ON target.Id = s.Id
                WHEN NOT MATCHED THEN
                    INSERT (Id, Name) VALUES (s.Id, s.Name)
                """));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        const string sql = @"
MERGE INTO users target
USING (SELECT 7 AS Id, 'Db2NoAs' AS Name) s
ON target.Id = s.Id
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (s.Id, s.Name)";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(7, (int)t[0][0]!);
        Assert.Equal("Db2NoAs", (string)t[0][1]!);
    }
}
