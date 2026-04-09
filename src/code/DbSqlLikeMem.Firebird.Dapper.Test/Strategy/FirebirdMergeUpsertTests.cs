namespace DbSqlLikeMem.Firebird.Dapper.Test.Strategy;

/// <summary>
/// EN: Covers Firebird MERGE upsert behavior in the command mock pipeline.
/// PT: Cobre o comportamento de upsert com MERGE no pipeline do comando simulado Firebird.
/// </summary>
public sealed class FirebirdMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures MERGE parsing follows Firebird dialect version support.
    /// PT: Garante que o parse de MERGE siga o suporte por versao do dialeto Firebird.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberData(nameof(FirebirdVersions))]
    public void Merge_ShouldFollowDialectVersionSupport(int version)
    {
        var db = new FirebirdDbMock(version);

        const string sql = """
MERGE INTO users target
USING (SELECT 1 AS Id FROM RDB$DATABASE) src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET target.Id = src.Id
""";

        if (version < FirebirdDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, db.Dialect));
            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, db, db.Dialect);
        Assert.IsType<SqlMergeQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures MERGE updates an existing row when the ON condition matches.
    /// PT: Garante que MERGE atualize uma linha existente quando a condicao ON e satisfeita.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberData(nameof(FirebirdVersions))]
    public void Merge_ShouldUpdate_WhenMatched(int version)
    {
        var db = new FirebirdDbMock(version);
        using var cnn = new FirebirdConnectionMock(db);
        cnn.Open();

        if (version < FirebirdDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
MERGE INTO users target
USING (SELECT 1 AS Id, 'NEW' AS Name FROM RDB$DATABASE) src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)
"""));

            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "OLD" });

        const string sql = """
MERGE INTO users target
USING (SELECT 1 AS Id, 'NEW' AS Name FROM RDB$DATABASE) src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)
""";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal("NEW", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Ensures MERGE inserts a row when the ON condition does not match.
    /// PT: Garante que MERGE insira uma linha quando a condicao ON nao e satisfeita.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberData(nameof(FirebirdVersions))]
    public void Merge_ShouldInsert_WhenNotMatched(int version)
    {
        var db = new FirebirdDbMock(version);
        using var cnn = new FirebirdConnectionMock(db);
        cnn.Open();

        if (version < FirebirdDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
MERGE INTO users target
USING (SELECT 7 AS Id, 'FBNoMatch' AS Name FROM RDB$DATABASE) src
ON target.Id = src.Id
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)
"""));

            Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var t = db.AddTable("users");
        t.AddColumn("Id", DbType.Int32, false);
        t.AddColumn("Name", DbType.String, false);
        t.AddPrimaryKeyIndexes("id");

        const string sql = """
MERGE INTO users target
USING (SELECT 7 AS Id, 'FBNoMatch' AS Name FROM RDB$DATABASE) src
ON target.Id = src.Id
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name)
""";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(7, (int)t[0][0]!);
        Assert.Equal("FBNoMatch", (string)t[0][1]!);
    }

    /// <summary>
    /// EN: Returns the Firebird versions supported by the MERGE scenarios.
    /// PT: Retorna as versoes do Firebird suportadas pelos cenarios de MERGE.
    /// </summary>
    public static IEnumerable<object[]> FirebirdVersions()
        => FirebirdDbVersions.Versions().Select(version => new object[] { version });
}
