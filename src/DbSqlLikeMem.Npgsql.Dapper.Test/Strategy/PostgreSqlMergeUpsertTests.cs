namespace DbSqlLikeMem.Npgsql.Test.Strategy;

/// <summary>
/// EN: Covers PostgreSQL MERGE upsert behavior in dialect versions that support it.
/// PT: Cobre o comportamento de upsert com merge no PostgreSQL em versões de dialeto com suporte.
/// </summary>
public sealed class PostgreSqlMergeUpsertTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures MERGE parsing follows PostgreSQL dialect version support.
    /// PT: Garante que o parse de merge siga o suporte por versão do dialeto PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Strategy")]
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

    /// <summary>
    /// EN: Ensures MERGE updates an existing row when the ON condition matches.
    /// PT: Garante que merge atualize uma linha existente quando a condição ON é satisfeita.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataNpgsqlVersion]
    public void Merge_ShouldUpdate_WhenMatched(int version)
    {
        var db = new NpgsqlDbMock(version);
        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        if (version < NpgsqlDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
                MERGE INTO users AS target
                USING (SELECT 1 AS Id, 'NEW' AS Name) AS src
                ON target.Id = src.Id
                WHEN MATCHED THEN
                    UPDATE SET Name = src.Name
                WHEN NOT MATCHED THEN
                    INSERT (Id, Name) VALUES (src.Id, src.Name);
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

    /// <summary>
    /// EN: Ensures MERGE resolves source alias without AS for insert path.
    /// PT: Garante que o merge resolva alias da fonte sem AS no caminho de inserção.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataNpgsqlVersion]
    public void Merge_SourceAliasWithoutAs_ShouldInsert_WhenNotMatched(int version)
    {
        var db = new NpgsqlDbMock(version);
        using var cnn = new NpgsqlConnectionMock(db);
        cnn.Open();

        if (version < NpgsqlDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => cnn.Execute("""
                MERGE INTO users target
                USING (SELECT 8 AS Id, 'PgNoAs' AS Name) s
                ON target.Id = s.Id
                WHEN NOT MATCHED THEN
                    INSERT (Id, Name) VALUES (s.Id, s.Name);
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
USING (SELECT 8 AS Id, 'PgNoAs' AS Name) s
ON target.Id = s.Id
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (s.Id, s.Name);";

        var affected = cnn.Execute(sql);

        Assert.Equal(1, affected);
        Assert.Single(t);
        Assert.Equal(8, (int)t[0][0]!);
        Assert.Equal("PgNoAs", (string)t[0][1]!);
    }
}
