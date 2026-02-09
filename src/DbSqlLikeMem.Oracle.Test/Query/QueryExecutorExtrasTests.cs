using System.Reflection;

namespace DbSqlLikeMem.Oracle.Test.Query;

public sealed class QueryExecutorExtrasTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private static OracleDbMock SeedDb()
    {
        var db = new OracleDbMock();
        var t = db.AddTable("tx");
        t.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        t.Columns["grp"] = new ColumnDef(1, DbType.String, false);
        t.Columns["amt"] = new ColumnDef(2, DbType.Decimal, false);
        // seed 1:A(10),1:B(20),2:A(30)
        t.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "A" }, { 2, 10m } });
        t.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "B" }, { 2, 20m } });
        t.Add(new Dictionary<int, object?> { { 0, 3 }, { 1, "A" }, { 2, 30m } });
        return db;
    }

    [Fact]
    public void GroupByAndAggregationsShouldComputeCorrectly()
    {
        // Arrange
        var db = SeedDb();
        using var c = new OracleConnectionMock(db);
        const string sql = @"
SELECT grp
    , COUNT(tx.id) AS C
    , SUM(amt) AS S
    , AVG(amt) AS A
    , MIN(amt) AS MI
    , MAX(amt) AS MA 
 FROM tx 
GROUP BY grp";
        using var cmd = new OracleCommandMock(c) { CommandText = sql };

        // Act
        using var reader = cmd.ExecuteReader();
        var rows = reader.Parse<dynamic>().ToList();

        // Assert
        Assert.Equal(2, rows.Count);
        var a = rows.Single(r => r.grp == "A");
        Assert.Equal(2L, a.C);      // COUNT
        Assert.Equal(40m, a.S);     // SUM
        Assert.Equal(20m, a.A);     // AVG
        Assert.Equal(10m, a.MI);     // MIN
        Assert.Equal(30m, a.MA);     // MAX
    }

    private static readonly int[] expected = [4, 3];

    [Fact]
    public void OrderByLimitOffsetShouldPageCorrectly()
    {
        // Arrange
        var db = new OracleDbMock();
        var table = db.AddTable("t");
        table.Columns["id"] = new ColumnDef(0, DbType.Int32, false);
        table.Columns["iddesc"] = new ColumnDef(1, DbType.Int32, false);
        for (int i = 1; i <= 5; i++)
            table.Add(new Dictionary<int, object?> { { 0, i }, { 1, 4 - i } });
        using var c = new OracleConnectionMock(db);
        const string sql = @"
SELECT t2.* FROM t t2 ORDER BY id DESC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;
SELECT * FROM t ORDER BY iddesc ASC OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY;";
        using var cmd = new OracleCommandMock(c) { CommandText = sql };

        // Act
        using var reader = cmd.ExecuteReader();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            Console.WriteLine($"{i}: {reader.GetName(i)}");
        }
        var ids = reader.Parse<dynamic>().Select(r => (int)r.id).ToList();

        // Assert
        Assert.Equal(expected, ids);
        reader.NextResult();

        for (int i = 0; i < reader.FieldCount; i++)
        {
            Console.WriteLine($"{i}: {reader.GetName(i)}");
        }
        var ids2 = reader.Parse<dynamic>().Select(r => (int)r.id).ToList();

        // Assert
        Assert.Equal(expected, ids2);
    }
}

public class SqlTranslatorTests
{
    [Fact]
    public void TranslateBasicWhereAndOrderBySqlCorrect()
    {
        // Arrange
        using var cnn = new OracleConnectionMock([]);
        var q = cnn.AsQueryable<Foo>()
                   .Where(f => f.X > 5 && f.Y == "abc")
                   .OrderBy(f => f.Y)
                   .Skip(2)
                   .Take(3);

        // Act
        var providerField = typeof(OracleQueryProvider)
            .GetField("_translator", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var provider = (OracleTranslator)providerField.GetValue(q.Provider)!;
        var sql = provider.Translate(q.Expression).Sql;

        // Assert
        Assert.StartsWith("SELECT * FROM Foo WHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET 2", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH NEXT 3", sql, StringComparison.OrdinalIgnoreCase);
    }

#pragma warning disable CA1812
    private sealed class Foo
    {
        public int X { get; set; }
        public string Y { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
