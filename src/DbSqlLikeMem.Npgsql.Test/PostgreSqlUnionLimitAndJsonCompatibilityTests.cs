namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// Tests that lock-in expected behavior for MySQL features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class PostgreSqlUnionLimitAndJsonCompatibilityTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _cnn;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public PostgreSqlUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new NpgsqlDbMock();
        var t = db.AddTable("t");
        t.Columns["id"] = new(0, DbType.Int32, false);
        t.Columns["payload"] = new(1, DbType.String, true);
        t.Add(new Dictionary<int, object?> { [0] = 1, [1] = "{\"a\":{\"b\":123}}" });
        t.Add(new Dictionary<int, object?> { [0] = 2, [1] = "{\"a\":{\"b\":456}}" });
        t.Add(new Dictionary<int, object?> { [0] = 3, [1] = null });

        _cnn = new NpgsqlConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// EN: Tests UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates behavior.
    /// PT: Testa o comportamento de UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates.
    /// </summary>
    [Fact]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
    {
        // UNION ALL keeps duplicates
        var all = _cnn.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION ALL
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1, 1], [.. all.Select(r => (int)r.id)]);

        // UNION removes duplicates
        var distinct = _cnn.Query<dynamic>(@"
SELECT id FROM t WHERE id = 1
UNION
SELECT id FROM t WHERE id = 1
").ToList();
        Assert.Equal([1], [.. distinct.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests LimitOffset_ShouldWork behavior.
    /// PT: Testa o comportamento de LimitOffset_ShouldWork.
    /// </summary>
    [Fact]
    public void LimitOffset_ShouldWork()
    {
        // MySQL supports: LIMIT offset, count
        var rows = _cnn.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests JsonPathExtract_ShouldWork behavior.
    /// PT: Testa o comportamento de JsonPathExtract_ShouldWork.
    /// </summary>
    [Fact]
    public void JsonPathExtract_ShouldWork()
    {
        var rows = _cnn.Query<dynamic>("SELECT id, (payload::jsonb #>> '{a,b}')::numeric AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }



    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT 1.0 AS v
UNION
SELECT 1 AS v
").ToList();

        Assert.Single(rows);
    }

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    public void Union_ShouldValidateIncompatibleColumnTypes()
    {
        Assert.Throws<InvalidOperationException>(() =>
            _cnn.Query<dynamic>(@"
SELECT 1 AS v
UNION
SELECT 'x' AS v
").ToList());
    }



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
    {
        var rows = _cnn.Query<dynamic>(@"
SELECT id AS v FROM t WHERE id IN (1, 2)
UNION ALL
SELECT id AS x FROM t WHERE id = 3
ORDER BY v
").ToList();

        Assert.Equal([1, 2, 3], [.. rows.Select(r => (int)r.v)]);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
