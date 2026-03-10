namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// Tests that lock-in expected behavior for SQLite features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class SqliteUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<SqliteDbMock, SqliteConnectionMock>
{
    /// <summary>
    /// EN: Tests SqliteUnionLimitAndJsonCompatibilityTests behavior.
    /// PT: Testa o comportamento de SqliteUnionLimitAndJsonCompatibilityTests.
    /// </summary>
    public SqliteUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);

    /// <summary>
    /// EN: Tests UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates behavior.
    /// PT: Testa o comportamento de UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Tests Limit_OffsetCommaSyntax_ShouldWork behavior.
    /// PT: Testa o comportamento de Limit_OffsetCommaSyntax_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetCommaSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 1, 2").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests Limit_OffsetKeywordSyntax_ShouldWork behavior.
    /// PT: Testa o comportamento de Limit_OffsetKeywordSyntax_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetKeywordSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests JsonExtract_SimpleObjectPath_ShouldWork behavior.
    /// PT: Testa o comportamento de JsonExtract_SimpleObjectPath_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void JsonExtract_SimpleObjectPath_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id, JSON_EXTRACT(payload, '$.a.b') AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }




    /// <summary>
    /// EN: Tests OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering behavior.
    /// PT: Testa o comportamento de OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST, id").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Tests JsonFunction_ShouldThrow_WhenNotSupportedByDialect behavior.
    /// PT: Testa o comportamento de JsonFunction_ShouldThrow_WhenNotSupportedByDialect.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void JsonFunction_ShouldThrow_WhenNotSupportedByDialect()
    {
        Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT JSON_VALUE(payload, '$.a.b') AS v FROM t").ToList());
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
