namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// Tests that lock-in expected behavior for MySQL features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class PostgreSqlUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<NpgsqlDbMock, NpgsqlConnectionMock>
{
    private const int PostgreSqlJsonbMinVersion = 9;

    /// <summary>
    /// EN: Tests PostgreSqlUnionLimitAndJsonCompatibilityTests behavior.
    /// PT: Testa o comportamento de PostgreSqlUnionLimitAndJsonCompatibilityTests.
    /// </summary>
    public PostgreSqlUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override NpgsqlDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db) => new(db);

    /// <summary>
    /// EN: Tests UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates behavior.
    /// PT: Testa o comportamento de UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Tests LimitOffset_ShouldWork behavior.
    /// PT: Testa o comportamento de LimitOffset_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void LimitOffset_ShouldWork()
    {
        // MySQL supports: LIMIT offset, count
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests JsonPathExtract_ShouldWork behavior.
    /// PT: Testa o comportamento de JsonPathExtract_ShouldWork.
    /// </summary>
    [Theory]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    [MemberDataNpgsqlVersion]
    public void JsonPathExtract_ShouldRespectVersion(int version)
    {
        using var cnn = CreateOpenConnection(version);

        if (version < PostgreSqlJsonbMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>("SELECT id, (payload::jsonb #>> '{a,b}')::numeric AS v FROM t ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>("SELECT id, (payload::jsonb #>> '{a,b}')::numeric AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }




    /// <summary>
    /// EN: Tests OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering behavior.
    /// PT: Testa o comportamento de OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
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
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void JsonFunction_ShouldThrow_WhenNotSupportedByDialect()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT JSON_VALUE(payload, '$.a.b') AS v FROM t").ToList());
        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("JSON_VALUE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
