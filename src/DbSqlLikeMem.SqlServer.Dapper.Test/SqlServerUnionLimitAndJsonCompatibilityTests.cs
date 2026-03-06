namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// Tests that lock-in expected behavior for MySQL features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class SqlServerUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<SqlServerDbMock, SqlServerConnectionMock>
{
    private const int SqlServerOffsetFetchMinVersion = 2012;
    private const int SqlServerJsonFunctionsMinVersion = 2016;

    /// <summary>
    /// EN: Tests SqlServerUnionLimitAndJsonCompatibilityTests behavior.
    /// PT: Testa o comportamento de SqlServerUnionLimitAndJsonCompatibilityTests.
    /// </summary>
    public SqlServerUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override SqlServerDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);

    /// <summary>
    /// EN: Tests UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates behavior.
    /// PT: Testa o comportamento de UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Tests OffsetFetch_ShouldWork behavior.
    /// PT: Testa o comportamento de OffsetFetch_ShouldWork.
    /// </summary>
    [Theory]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    [MemberDataSqlServerVersion]
    public void OffsetFetch_ShouldRespectVersion(int version)
    {
        using var cnn = CreateOpenConnection(version);

        if (version < SqlServerOffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList());
            return;
        }

        // SQL Server: OFFSET/FETCH
        var rows = cnn.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests JsonValue_SimpleObjectPath_ShouldWork behavior.
    /// PT: Testa o comportamento de JsonValue_SimpleObjectPath_ShouldWork.
    /// </summary>
    [Theory]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    [MemberDataSqlServerVersion]
    public void JsonValue_SimpleObjectPath_ShouldRespectVersion(int version)
    {
        using var cnn = CreateOpenConnection(version);

        if (version < SqlServerJsonFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>("SELECT id, TRY_CAST(JSON_VALUE(payload, '$.a.b') AS DECIMAL(18,0)) AS v FROM t ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>("SELECT id, TRY_CAST(JSON_VALUE(payload, '$.a.b') AS DECIMAL(18,0)) AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }




    /// <summary>
    /// EN: Tests OrderBy_NullsFirst_ShouldThrow_WhenDialectDoesNotSupportModifier behavior.
    /// PT: Testa o comportamento de OrderBy_NullsFirst_ShouldThrow_WhenDialectDoesNotSupportModifier.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldThrow_WhenDialectDoesNotSupportModifier()
    {
        Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST").ToList());
    }


    /// <summary>
    /// EN: Tests JsonFunction_ShouldThrow_WhenNotSupportedByDialect behavior.
    /// PT: Testa o comportamento de JsonFunction_ShouldThrow_WhenNotSupportedByDialect.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void JsonFunction_ShouldThrow_WhenNotSupportedByDialect()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT JSON_EXTRACT(payload, '$.a.b') AS v FROM t").ToList());
        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("JSON_EXTRACT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
