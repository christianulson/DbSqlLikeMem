namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// Tests that lock-in expected behavior for MySQL features that the in-memory mock already supports.
/// Keep these green: they protect you from regressions while you implement more advanced gaps elsewhere.
/// </summary>
public sealed class OracleUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<OracleDbMock, OracleConnectionMock>
{
    /// <summary>
    /// EN: Initializes a new instance of OracleUnionLimitAndJsonCompatibilityTests.
    /// PT: Inicializa uma nova instância de OracleUnionLimitAndJsonCompatibilityTests.
    /// </summary>
    public OracleUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <summary>
    /// EN: Tests UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates behavior.
    /// PT: Testa o comportamento de UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Tests OffsetFetch_ShouldWork behavior.
    /// PT: Testa o comportamento de OffsetFetch_ShouldWork.
    /// </summary>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void OffsetFetch_ShouldWork(int version)
    {
        using var connection = CreateOpenConnection(version);

        if (version < OracleDialect.OffsetFetchMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                connection.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList());
            Assert.Contains("OFFSET", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var rows = connection.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Tests JsonValue_SimpleObjectPath_ShouldWork behavior.
    /// PT: Testa o comportamento de JsonValue_SimpleObjectPath_ShouldWork.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void JsonValue_SimpleObjectPath_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id, JSON_VALUE(payload, '$.a.b' RETURNING NUMBER) AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }

    /// <summary>
    /// EN: Ensures JSON_VALUE RETURNING text types are applied by the executor instead of leaving numeric payloads untouched.
    /// PT: Garante que JSON_VALUE RETURNING em tipos textuais seja aplicado pelo executor em vez de deixar payloads numéricos inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void JsonValue_WithReturningVarchar2_ShouldCoerceValueToText()
    {
        var rows = Connection.Query<dynamic>("SELECT id, JSON_VALUE(payload, '$.a.b' RETURNING VARCHAR2(30)) AS v FROM t ORDER BY id").ToList();

        Assert.Equal(["123", "456", null], [.. rows.Select(r => (object?)r.v)]);
    }


    /// <summary>
    /// EN: Tests OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering behavior.
    /// PT: Testa o comportamento de OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
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
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void JsonFunction_ShouldThrow_WhenNotSupportedByDialect()
    {
        Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT JSON_EXTRACT(payload, '$.a.b') AS v FROM t").ToList());
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
