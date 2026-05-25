namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird UNION, pagination, and JSON compatibility scenarios supported by the in-memory mock.
/// PT-br: Cobre cenarios de compatibilidade de UNION, paginacao e JSON do Firebird suportados pelo mock em memoria.
/// </summary>
/// <remarks>
/// EN: Creates the in-memory Firebird connection used by the UNION, pagination, and JSON compatibility tests.
/// PT-br: Cria a conexao Firebird em memoria usada pelos testes de compatibilidade de UNION, paginacao e JSON.
/// </remarks>
public sealed class FirebirdUnionLimitAndJsonCompatibilityTests(
    ITestOutputHelper helper
    ) : DapperUnionLimitAndJsonCompatibilityTestsBase<FirebirdDbMock, FirebirdConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override FirebirdDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateConnection(FirebirdDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates and UNION removes them.
    /// PT-br: Verifica se UNION ALL mantem duplicatas e se UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies OFFSET and FETCH pagination works in Firebird coverage.
    /// PT-br: Verifica se a paginacao com OFFSET e FETCH funciona na cobertura do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void LimitOffset_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => Convert.ToInt32(GetValueIgnoreCase((object)r, "id")) )]);
    }

    /// <summary>
    /// EN: Verifies unsupported JSON functions still throw for this dialect.
    /// PT-br: Verifica se funcoes JSON sem suporte ainda lancam erro para este dialeto.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void JsonFunction_ShouldThrow_WhenNotSupportedByDialect()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT JSON_VALUE(payload, '$.a.b') AS v FROM t").ToList());
        Assert.Contains("JSON_VALUE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric types into a single row.
    /// PT-br: Garante que UNION normalize tipos numericos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across branches.
    /// PT-br: Garante que UNION rejeite tipos de coluna incompativeis entre seus ramos.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();

    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT-br: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
