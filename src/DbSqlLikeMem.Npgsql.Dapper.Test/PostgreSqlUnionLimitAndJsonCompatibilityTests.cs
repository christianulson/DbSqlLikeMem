namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers PostgreSQL UNION, LIMIT, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT: Cobre cenarios de compatibilidade de UNION, LIMIT e JSON do PostgreSQL que o mock em memoria ja suporta.
/// </summary>
public sealed class PostgreSqlUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<NpgsqlDbMock, NpgsqlConnectionMock>
{
    private const int PostgreSqlJsonbMinVersion = 9;

    /// <summary>
    /// EN: Creates the in-memory PostgreSQL connection used by the UNION, LIMIT, and JSON compatibility tests.
    /// PT: Cria a conexao PostgreSQL em memoria usada pelos testes de compatibilidade de UNION, LIMIT e JSON.
    /// </summary>
    public PostgreSqlUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override NpgsqlDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates and UNION removes them.
    /// PT: Verifica se UNION ALL mantem duplicatas e se UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies OFFSET and LIMIT pagination works in PostgreSQL coverage.
    /// PT: Verifica se a paginacao com OFFSET e LIMIT funciona na cobertura do PostgreSQL.
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
    /// EN: Verifies JSON path extraction respects the PostgreSQL version gate.
    /// PT: Verifica se a extracao por caminho JSON respeita a restricao de versao do PostgreSQL.
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
    /// EN: Verifies explicit NULLS FIRST ordering is applied in PostgreSQL coverage.
    /// PT: Verifica se a ordenacao explicita NULLS FIRST e aplicada na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST, id").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies unsupported JSON functions still throw for this dialect.
    /// PT: Verifica se funcoes JSON sem suporte ainda lancam erro para este dialeto.
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
