namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Covers MySQL UNION, LIMIT, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT: Cobre cenarios de compatibilidade de UNION, LIMIT e JSON do MySQL que o mock em memoria ja suporta.
/// </summary>
/// <remarks>
/// EN: Creates the in-memory MySQL connection used by the UNION, LIMIT, and JSON compatibility tests.
/// PT: Cria a conexao MySQL em memoria usada pelos testes de compatibilidade de UNION, LIMIT e JSON.
/// </remarks>
public sealed class MySqlUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : DapperUnionLimitAndJsonCompatibilityTestsBase<MySqlDbMock, MySqlConnectionMock>(helper)
{
    private const int MySqlJsonExtractMinVersion = 57;

    /// <inheritdoc />
    protected override MySqlDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db) => new(db);


    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates while UNION removes them.
    /// PT: Verifica se UNION ALL mantem duplicatas enquanto UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies LIMIT supports the comma offset syntax.
    /// PT: Verifica se LIMIT suporta a sintaxe de offset com virgula.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetCommaSyntax_ShouldWork()
    {
        // MySQL supports: LIMIT offset, count
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 1, 2").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies LIMIT supports the OFFSET keyword syntax.
    /// PT: Verifica se LIMIT suporta a sintaxe com a palavra-chave OFFSET.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetKeywordSyntax_ShouldWork()
    {
        // MySQL supports: LIMIT count OFFSET offset
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies JSON_EXTRACT respects the configured MySQL version.
    /// PT: Verifica se JSON_EXTRACT respeita a versao MySQL configurada.
    /// </summary>
    [Theory]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    [MemberDataMySqlVersion]
    public void JsonExtract_SimpleObjectPath_ShouldRespectVersion(int version)
    {
        using var cnn = CreateOpenConnection(version);

        if (version < MySqlJsonExtractMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                cnn.Query<dynamic>("SELECT id, JSON_EXTRACT(payload, '$.a.b') AS v FROM t ORDER BY id").ToList());
            return;
        }

        var rows = cnn.Query<dynamic>("SELECT id, JSON_EXTRACT(payload, '$.a.b') AS v FROM t ORDER BY id").ToList();

        // implemented as best-effort; null JSON -> null
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }




    /// <summary>
    /// EN: Verifies ORDER BY NULLS FIRST throws when the dialect does not support that modifier.
    /// PT: Verifica se ORDER BY NULLS FIRST gera erro quando o dialeto nao suporta esse modificador.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldThrow_WhenDialectDoesNotSupportModifier()
    {
        Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST").ToList());
    }


    /// <summary>
    /// EN: Verifies unsupported JSON functions throw the expected exception.
    /// PT: Verifica se funcoes JSON sem suporte lancam a excecao esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
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
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
