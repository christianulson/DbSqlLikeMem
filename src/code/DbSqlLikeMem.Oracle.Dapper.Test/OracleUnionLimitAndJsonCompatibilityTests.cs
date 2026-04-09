namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers Oracle UNION, offset/fetch, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT: Cobre cenarios de compatibilidade de UNION, offset/fetch e JSON do Oracle que o mock em memoria ja suporta.
/// </summary>
/// <remarks>
/// EN: Creates the in-memory Oracle connection used by the UNION, offset/fetch, and JSON compatibility tests.
/// PT: Cria a conexao Oracle em memoria usada pelos testes de compatibilidade de UNION, offset/fetch e JSON.
/// </remarks>
public sealed class OracleUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : DapperUnionLimitAndJsonCompatibilityTestsBase<OracleDbMock, OracleConnectionMock>(helper)
{

    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates while UNION removes them.
    /// PT: Verifica se UNION ALL mantem duplicatas enquanto UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies OFFSET/FETCH pagination returns the expected rows.
    /// PT: Verifica se a paginacao OFFSET/FETCH retorna as linhas esperadas.
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
            Assert.Contains(SqlConst.OFFSET, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var rows = connection.Query<dynamic>("SELECT id FROM t ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies JSON_VALUE returns the expected scalar values.
    /// PT: Verifica se JSON_VALUE retorna os valores escalares esperados.
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
    /// EN: Verifies ORDER BY NULLS FIRST applies explicit null ordering.
    /// PT: Verifica se ORDER BY NULLS FIRST aplica a ordenacao explicita de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST, id").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies unsupported JSON functions throw the expected exception.
    /// PT: Verifica se funcoes JSON sem suporte lancam a excecao esperada.
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
