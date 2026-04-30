namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Covers DB2 UNION, LIMIT, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT: Cobre cenarios de compatibilidade de UNION, LIMIT e JSON do DB2 que o mock em memoria ja suporta.
/// </summary>
/// <remarks>
/// EN: Creates the in-memory DB2 connection used by the UNION, LIMIT, and JSON compatibility tests.
/// PT: Cria a conexao DB2 em memoria usada pelos testes de compatibilidade de UNION, LIMIT e JSON.
/// </remarks>
public sealed class Db2UnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : DapperUnionLimitAndJsonCompatibilityTestsBase<Db2DbMock, Db2ConnectionMock>(helper)
{

    /// <inheritdoc />
    protected override Db2ConnectionMock CreateConnection(Db2DbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates while UNION removes them.
    /// PT: Verifica se UNION ALL mantem duplicatas enquanto UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies LIMIT supports the comma offset syntax.
    /// PT: Verifica se LIMIT suporta a sintaxe de offset com virgula.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void Limit_OffsetCommaSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 1, 2").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies LIMIT supports the OFFSET keyword syntax.
    /// PT: Verifica se LIMIT suporta a sintaxe com a palavra-chave OFFSET.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void Limit_OffsetKeywordSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies JSON_EXTRACT throws when the dialect does not support it.
    /// PT: Verifica se JSON_EXTRACT lança erro quando o dialeto nao suporta.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void JsonExtract_SimpleObjectPath_ShouldThrow_WhenNotSupportedByDialect()
    {
        Assert.Throws<NotSupportedException>(() =>
            Connection.Query<dynamic>("SELECT id, JSON_EXTRACT(payload, '$.a.b') AS v FROM t ORDER BY id").ToList());
    }




    /// <summary>
    /// EN: Verifies ORDER BY NULLS FIRST throws when the dialect does not support that modifier.
    /// PT: Verifica se ORDER BY NULLS FIRST gera erro quando o dialeto nao suporta esse modificador.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
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
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
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
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2UnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
