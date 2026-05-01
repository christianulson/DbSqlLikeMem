namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers SQLite UNION, LIMIT, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT-br: Cobre cenarios de compatibilidade de UNION, LIMIT e JSON do SQLite que o mock em memoria ja suporta.
/// </summary>
public sealed class SqliteUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<SqliteDbMock, SqliteConnectionMock>
{
    /// <summary>
    /// EN: Creates the in-memory SQLite connection used by the UNION, LIMIT, and JSON compatibility tests.
    /// PT-br: Cria a conexao SQLite em memoria usada pelos testes de compatibilidade de UNION, LIMIT e JSON.
    /// </summary>
    public SqliteUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates while UNION removes them.
    /// PT-br: Verifica se UNION ALL mantem duplicatas enquanto UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies LIMIT supports the comma offset syntax.
    /// PT-br: Verifica se LIMIT suporta a sintaxe de offset com virgula.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetCommaSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 1, 2").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies LIMIT supports the OFFSET keyword syntax.
    /// PT-br: Verifica se LIMIT suporta a sintaxe com a palavra-chave OFFSET.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Limit_OffsetKeywordSyntax_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1").ToList();
        Assert.Equal([2, 3], [.. rows.Select(r => (int)r.id)]);
    }

    /// <summary>
    /// EN: Verifies JSON_EXTRACT returns the expected values.
    /// PT-br: Verifica se JSON_EXTRACT retorna os valores esperados.
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
    /// EN: Verifies ORDER BY NULLS FIRST applies explicit null ordering.
    /// PT-br: Verifica se ORDER BY NULLS FIRST aplica a ordenacao explicita de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void OrderBy_NullsFirst_ShouldApplyExplicitNullOrdering()
    {
        var rows = Connection.Query<dynamic>("SELECT id FROM t ORDER BY payload NULLS FIRST, id").ToList();
        Assert.Equal([3, 1, 2], [.. rows.Select(r => (int)r.id)]);
    }


    /// <summary>
    /// EN: Verifies JSON_VALUE returns the expected values.
    /// PT-br: Verifica se JSON_VALUE retorna os valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void JsonValue_SimpleObjectPath_ShouldWork()
    {
        var rows = Connection.Query<dynamic>("SELECT id, JSON_VALUE(payload, '$.a.b') AS v FROM t ORDER BY id").ToList();
        Assert.Equal([123m, 456m, null], [.. rows.Select(r => (object?)r.v)]);
    }

    /// <summary>
    /// EN: Ensures UNION normalizes equivalent numeric literals into a single row.
    /// PT-br: Garante que o UNION normalize literais numéricos equivalentes em uma única linha.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeEquivalentNumericTypes()
        => AssertUnionNormalizesEquivalentNumericTypes();

    /// <summary>
    /// EN: Ensures UNION rejects incompatible column types across SELECT parts.
    /// PT-br: Garante que o UNION rejeite tipos de coluna incompatíveis entre partes do SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldValidateIncompatibleColumnTypes()
        => AssertUnionValidatesIncompatibleColumnTypes();



    /// <summary>
    /// EN: Ensures UNION schema keeps aliases from the first SELECT projection.
    /// PT-br: Garante que o schema do UNION mantenha os aliases da primeira projeção SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteUnionLimitAndJsonCompatibility")]
    public void Union_ShouldNormalizeSchemaToFirstSelectAlias()
        => AssertUnionNormalizesSchemaToFirstSelectAlias();
}
