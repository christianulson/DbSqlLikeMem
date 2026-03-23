namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers SQL Server UNION, OFFSET/FETCH, and JSON compatibility scenarios already supported by the in-memory mock.
/// PT: Cobre cenarios de compatibilidade de UNION, OFFSET/FETCH e JSON do SQL Server que o mock em memoria ja suporta.
/// </summary>
public sealed class SqlServerUnionLimitAndJsonCompatibilityTests : DapperUnionLimitAndJsonCompatibilityTestsBase<SqlServerDbMock, SqlServerConnectionMock>
{
    private const int SqlServerOffsetFetchMinVersion = 2012;
    private const int SqlServerJsonFunctionsMinVersion = 2016;

    /// <summary>
    /// EN: Creates the in-memory SQL Server connection used by the UNION, OFFSET/FETCH, and JSON compatibility tests.
    /// PT: Cria a conexao SQL Server em memoria usada pelos testes de compatibilidade de UNION, OFFSET/FETCH e JSON.
    /// </summary>
    public SqlServerUnionLimitAndJsonCompatibilityTests(ITestOutputHelper helper) : base(helper) { }

    /// <inheritdoc />
    protected override SqlServerDbMock CreateDb(int? version) => new(version);

    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies UNION ALL keeps duplicates while UNION removes them.
    /// PT: Verifica se UNION ALL mantem duplicatas enquanto UNION as remove.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
    public void UnionAll_ShouldKeepDuplicates_UnionShouldRemoveDuplicates()
        => AssertUnionAllKeepsDuplicatesAndUnionRemovesThem();

    /// <summary>
    /// EN: Verifies OFFSET/FETCH respects the configured SQL Server version.
    /// PT: Verifica se OFFSET/FETCH respeita a versao SQL Server configurada.
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
    /// EN: Verifies JSON_VALUE respects the configured SQL Server version.
    /// PT: Verifica se JSON_VALUE respeita a versao SQL Server configurada.
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
    /// EN: Verifies ORDER BY NULLS FIRST throws when the dialect does not support that modifier.
    /// PT: Verifica se ORDER BY NULLS FIRST gera erro quando o dialeto nao suporta esse modificador.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerUnionLimitAndJsonCompatibility")]
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
