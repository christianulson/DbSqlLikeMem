namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// Validates hotspot SQL Server function behavior to ensure fallback and evaluation compatibility.
/// Valida comportamentos críticos de funções do SQL Server para garantir compatibilidade de fallback e avaliação.
/// </summary>
public sealed class SqlServerFunctionHotspotCoverageTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// Initializes the in-memory SQL Server mock schema and seeds test data for function-coverage scenarios.
    /// Inicializa o esquema simulado de SQL Server em memória e popula dados de teste para cenários de cobertura de funções.
    /// </summary>
    /// <param name="helper">
    /// Provides xUnit output integration for test diagnostics.
    /// Fornece integração de saída do xUnit para diagnóstico dos testes.
    /// </param>
    public SqlServerFunctionHotspotCoverageTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new SqlServerDbMock();
        var t = db.AddTable("fn_data");
        t.AddColumn("id", DbType.Int32, false);
        t.AddColumn("name", DbType.String, false);
        t.AddColumn("email", DbType.String, true);
        t.AddColumn("payload", DbType.String, true);
        t.AddColumn("created", DbType.DateTime, false);

        t.Add(new Dictionary<int, object?>
        {
            [0] = 1,
            [1] = "John",
            [2] = null,
            [3] = "{\"a\":{\"b\":42}}",
            [4] = new DateTime(2020, 1, 1)
        });

        _cnn = new SqlServerConnectionMock(db);
        _cnn.Open();
    }

    /// <summary>
    /// Verifies that <c>CAST</c> uses expected fallback conversion while <c>TRY_CAST</c> returns <see langword="null"/> on invalid conversion.
    /// Verifica se <c>CAST</c> usa a conversão de fallback esperada enquanto <c>TRY_CAST</c> retorna <see langword="null"/> em conversões inválidas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void Cast_And_TryCast_ShouldFollowExpectedFallbacks()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT CAST('abc' AS INT) AS cast_value, TRY_CAST('abc' AS INT) AS try_cast_value");

        Assert.Equal(0, (int)row.cast_value);
        Assert.Null((object?)row.try_cast_value);
    }

    /// <summary>
    /// Ensures <c>OPENJSON</c>, <c>CONCAT_WS</c>, and <c>DATEADD</c> are evaluated correctly in a single query projection.
    /// Garante que <c>OPENJSON</c>, <c>CONCAT_WS</c> e <c>DATEADD</c> sejam avaliadas corretamente em uma única projeção de consulta.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void OpenJson_ConcatWs_And_DateAdd_ShouldBeEvaluated()
    {
        var row = _cnn.QuerySingle<dynamic>(@"
SELECT
    OPENJSON(payload) AS json_text,
    CONCAT_WS('-', name, email, 'end') AS joined,
    DATEADD(DAY, 2, created) AS plus_two_days
FROM fn_data
WHERE id = 1");

        Assert.Equal("{\"a\":{\"b\":42}}", (string)row.json_text);
        Assert.Equal("John-end", (string)row.joined);
        Assert.Equal(new DateTime(2020, 1, 3), (DateTime)row.plus_two_days);
    }



    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void JsonUnquote_And_ToNumber_ShouldConvertValues()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT JSON_UNQUOTE('\"alpha\"') AS uq, TO_NUMBER('42.50') AS num");

        Assert.Equal("alpha", (string)row.uq);
        Assert.Equal(42.50m, (decimal)row.num);
    }

    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void DateAdd_WithUnsupportedUnit_ShouldReturnOriginalDate()
    {
        var row = _cnn.QuerySingle<dynamic>(@"
SELECT DATEADD(FOO, 7, created) AS same_date
FROM fn_data
WHERE id = 1");

        Assert.Equal(new DateTime(2020, 1, 1), (DateTime)row.same_date);
    }

    /// <summary>
    /// Releases the test connection resources and then delegates disposal to the base test fixture.
    /// Libera os recursos de conexão de teste e depois delega o descarte para o fixture base de teste.
    /// </summary>
    /// <param name="disposing">
    /// Indicates whether managed resources should be disposed.
    /// Indica se os recursos gerenciados devem ser descartados.
    /// </param>
    protected override void Dispose(bool disposing)
    {
        _cnn?.Dispose();
        base.Dispose(disposing);
    }
}
