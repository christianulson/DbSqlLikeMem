namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Covers hotspot SQL Server function scenarios that exercise fallback and evaluation compatibility.
/// PT-br: Cobre cenarios de funcoes SQL Server que exercitam compatibilidade de fallback e avaliacao.
/// </summary>
public sealed class SqlServerFunctionHotspotCoverageTests : XUnitTestBase
{
    private readonly SqlServerConnectionMock _cnn;

    /// <summary>
    /// EN: Creates the in-memory SQL Server schema and seeds test data for function-coverage scenarios.
    /// PT-br: Cria o esquema SQL Server em memoria e popula dados de teste para cenarios de cobertura de funcoes.
    /// </summary>
    /// <param name="helper">EN: xUnit output helper. PT-br: Helper de saida do xUnit.</param>
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
    /// EN: Verifies that invalid <c>CAST</c> throws while <c>TRY_CAST</c> returns <see langword="null"/> on invalid conversion.
    /// PT-br: Verifica que <c>CAST</c> inválido lança exceção enquanto <c>TRY_CAST</c> retorna <see langword="null"/> em conversões inválidas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void Cast_Throws_And_TryCast_ReturnsNull_OnInvalidConversion()
    {
        Assert.Throws<InvalidCastException>(() => _cnn.QuerySingle<dynamic>("SELECT CAST('abc' AS INT) AS cast_value"));

        var row = _cnn.QuerySingle<dynamic>("SELECT TRY_CAST('abc' AS INT) AS try_cast_value");

        Assert.Null((object?)row.try_cast_value);
    }

    /// <summary>
    /// EN: Ensures <c>OPENJSON</c> with explicit schema, <c>CONCAT_WS</c>, and <c>DATEADD</c> are evaluated correctly in a single query projection.
    /// PT-br: Garante que <c>OPENJSON</c> com schema explicito, <c>CONCAT_WS</c> e <c>DATEADD</c> sejam avaliadas corretamente em uma única projeção de consulta.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void OpenJson_ConcatWs_And_DateAdd_ShouldBeEvaluated()
    {
        var row = _cnn.QuerySingle<dynamic>(@"
SELECT
    j.b AS json_text,
    CONCAT_WS('-', name, email, 'end') AS joined,
    DATEADD(DAY, 2, created) AS plus_two_days
FROM fn_data
CROSS APPLY OPENJSON(payload) WITH (
    b INT '$.a.b'
) j
WHERE id = 1");

        Assert.Equal(42, (int)row.json_text);
        Assert.Equal("John-end", (string)row.joined);
        Assert.Equal(new DateTime(2020, 1, 3), (DateTime)row.plus_two_days);
    }



    /// <summary>
    /// EN: Verifies JSON_VALUE and CAST convert string inputs to normalized scalar values.
    /// PT-br: Verifica que JSON_VALUE e CAST convertem entradas de texto em valores escalares normalizados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void JsonValue_And_Cast_ShouldConvertValues()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT JSON_VALUE('{\"name\":\"alpha\"}', '$.name') AS uq, CAST('42.50' AS DECIMAL(10,2)) AS num");

        Assert.Equal("alpha", (string)row.uq);
        Assert.Equal(42.50m, (decimal)row.num);
    }

    /// <summary>
    /// EN: Verifies DATEADD returns the original date when an unsupported unit token is provided.
    /// PT-br: Verifica que DATEADD retorna a data original quando um token de unidade não suportado é informado.
    /// </summary>
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
    /// EN: Verifies temporal functions work consistently in SELECT and WHERE clauses.
    /// PT-br: Verifica se funcoes temporais funcionam de forma consistente em clausulas SELECT e WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerFunctionCoverage")]
    public void TemporalFunctions_ShouldWorkInSelectAndWhere()
    {
        var row = _cnn.QuerySingle<dynamic>("SELECT GETDATE() AS d1, SYSDATETIME() AS d2, CURRENT_TIMESTAMP AS d3 FROM fn_data WHERE GETDATE() IS NOT NULL AND id = 1");

        Assert.IsType<DateTime>((object)row.d1);
        Assert.IsType<DateTime>((object)row.d2);
        Assert.IsType<DateTime>((object)row.d3);
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

