namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Compatibility tests for MySQL JSON functions used by the Krnl-AI stores
/// (JSON_UNQUOTE/JSON_EXTRACT with the BINARY cast prefix).
/// PT-br: Testes de compatibilidade das funções JSON do MySQL usadas pelos stores do Krnl-AI
/// (JSON_UNQUOTE/JSON_EXTRACT com o prefixo de cast BINARY).
/// </summary>
public sealed class JsonFunctionsCompatibilityTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>Parses and evaluates BINARY JSON_UNQUOTE(JSON_EXTRACT(...)) = BINARY value.</summary>
    [Fact]
    public void SelectWithBinaryJsonUnquote_ShouldParseAndFilter()
    {
        using var cnn = new MySqlConnectionMock();
        cnn.Define("moments");
        cnn.Column<string>("moments", "moment_id");
        cnn.Column<string>("moments", "metadata_json");
        cnn.Seed("moments", null,
            ["m1", """{"tenant_id":"t1"}"""],
            ["m2", """{"tenant_id":"t2"}"""]);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT moment_id FROM moments WHERE BINARY JSON_UNQUOTE(JSON_EXTRACT(metadata_json, '$.tenant_id')) = BINARY 't1'"
        };

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("m1", reader.GetString(0));
        Assert.False(reader.Read());
    }
}