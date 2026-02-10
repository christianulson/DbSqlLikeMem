using System.Text.Json;

namespace DbSqlLikeMem.Oracle.Test;

public sealed class SqlValueHelperTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests Resolve_ShouldReadDapperParameter_ByName behavior.
    /// PT: Testa o comportamento de Resolve_ShouldReadDapperParameter_ByName.
    /// </summary>
    [Fact]
    public void Resolve_ShouldReadDapperParameter_ByName()
    {
        using var cnn = new OracleConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = OracleValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(123, v);
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldThrow_WhenParameterMissing behavior.
    /// PT: Testa o comportamento de Resolve_ShouldThrow_WhenParameterMissing.
    /// </summary>
    [Fact]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldParseInList_ToListOfResolvedValues behavior.
    /// PT: Testa o comportamento de Resolve_ShouldParseInList_ToListOfResolvedValues.
    /// </summary>
    [Fact]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = OracleValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = Assert.IsType<List<object?>>(v);
        Assert.Equal([1, 2, 3], [.. list.Cast<int>()]);
    }

    /// <summary>
    /// EN: Tests Resolve_NullOnNonNullable_ShouldThrow behavior.
    /// PT: Testa o comportamento de Resolve_NullOnNonNullable_ShouldThrow.
    /// </summary>
    [Fact]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_Json_ShouldReturnJsonDocument_WhenValid behavior.
    /// PT: Testa o comportamento de Resolve_Json_ShouldReturnJsonDocument_WhenValid.
    /// </summary>
    [Fact]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = OracleValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Tests Like_ShouldMatch_MySqlStyle behavior.
    /// PT: Testa o comportamento de Like_ShouldMatch_MySqlStyle.
    /// </summary>
    [Theory]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_MySqlStyle(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, OracleValueHelper.Like(value, pattern));
    }

    /// <summary>
    /// EN: Tests Resolve_Enum_ShouldValidateAgainstColumnDef behavior.
    /// PT: Testa o comportamento de Resolve_Enum_ShouldValidateAgainstColumnDef.
    /// </summary>
    [Fact]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var cols = new ColumnDictionary
        {
            ["Status"] = new ColumnDef(0, DbType.String, false)
        };
        cols["Status"].EnumValues.UnionWith(["active", "inactive"]);

        OracleValueHelper.CurrentColumn = "Status";

        var ok = OracleValueHelper.Resolve("'Active'", DbType.String, false, null, cols);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("'blocked'", DbType.String, false, null, cols));
        Assert.Equal(1265, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests Resolve_Set_ShouldReturnHashSet_AndValidate behavior.
    /// PT: Testa o comportamento de Resolve_Set_ShouldReturnHashSet_AndValidate.
    /// </summary>
    [Fact]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var cols = new ColumnDictionary
        {
            ["Tags"] = new ColumnDef(0, DbType.Int32, false)
            {
                EnumValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "a", "b", "c" }
            }
        };

        var prev = OracleValueHelper.CurrentColumn;
        try
        {
            OracleValueHelper.CurrentColumn = "Tags";

            var ok = OracleValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: cols);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<OracleMockException>(() =>
                OracleValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: cols));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            OracleValueHelper.CurrentColumn = prev;
        }
    }
}
