using System.Text.Json;

namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class SqlValueHelperTests.
/// PT: Define o(a) class SqlValueHelperTests.
/// </summary>
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
    /// EN: Describes the behavior validated by Like_ShouldMatch_MySqlStyle.
    /// PT: Descreve o comportamento validado por Like_ShouldMatch_MySqlStyle.
    /// </summary>
    /// <summary>
    /// EN: Describes the behavior validated by Like_ShouldMatch_MySqlStyle.
    /// PT: Descreve o comportamento validado por Like_ShouldMatch_MySqlStyle.
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
        var tb = new OracleDbMock().AddTable("tb");

        tb.AddColumn("Status", DbType.String, false)
        .EnumValues.UnionWith(["active", "inactive"]);

        OracleValueHelper.CurrentColumn = "Status";

        var ok = OracleValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns));
        Assert.Equal(1265, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests Resolve_Set_ShouldReturnHashSet_AndValidate behavior.
    /// PT: Testa o comportamento de Resolve_Set_ShouldReturnHashSet_AndValidate.
    /// </summary>
    [Fact]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new OracleDbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
                enumValues: ["a", "b", "c"]);

        var prev = OracleValueHelper.CurrentColumn;
        try
        {
            OracleValueHelper.CurrentColumn = "Tags";

            var ok = OracleValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<OracleMockException>(() =>
                OracleValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            OracleValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateStringSize behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateStringSize.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateStringSize()
    {
        var tb = new OracleDbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = OracleValueHelper.CurrentColumn;
        try
        {
            OracleValueHelper.CurrentColumn = "Name";
            var ex = Assert.Throws<OracleMockException>(() =>
                OracleValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns));
            Assert.Equal(12899, ex.ErrorCode);
        }
        finally
        {
            OracleValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateDecimalPlaces behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateDecimalPlaces.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateDecimalPlaces()
    {
        var tb = new OracleDbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = OracleValueHelper.CurrentColumn;
        try
        {
            OracleValueHelper.CurrentColumn = "Amount";
            var ex = Assert.Throws<OracleMockException>(() =>
                OracleValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns));
            Assert.Equal(1438, ex.ErrorCode);
        }
        finally
        {
            OracleValueHelper.CurrentColumn = prev;
        }
    }
}
