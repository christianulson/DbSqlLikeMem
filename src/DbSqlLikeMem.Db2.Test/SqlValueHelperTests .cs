using System.Text.Json;

namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// Auto-generated summary.
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
        using var cnn = new Db2ConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = Db2ValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(123, v);
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldThrow_WhenParameterMissing behavior.
    /// PT: Testa o comportamento de Resolve_ShouldThrow_WhenParameterMissing.
    /// </summary>
    [Fact]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<Db2MockException>(() =>
            Db2ValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldParseInList_ToListOfResolvedValues behavior.
    /// PT: Testa o comportamento de Resolve_ShouldParseInList_ToListOfResolvedValues.
    /// </summary>
    [Fact]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = Db2ValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

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
        Assert.Throws<Db2MockException>(() =>
            Db2ValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_Json_ShouldReturnJsonDocument_WhenValid behavior.
    /// PT: Testa o comportamento de Resolve_Json_ShouldReturnJsonDocument_WhenValid.
    /// </summary>
    [Fact]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = Db2ValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Tests Like_ShouldMatch_Db2Style behavior.
    /// PT: Testa o comportamento de Like_ShouldMatch_Db2Style.
    /// </summary>
    [Theory]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_Db2Style(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, Db2ValueHelper.Like(value, pattern));
    }

    /// <summary>
    /// EN: Tests Resolve_Enum_ShouldValidateAgainstColumnDef behavior.
    /// PT: Testa o comportamento de Resolve_Enum_ShouldValidateAgainstColumnDef.
    /// </summary>
    [Fact]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var tb = new Db2DbMock().AddTable("tb");

        tb.AddColumn("Status", DbType.String, false);

        tb.Columns["Status"].EnumValues.UnionWith(["active", "inactive"]);

        Db2ValueHelper.CurrentColumn = "Status";

        var ok = Db2ValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<Db2MockException>(() =>
            Db2ValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns));
        Assert.Equal(1265, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests Resolve_Set_ShouldReturnHashSet_AndValidate behavior.
    /// PT: Testa o comportamento de Resolve_Set_ShouldReturnHashSet_AndValidate.
    /// </summary>
    [Fact]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new Db2DbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
                enumValues: ["a", "b", "c"]);

        var prev = Db2ValueHelper.CurrentColumn;
        try
        {
            Db2ValueHelper.CurrentColumn = "Tags";

            var ok = Db2ValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<Db2MockException>(() =>
                Db2ValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            Db2ValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateStringSize behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateStringSize.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateStringSize()
    {
        var tb = new Db2DbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = Db2ValueHelper.CurrentColumn;
        try
        {
            Db2ValueHelper.CurrentColumn = "Name";
            var ex = Assert.Throws<Db2MockException>(() =>
                Db2ValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns));
            Assert.Equal(1406, ex.ErrorCode);
        }
        finally
        {
            Db2ValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateDecimalPlaces behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateDecimalPlaces.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateDecimalPlaces()
    {
        var tb = new Db2DbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = Db2ValueHelper.CurrentColumn;
        try
        {
            Db2ValueHelper.CurrentColumn = "Amount";
            var ex = Assert.Throws<Db2MockException>(() =>
                Db2ValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            Db2ValueHelper.CurrentColumn = prev;
        }
    }
}
