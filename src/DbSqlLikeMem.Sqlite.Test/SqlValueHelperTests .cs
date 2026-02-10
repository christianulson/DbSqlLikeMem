using System.Text.Json;

namespace DbSqlLikeMem.Sqlite.Test;

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
        using var cnn = new SqliteConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = SqliteValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(123, v);
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldThrow_WhenParameterMissing behavior.
    /// PT: Testa o comportamento de Resolve_ShouldThrow_WhenParameterMissing.
    /// </summary>
    [Fact]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<SqliteMockException>(() =>
            SqliteValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldParseInList_ToListOfResolvedValues behavior.
    /// PT: Testa o comportamento de Resolve_ShouldParseInList_ToListOfResolvedValues.
    /// </summary>
    [Fact]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = SqliteValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

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
        Assert.Throws<SqliteMockException>(() =>
            SqliteValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Tests Resolve_Json_ShouldReturnJsonDocument_WhenValid behavior.
    /// PT: Testa o comportamento de Resolve_Json_ShouldReturnJsonDocument_WhenValid.
    /// </summary>
    [Fact]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = SqliteValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Tests Like_ShouldMatch_SqliteStyle behavior.
    /// PT: Testa o comportamento de Like_ShouldMatch_SqliteStyle.
    /// </summary>
    [Theory]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_SqliteStyle(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, SqliteValueHelper.Like(value, pattern));
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

        SqliteValueHelper.CurrentColumn = "Status";

        var ok = SqliteValueHelper.Resolve("'Active'", DbType.String, false, null, cols);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<SqliteMockException>(() =>
            SqliteValueHelper.Resolve("'blocked'", DbType.String, false, null, cols));
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

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Tags";

            var ok = SqliteValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: cols);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<SqliteMockException>(() =>
                SqliteValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: cols));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateStringSize behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateStringSize.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateStringSize()
    {
        var cols = new ColumnDictionary
        {
            ["Name"] = new ColumnDef(0, DbType.String, false) { Size = 3 }
        };

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Name";
            var ex = Assert.Throws<SqliteMockException>(() =>
                SqliteValueHelper.Resolve("'abcd'", DbType.String, false, null, cols));
            Assert.Equal(1406, ex.ErrorCode);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Tests Resolve_ShouldValidateDecimalPlaces behavior.
    /// PT: Testa o comportamento de Resolve_ShouldValidateDecimalPlaces.
    /// </summary>
    [Fact]
    public void Resolve_ShouldValidateDecimalPlaces()
    {
        var cols = new ColumnDictionary
        {
            ["Amount"] = new ColumnDef(0, DbType.Decimal, false) { DecimalPlaces = 2 }
        };

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Amount";
            var ex = Assert.Throws<SqliteMockException>(() =>
                SqliteValueHelper.Resolve("10.123", DbType.Decimal, false, null, cols));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
        }
    }
}
