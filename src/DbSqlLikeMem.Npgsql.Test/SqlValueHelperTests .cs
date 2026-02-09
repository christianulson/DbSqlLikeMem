using System.Text.Json;

namespace DbSqlLikeMem.Npgsql.Test;

public sealed class SqlValueHelperTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Fact]
    public void Resolve_ShouldReadDapperParameter_ByName()
    {
        using var cnn = new NpgsqlConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = NpgsqlValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(123, v);
    }

    [Fact]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    [Fact]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = NpgsqlValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = Assert.IsType<List<object?>>(v);
        Assert.Equal([1, 2, 3], [.. list.Cast<int>()]);
    }

    [Fact]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    [Fact]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = NpgsqlValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    [Theory]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_MySqlStyle(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, NpgsqlValueHelper.Like(value, pattern));
    }

    [Fact]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var cols = new ColumnDictionary
        {
            ["Status"] = new ColumnDef(0, DbType.String, false)
        };
        cols["Status"].EnumValues.UnionWith(["active", "inactive"]);

        NpgsqlValueHelper.CurrentColumn = "Status";

        var ok = NpgsqlValueHelper.Resolve("'Active'", DbType.String, false, null, cols);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("'blocked'", DbType.String, false, null, cols));
        Assert.Equal(1265, ex.ErrorCode);
    }

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

        var prev = NpgsqlValueHelper.CurrentColumn;
        try
        {
            NpgsqlValueHelper.CurrentColumn = "Tags";

            var ok = NpgsqlValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: cols);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<NpgsqlMockException>(() =>
                NpgsqlValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: cols));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            NpgsqlValueHelper.CurrentColumn = prev;
        }
    }
}
