using System.Text.Json;

namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Covers SQL value resolution helpers in the SqlServer mock.
/// PT: Cobre os helpers de resolucao de valores SQL no mock SqlServer.
/// </summary>
public sealed class SqlValueHelperTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parameters can be resolved by name.
    /// PT: Verifica se parametros podem ser resolvidos pelo nome.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldReadDapperParameter_ByName()
    {
        using var cnn = new SqlServerConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = SqlServerValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(123, v);
    }

    /// <summary>
    /// EN: Verifies missing parameters raise an exception.
    /// PT: Verifica se parametros ausentes geram excecao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<SqlServerMockException>(() =>
            SqlServerValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies IN lists resolve to a list of values.
    /// PT: Verifica se listas IN sao resolvidas como uma lista de valores.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = SqlServerValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = Assert.IsType<List<object?>>(v);
        Assert.Equal([1, 2, 3], [.. list.Cast<int>()]);
    }

    /// <summary>
    /// EN: Verifies NULL is rejected for non-nullable columns.
    /// PT: Verifica se NULL e rejeitado para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        Assert.Throws<SqlServerMockException>(() =>
            SqlServerValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies valid JSON literals resolve to JsonDocument values.
    /// PT: Verifica se literais JSON validos sao resolvidos como JsonDocument.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = SqlServerValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Verifies LIKE follows MySQL-style wildcard matching.
    /// PT: Verifica se LIKE segue a correspondencia de curingas no estilo MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "SqlValueHelperTests ")]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_MySqlStyle(string value, string pattern, bool expected)
    {
        Assert.Equal(expected, SqlServerValueHelper.Like(value, pattern));
    }

    /// <summary>
    /// EN: Verifies enum columns accept configured values and reject invalid ones.
    /// PT: Verifica se colunas enum aceitam valores configurados e rejeitam valores invalidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var tb = new SqlServerDbMock().AddTable("tb");

        tb.AddColumn("Status", DbType.String, false)
            .EnumValues.UnionWith(["active", "inactive"]);

        SqlServerValueHelper.CurrentColumn = "Status";

        var ok = SqlServerValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<SqlServerMockException>(() =>
            SqlServerValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns));
        Assert.Equal(1265, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Verifies SET columns resolve to a hash set and validate allowed values.
    /// PT: Verifica se colunas SET sao resolvidas como um hash set e validam os valores permitidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new SqlServerDbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
            enumValues: ["a", "b", "c"]);

        var prev = SqlServerValueHelper.CurrentColumn;
        try
        {
            SqlServerValueHelper.CurrentColumn = "Tags";

            var ok = SqlServerValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<SqlServerMockException>(() =>
                SqlServerValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            SqlServerValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Verifies string values respect the declared size limit.
    /// PT: Verifica se valores de texto respeitam o limite de tamanho declarado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldValidateStringSize()
    {
        var tb = new SqlServerDbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = SqlServerValueHelper.CurrentColumn;
        try
        {
            SqlServerValueHelper.CurrentColumn = "Name";
            var ex = Assert.Throws<SqlServerMockException>(() =>
                SqlServerValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns));
            Assert.Equal(8152, ex.ErrorCode);
        }
        finally
        {
            SqlServerValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Verifies decimal values respect the declared scale.
    /// PT: Verifica se valores decimais respeitam a escala declarada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldValidateDecimalPlaces()
    {
        var tb = new SqlServerDbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = SqlServerValueHelper.CurrentColumn;
        try
        {
            SqlServerValueHelper.CurrentColumn = "Amount";
            var ex = Assert.Throws<SqlServerMockException>(() =>
                SqlServerValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns));
            Assert.Equal(8115, ex.ErrorCode);
        }
        finally
        {
            SqlServerValueHelper.CurrentColumn = prev;
        }
    }
}
