using System.Text.Json;

namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Covers SQL value resolution helpers in the Npgsql mock.
/// PT-br: Cobre os helpers de resolucao de valores SQL no mock Npgsql.
/// </summary>
public sealed class SqlValueHelperTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parameters can be resolved by name.
    /// PT-br: Verifica se parametros podem ser resolvidos pelo nome.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
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

    /// <summary>
    /// EN: Verifies missing parameters raise an exception.
    /// PT-br: Verifica se parametros ausentes geram excecao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies IN lists resolve to a list of values.
    /// PT-br: Verifica se listas IN sao resolvidas como uma lista de valores.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = NpgsqlValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = Assert.IsType<List<object?>>(v);
        Assert.Equal([1, 2, 3], [.. list.Cast<int>()]);
    }

    /// <summary>
    /// EN: Verifies NULL is rejected for non-nullable columns.
    /// PT-br: Verifica se NULL e rejeitado para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies valid JSON literals resolve to JsonDocument values.
    /// PT-br: Verifica se literais JSON validos sao resolvidos como JsonDocument.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = NpgsqlValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Verifies LIKE follows MySQL-style wildcard matching.
    /// PT-br: Verifica se LIKE segue a correspondencia de curingas no estilo MySQL.
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
        Assert.Equal(expected, NpgsqlValueHelper.Like(value, pattern));
    }

    /// <summary>
    /// EN: Verifies enum columns accept configured values and reject invalid ones.
    /// PT-br: Verifica se colunas enum aceitam valores configurados e rejeitam valores invalidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var tb = new NpgsqlDbMock().AddTable("tb");

        tb.AddColumn("Status", DbType.String, false)
        .EnumValues.UnionWith(["active", "inactive"]);

        NpgsqlValueHelper.CurrentColumn = "Status";

        var ok = NpgsqlValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        Assert.Equal("active", ok);

        var ex = Assert.Throws<NpgsqlMockException>(() =>
            NpgsqlValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns));
        Assert.Equal(1265, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Verifies SET columns resolve to a hash set and validate allowed values.
    /// PT-br: Verifica se colunas SET sao resolvidas como um hash set e validam os valores permitidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new NpgsqlDbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
                enumValues: ["a", "b", "c"]);

        var prev = NpgsqlValueHelper.CurrentColumn;
        try
        {
            NpgsqlValueHelper.CurrentColumn = "Tags";

            var ok = NpgsqlValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = Assert.IsType<HashSet<string>>(ok);
            Assert.True(hs.SetEquals(["a", "b"]));

            var ex = Assert.Throws<NpgsqlMockException>(() =>
                NpgsqlValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns));
            Assert.Equal(1265, ex.ErrorCode);
        }
        finally
        {
            NpgsqlValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Verifies string values respect the declared size limit.
    /// PT-br: Verifica se valores de texto respeitam o limite de tamanho declarado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldValidateStringSize()
    {
        var tb = new NpgsqlDbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = NpgsqlValueHelper.CurrentColumn;
        try
        {
            NpgsqlValueHelper.CurrentColumn = "Name";
            var ex = Assert.Throws<NpgsqlMockException>(() =>
                NpgsqlValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns));
            Assert.Equal(22001, ex.ErrorCode);
        }
        finally
        {
            NpgsqlValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Verifies decimal values respect the declared scale.
    /// PT-br: Verifica se valores decimais respeitam a escala declarada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldValidateDecimalPlaces()
    {
        var tb = new NpgsqlDbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = NpgsqlValueHelper.CurrentColumn;
        try
        {
            NpgsqlValueHelper.CurrentColumn = "Amount";
            var ex = Assert.Throws<NpgsqlMockException>(() =>
                NpgsqlValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns));
            Assert.Equal(22003, ex.ErrorCode);
        }
        finally
        {
            NpgsqlValueHelper.CurrentColumn = prev;
        }
    }

    /// <summary>
    /// EN: Verifies timestamptz values preserve their original offset when resolved through the Npgsql helper.
    /// PT-br: Verifica se valores timestamptz preservam o offset original ao serem resolvidos pelo helper do Npgsql.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldPreserveDateTimeOffset_Offset()
    {
        var value = new DateTimeOffset(2026, 1, 19, 8, 15, 30, TimeSpan.FromHours(-3));

        using var connection = new NpgsqlConnectionMock();
        using var command = connection.CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = "p0";
        parameter.Value = value;
        command.Parameters.Add(parameter);

        var resolved = NpgsqlValueHelper.Resolve(
            "@p0",
            DbType.DateTimeOffset,
            isNullable: false,
            pars: command.Parameters,
            colDict: null);

        var dto = Assert.IsType<DateTimeOffset>(resolved);
        Assert.Equal(value.Offset, dto.Offset);
        Assert.Equal(value, dto);
    }
}
