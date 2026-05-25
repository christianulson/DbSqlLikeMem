using System.Text.Json;

namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Covers SQL value resolution helpers in the MySql mock.
/// PT-br: Cobre os helpers de resolucao de valores SQL no mock MySql.
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
        using var cnn = new MySqlConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = MySqlValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        v.Should().Be(123);
    }

    /// <summary>
    /// EN: Verifies missing parameters raise an exception.
    /// PT-br: Verifica se parametros ausentes geram excecao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        FluentActions.Invoking(() => MySqlValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null))
            .Should().Throw<MySqlMockException>();
    }

    /// <summary>
    /// EN: Verifies IN lists resolve to a list of values.
    /// PT-br: Verifica se listas IN sao resolvidas como uma lista de valores.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = MySqlValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = v.Should().BeOfType<List<object?>>().Which;
        list.Cast<int>().Should().Equal(new[] { 1, 2, 3 });
    }

    /// <summary>
    /// EN: Verifies NULL is rejected for non-nullable columns.
    /// PT-br: Verifica se NULL e rejeitado para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        FluentActions.Invoking(() => MySqlValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null))
            .Should().Throw<MySqlMockException>();
    }

    /// <summary>
    /// EN: Verifies NULL is returned as null for nullable columns.
    /// PT-br: Verifica se NULL e retornado como null para colunas anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_NullOnNullable_ShouldReturnNull()
    {
        var value = MySqlValueHelper.Resolve("null", DbType.Int32, isNullable: true, pars: null, colDict: null);

        value.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies valid JSON literals resolve to JsonDocument values.
    /// PT-br: Verifica se literais JSON validos sao resolvidos como JsonDocument.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = MySqlValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = v.Should().BeOfType<JsonDocument>().Which;
        doc.RootElement.GetProperty("a").GetInt32().Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies invalid JSON literals remain as raw strings.
    /// PT-br: Verifica se literais JSON invalidos permanecem como strings brutas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_InvalidJson_ShouldReturnRawString()
    {
        var value = MySqlValueHelper.Resolve("{invalid json", DbType.Object, isNullable: false, pars: null, colDict: null);

        value.Should().Be("{invalid json");
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
        MySqlValueHelper.Like(value, pattern).Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies enum columns accept configured values and reject invalid ones.
    /// PT-br: Verifica se colunas enum aceitam valores configurados e rejeitam valores invalidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {
        var tb = new MySqlDbMock().AddTable("tb");

        tb.AddColumn("Status", DbType.String, false)
        .EnumValues.UnionWith(["active", "inactive"]);

        MySqlValueHelper.CurrentColumn = "Status";

        var ok = MySqlValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        ok.Should().Be("active");

        var ex = FluentActions.Invoking(() => MySqlValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns))
            .Should().Throw<MySqlMockException>().Which;
        ex.ErrorCode.Should().Be(1265);
    }

    /// <summary>
    /// EN: Verifies SET columns resolve to a hash set and validate allowed values.
    /// PT-br: Verifica se colunas SET sao resolvidas como um hash set e validam os valores permitidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new MySqlDbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
                enumValues: ["a", "b", "c"]);

        var prev = MySqlValueHelper.CurrentColumn;
        try
        {
            MySqlValueHelper.CurrentColumn = "Tags";

            var ok = MySqlValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = ok.Should().BeOfType<HashSet<string>>().Which;
            hs.SetEquals(["a", "b"]).Should().BeTrue();

            var ex = FluentActions.Invoking(() => MySqlValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns))
                .Should().Throw<MySqlMockException>().Which;
            ex.ErrorCode.Should().Be(1265);
        }
        finally
        {
            MySqlValueHelper.CurrentColumn = prev;
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
        var tb = new MySqlDbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = MySqlValueHelper.CurrentColumn;
        try
        {
            MySqlValueHelper.CurrentColumn = "Name";
            var ex = FluentActions.Invoking(() => MySqlValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns))
                .Should().Throw<MySqlMockException>().Which;
            ex.ErrorCode.Should().Be(1406);
        }
        finally
        {
            MySqlValueHelper.CurrentColumn = prev;
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
        var tb = new MySqlDbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = MySqlValueHelper.CurrentColumn;
        try
        {
            MySqlValueHelper.CurrentColumn = "Amount";
            var ex = FluentActions.Invoking(() => MySqlValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns))
                .Should().Throw<MySqlMockException>().Which;
            ex.ErrorCode.Should().Be(1265);
        }
        finally
        {
            MySqlValueHelper.CurrentColumn = prev;
        }
    }
}
