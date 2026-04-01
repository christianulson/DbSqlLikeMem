using FluentAssertions;
using System.Text.Json;

namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Covers SQL value resolution helpers in the Sqlite mock.
/// PT: Cobre os helpers de resolucao de valores SQL no mock Sqlite.
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
        using var cnn = new SqliteConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "p0";
        p.Value = 123;
        cmd.Parameters.Add(p);

        var v = SqliteValueHelper.Resolve("@p0", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        v.Should().Be(123);
    }

    /// <summary>
    /// EN: Verifies missing parameters raise an exception.
    /// PT: Verifica se parametros ausentes geram excecao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Action act = () => SqliteValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null);
        act.Should().Throw<SqliteMockException>();
    }

    /// <summary>
    /// EN: Verifies IN lists resolve to a list of values.
    /// PT: Verifica se listas IN sao resolvidas como uma lista de valores.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = SqliteValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

        var list = v.Should().BeOfType<List<object?>>().Which;
        list.Cast<int>().Should().Equal([1, 2, 3]);
    }

    /// <summary>
    /// EN: Verifies NULL is rejected for non-nullable columns.
    /// PT: Verifica se NULL e rejeitado para colunas nao anulaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_NullOnNonNullable_ShouldThrow()
    {
        Action act = () => SqliteValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null);
        act.Should().Throw<SqliteMockException>();
    }

    /// <summary>
    /// EN: Verifies valid JSON literals resolve to JsonDocument values.
    /// PT: Verifica se literais JSON validos sao resolvidos como JsonDocument.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = SqliteValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = v.Should().BeOfType<JsonDocument>().Which;
        doc.RootElement.GetProperty("a").GetInt32().Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies LIKE follows Sqlite-style wildcard matching.
    /// PT: Verifica se LIKE segue a correspondencia de curingas no estilo Sqlite.
    /// </summary>
    [Theory]
    [Trait("Category", "SqlValueHelperTests ")]
    [InlineData("John", "%oh%", true)]
    [InlineData("John", "J_hn", true)]
    [InlineData("John", "J__n", true)]
    [InlineData("John", "J__x", false)]
    [InlineData("John", "%OH%", true)] // ignore case
    public void Like_ShouldMatch_SqliteStyle(string value, string pattern, bool expected)
    {
        SqliteValueHelper.Like(value, pattern).Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies enum columns accept configured values and reject invalid ones.
    /// PT: Verifica se colunas enum aceitam valores configurados e rejeitam valores invalidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Enum_ShouldValidateAgainstColumnDef()
    {

        var tb = new SqliteDbMock().AddTable("tb");

        var c = tb.AddColumn("Status", DbType.String, false);
        c.EnumValues.UnionWith(["active", "inactive"]);

        SqliteValueHelper.CurrentColumn = "Status";

        var ok = SqliteValueHelper.Resolve("'Active'", DbType.String, false, null, tb.Columns);
        ok.Should().Be("active");

        var ex = Record.Exception(() => SqliteValueHelper.Resolve("'blocked'", DbType.String, false, null, tb.Columns));
        ex.Should().BeOfType<SqliteMockException>().Which.ErrorCode.Should().Be(1265);
    }

    /// <summary>
    /// EN: Verifies SET columns resolve to a hash set and validate allowed values.
    /// PT: Verifica se colunas SET sao resolvidas como um hash set e validam os valores permitidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Set_ShouldReturnHashSet_AndValidate()
    {
        var tb = new SqliteDbMock().AddTable("tb");

        tb.AddColumn("Tags", DbType.Int32, false,
                enumValues: ["a", "b", "c"]);

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Tags";

            var ok = SqliteValueHelper.Resolve("'a,b'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns);
            var hs = ok.Should().BeOfType<HashSet<string>>().Which;
            hs.Should().BeEquivalentTo(["a", "b"]);

            var ex = Record.Exception(() => SqliteValueHelper.Resolve("'a,x'", DbType.Int32, isNullable: false, pars: null, colDict: tb.Columns));
            ex.Should().BeOfType<SqliteMockException>().Which.ErrorCode.Should().Be(1265);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
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

        var tb = new SqliteDbMock().AddTable("tb");

        tb.AddColumn("Name", DbType.String, false, size: 3);

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Name";
            var ex = Record.Exception(() => SqliteValueHelper.Resolve("'abcd'", DbType.String, false, null, tb.Columns));
            ex.Should().BeOfType<SqliteMockException>().Which.ErrorCode.Should().Be(1406);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
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
        var tb = new SqliteDbMock().AddTable("tb");

        tb.AddColumn("Amount", DbType.Decimal, false, decimalPlaces: 2);

        var prev = SqliteValueHelper.CurrentColumn;
        try
        {
            SqliteValueHelper.CurrentColumn = "Amount";
            var ex = Record.Exception(() => SqliteValueHelper.Resolve("10.123", DbType.Decimal, false, null, tb.Columns));
            ex.Should().BeOfType<SqliteMockException>().Which.ErrorCode.Should().Be(1265);
        }
        finally
        {
            SqliteValueHelper.CurrentColumn = prev;
        }
    }
}
