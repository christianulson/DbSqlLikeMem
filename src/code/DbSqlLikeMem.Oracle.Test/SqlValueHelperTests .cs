namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Covers SQL value resolution helpers in the Oracle mock.
/// PT: Cobre os helpers de resolucao de valores SQL no mock Oracle.
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
    /// EN: Verifies parameters can be resolved with a colon prefix.
    /// PT: Verifica se parametros podem ser resolvidos com prefixo de dois pontos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldReadDapperParameter_WithColonPrefix()
    {
        using var cnn = new OracleConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = "Id";
        p.Value = 1;
        cmd.Parameters.Add(p);

        var v = OracleValueHelper.Resolve(":Id", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(1, v);
    }

    /// <summary>
    /// EN: Verifies parameters can be resolved when the collection stores the prefixed name.
    /// PT: Verifica se parametros podem ser resolvidos quando a colecao armazena o nome com prefixo.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldReadDapperParameter_WhenCollectionStoresPrefixedName()
    {
        using var cnn = new OracleConnectionMock();
        using var cmd = cnn.CreateCommand();

        var p = cmd.CreateParameter();
        p.ParameterName = ":Id";
        p.Value = 1;
        cmd.Parameters.Add(p);

        var v = OracleValueHelper.Resolve(":Id", DbType.Int32, isNullable: false, cmd.Parameters, colDict: null);

        Assert.Equal(1, v);
    }

    /// <summary>
    /// EN: Verifies missing parameters raise an exception.
    /// PT: Verifica se parametros ausentes geram excecao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldThrow_WhenParameterMissing()
    {
        Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("@p404", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies IN lists resolve to a list of values.
    /// PT: Verifica se listas IN sao resolvidas como uma lista de valores.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_ShouldParseInList_ToListOfResolvedValues()
    {
        var v = OracleValueHelper.Resolve("(1, 2, 3)", DbType.Int32, isNullable: false, pars: null, colDict: null);

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
        Assert.Throws<OracleMockException>(() =>
            OracleValueHelper.Resolve("null", DbType.Int32, isNullable: false, pars: null, colDict: null));
    }

    /// <summary>
    /// EN: Verifies valid JSON literals resolve to JsonDocument values.
    /// PT: Verifica se literais JSON validos sao resolvidos como JsonDocument.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
    public void Resolve_Json_ShouldReturnJsonDocument_WhenValid()
    {
        var v = OracleValueHelper.Resolve("{\"a\":1}", DbType.Object, isNullable: false, pars: null, colDict: null);

        var doc = Assert.IsType<JsonDocument>(v);
        Assert.Equal(1, doc.RootElement.GetProperty("a").GetInt32());
    }

    /// <summary>
    /// EN: Validates OracleValueHelper LIKE matching semantics against representative patterns.
    /// PT: Valida a semântica de correspondência do LIKE no OracleValueHelper com padrões representativos.
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
        Assert.Equal(expected, OracleValueHelper.Like(value, pattern));
    }

    /// <summary>
    /// EN: Verifies enum columns accept configured values and reject invalid ones.
    /// PT: Verifica se colunas enum aceitam valores configurados e rejeitam valores invalidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
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
    /// EN: Verifies SET columns resolve to a hash set and validate allowed values.
    /// PT: Verifica se colunas SET sao resolvidas como um hash set e validam os valores permitidos.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
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
    /// EN: Verifies string values respect the declared size limit.
    /// PT: Verifica se valores de texto respeitam o limite de tamanho declarado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
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
    /// EN: Verifies decimal values respect the declared scale.
    /// PT: Verifica se valores decimais respeitam a escala declarada.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlValueHelperTests ")]
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
