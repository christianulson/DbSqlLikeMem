namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers all DbType branches of SqlParameterDbTypeParserHelper.ParseDbType.
/// PT-br: Cobre todos os ramos de DbType do SqlParameterDbTypeParserHelper.ParseDbType.
/// </summary>
public sealed class SqlParameterDbTypeParserHelperTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies that integer-like SQL type tokens map to DbType.Int32.
    /// PT-br: Verifica que tokens de tipo SQL inteiro mapeiam para DbType.Int32.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("INT")]
    [InlineData("INTEGER")]
    [InlineData("SMALLINT")]
    [InlineData("int")]
    [InlineData("Integer")]
    [InlineData("  INTEGER  ")]
    public void ParseDbType_IntegerVariants_ShouldReturnInt32(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Int32);
    }

    /// <summary>
    /// EN: Verifies that BIGINT maps to DbType.Int64.
    /// PT-br: Verifica que BIGINT mapeia para DbType.Int64.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("BIGINT")]
    [InlineData("bigint")]
    public void ParseDbType_BigInt_ShouldReturnInt64(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Int64);
    }

    /// <summary>
    /// EN: Verifies DECIMAL and NUMERIC map to DbType.Decimal.
    /// PT-br: Verifica que DECIMAL e NUMERIC mapeiam para DbType.Decimal.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("DECIMAL")]
    [InlineData("NUMERIC")]
    [InlineData("DECIMAL(10,2)")]
    [InlineData("NUMERIC(5)")]
    public void ParseDbType_DecimalAndNumeric_ShouldReturnDecimal(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Decimal);
    }

    /// <summary>
    /// EN: Verifies NUMBER maps to DbType.Decimal.
    /// PT-br: Verifica que NUMBER mapeia para DbType.Decimal.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ParseDbType_Number_ShouldReturnDecimal()
    {
        SqlParameterDbTypeParserHelper.ParseDbType("NUMBER").Should().Be(DbType.Decimal);
    }

    /// <summary>
    /// EN: Verifies floating-point type tokens map to DbType.Double.
    /// PT-br: Verifica que tokens de tipo ponto flutuante mapeiam para DbType.Double.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("FLOAT")]
    [InlineData("REAL")]
    [InlineData("DOUBLE")]
    [InlineData("float")]
    public void ParseDbType_FloatVariants_ShouldReturnDouble(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Double);
    }

    /// <summary>
    /// EN: Verifies BOOLEAN and BOOL map to DbType.Boolean.
    /// PT-br: Verifica que BOOLEAN e BOOL mapeiam para DbType.Boolean.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("BOOLEAN")]
    [InlineData("BOOL")]
    [InlineData("boolean")]
    public void ParseDbType_BooleanVariants_ShouldReturnBoolean(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Boolean);
    }

    /// <summary>
    /// EN: Verifies DATE maps to DbType.Date.
    /// PT-br: Verifica que DATE mapeia para DbType.Date.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ParseDbType_Date_ShouldReturnDate()
    {
        SqlParameterDbTypeParserHelper.ParseDbType("DATE").Should().Be(DbType.Date);
    }

    /// <summary>
    /// EN: Verifies TIMESTAMP and DATETIME map to DbType.DateTime.
    /// PT-br: Verifica que TIMESTAMP e DATETIME mapeiam para DbType.DateTime.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("TIMESTAMP")]
    [InlineData("DATETIME")]
    [InlineData("timestamp")]
    public void ParseDbType_DateTimeVariants_ShouldReturnDateTime(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.DateTime);
    }

    /// <summary>
    /// EN: Verifies GUID and UUID map to DbType.Guid.
    /// PT-br: Verifica que GUID e UUID mapeiam para DbType.Guid.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("GUID")]
    [InlineData("UUID")]
    [InlineData("uuid")]
    public void ParseDbType_GuidVariants_ShouldReturnGuid(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Guid);
    }

    /// <summary>
    /// EN: Verifies binary type tokens map to DbType.Binary.
    /// PT-br: Verifica que tokens de tipo binário mapeiam para DbType.Binary.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("BLOB")]
    [InlineData("BINARY")]
    [InlineData("VARBINARY")]
    [InlineData("BYTEA")]
    [InlineData("varbinary")]
    public void ParseDbType_BinaryVariants_ShouldReturnBinary(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.Binary);
    }

    /// <summary>
    /// EN: Verifies that unknown or VARCHAR-style tokens fall back to DbType.String.
    /// PT-br: Verifica que tokens desconhecidos ou do estilo VARCHAR voltam para DbType.String.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("VARCHAR")]
    [InlineData("NVARCHAR")]
    [InlineData("CHAR")]
    [InlineData("TEXT")]
    [InlineData("CLOB")]
    [InlineData("NCHAR")]
    [InlineData("VARCHAR(255)")]
    public void ParseDbType_StringLikeTypes_ShouldReturnString(string typeSql)
    {
        SqlParameterDbTypeParserHelper.ParseDbType(typeSql).Should().Be(DbType.String);
    }
}
