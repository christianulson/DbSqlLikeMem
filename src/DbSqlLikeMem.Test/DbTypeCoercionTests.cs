namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers DbType parsing and .NET type mapping contracts used by core coercion paths.
/// PT: Cobre contratos de parsing de DbType e mapeamento de tipos .NET usados pelos caminhos centrais de coerção.
/// </summary>
public sealed class DbTypeCoercionTests
{
    /// <summary>
    /// EN: Ensures DbType.Time literals parse to TimeSpan.
    /// PT: Garante que literais de DbType.Time sejam convertidos para TimeSpan.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_TimeLiteral_ShouldParseToTimeSpan()
    {
        var parsed = DbType.Time.Parse("'13:45:12'");

        var value = Assert.IsType<TimeSpan>(parsed);
        Assert.Equal(new TimeSpan(13, 45, 12), value);
    }

    /// <summary>
    /// EN: Ensures boolean aliases commonly used in SQL payloads are accepted.
    /// PT: Garante que aliases booleanos comuns em payloads SQL sejam aceitos.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("yes", true)]
    [InlineData("no", false)]
    [InlineData("on", true)]
    [InlineData("off", false)]
    public void DbTypeParser_BooleanAliases_ShouldParse(string literal, bool expected)
    {
        var parsed = DbType.Boolean.Parse(literal);

        Assert.Equal(expected, Assert.IsType<bool>(parsed));
    }

    /// <summary>
    /// EN: Ensures DateTimeOffset type mapping is preserved in DbType conversion helpers.
    /// PT: Garante que o mapeamento de DateTimeOffset seja preservado nos helpers de conversão de DbType.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_DateTimeOffsetMapping_ShouldRoundTrip()
    {
        Assert.Equal(typeof(DateTimeOffset), DbType.DateTimeOffset.ConvertDbTypeToType());
        Assert.Equal(DbType.DateTimeOffset, typeof(DateTimeOffset).ConvertTypeToDbType());
    }

    /// <summary>
    /// EN: Ensures ConvertTypeToDbType unwraps Nullable and maps using the underlying scalar type.
    /// PT: Garante que ConvertTypeToDbType desembrulhe Nullable e mapeie usando o tipo escalar subjacente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_NullableType_ShouldMapFromUnderlyingType()
    {
        Assert.Equal(DbType.Int32, typeof(int?).ConvertTypeToDbType());
        Assert.Equal(DbType.DateTimeOffset, typeof(DateTimeOffset?).ConvertTypeToDbType());
    }

    /// <summary>
    /// EN: Ensures ConvertTypeToDbType maps enums through their integral backing type.
    /// PT: Garante que ConvertTypeToDbType mapeie enums pelo tipo integral subjacente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_EnumType_ShouldMapFromUnderlyingIntegralType()
    {
        Assert.Equal(DbType.Int16, typeof(SqlExtensionsEnumShort).ConvertTypeToDbType());
        Assert.Equal(DbType.Int32, typeof(SqlExtensionsEnumInt).ConvertTypeToDbType());
    }

    /// <summary>
    /// EN: Ensures parser supports byte and sbyte conversion paths.
    /// PT: Garante que o parser suporte caminhos de conversão para byte e sbyte.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_ByteAndSByte_ShouldParse()
    {
        Assert.Equal((byte)255, Assert.IsType<byte>(DbType.Byte.Parse("255")));
        Assert.Equal((sbyte)-12, Assert.IsType<sbyte>(DbType.SByte.Parse("-12")));
    }

    /// <summary>
    /// EN: Ensures parser accepts fixed and ANSI string DbType flavors as plain text.
    /// PT: Garante que o parser aceite variações de DbType string fixed/ANSI como texto simples.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_AnsiAndFixedString_ShouldParseAsString()
    {
        Assert.Equal("abc", Assert.IsType<string>(DbType.AnsiString.Parse("'abc'")));
        Assert.Equal("xyz", Assert.IsType<string>(DbType.StringFixedLength.Parse("'xyz'")));
    }

    /// <summary>
    /// EN: Ensures DbType->CLR mapping covers signed/unsigned integral families used by parser/executor coercion.
    /// PT: Garante que o mapeamento DbType->CLR cubra famílias integrais signed/unsigned usadas pela coerção de parser/executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_IntegralFamilies_ShouldMapToClrTypes()
    {
        Assert.Equal(typeof(sbyte), DbType.SByte.ConvertDbTypeToType());
        Assert.Equal(typeof(ushort), DbType.UInt16.ConvertDbTypeToType());
        Assert.Equal(typeof(uint), DbType.UInt32.ConvertDbTypeToType());
        Assert.Equal(typeof(ulong), DbType.UInt64.ConvertDbTypeToType());
    }

    /// <summary>
    /// EN: Ensures VarNumeric keeps numeric semantics across parser and type mapping helpers.
    /// PT: Garante que VarNumeric mantenha semântica numérica entre parser e helpers de mapeamento de tipo.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_AndParser_VarNumeric_ShouldRemainNumeric()
    {
        Assert.Equal(typeof(decimal), DbType.VarNumeric.ConvertDbTypeToType());
        Assert.Equal(123.45m, Assert.IsType<decimal>(DbType.VarNumeric.Parse("123.45")));
    }

    /// <summary>
    /// EN: Ensures object parsing infers common scalar/JSON forms for broader literal coverage.
    /// PT: Garante que o parsing de object infira formas comuns escalares/JSON para maior cobertura de literais.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferCommonLiteralForms()
    {
        var json = DbType.Object.Parse("{\"a\":1}");
        var boolean = DbType.Object.Parse("on");
        var number = DbType.Object.Parse("12.5");
        var text = DbType.Object.Parse("'hello'");

        Assert.IsType<System.Text.Json.JsonDocument>(json);
        Assert.True(Assert.IsType<bool>(boolean));
        Assert.Equal(12.5m, Assert.IsType<decimal>(number));
        Assert.Equal("hello", Assert.IsType<string>(text));
    }

    /// <summary>
    /// EN: Ensures binary parser accepts hexadecimal SQL-style literals with 0x prefix.
    /// PT: Garante que o parser binário aceite literais estilo SQL hexadecimal com prefixo 0x.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryHexLiteral_ShouldParseBytes()
    {
        var parsed = Assert.IsType<byte[]>(DbType.Binary.Parse("0x0A0B0C"));

        Assert.Equal([0x0A, 0x0B, 0x0C], parsed);
    }

    /// <summary>
    /// EN: Ensures binary parser accepts SQL quoted-hex format X'ABCD'.
    /// PT: Garante que o parser binário aceite o formato SQL hex quoted X'ABCD'.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryQuotedHexLiteral_ShouldParseBytes()
    {
        var parsedUpper = Assert.IsType<byte[]>(DbType.Binary.Parse("X'0A0B0C'"));
        var parsedLower = Assert.IsType<byte[]>(DbType.Binary.Parse("x'0A0B0C'"));

        Assert.Equal([0x0A, 0x0B, 0x0C], parsedUpper);
        Assert.Equal([0x0A, 0x0B, 0x0C], parsedLower);
    }

    /// <summary>
    /// EN: Ensures binary parser accepts PostgreSQL bytea hexadecimal format with \x prefix.
    /// PT: Garante que o parser binário aceite formato hexadecimal bytea do PostgreSQL com prefixo \x.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryPostgreSqlHexLiteral_ShouldParseBytes()
    {
        var parsed = Assert.IsType<byte[]>(DbType.Binary.Parse("\\x0A0B0C"));

        Assert.Equal([0x0A, 0x0B, 0x0C], parsed);
    }

    /// <summary>
    /// EN: Ensures object parsing infers Guid and DateTimeOffset literals for richer coercion coverage.
    /// PT: Garante que o parsing de object infira literais de Guid e DateTimeOffset para maior cobertura de coerção.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferGuidAndDateTimeOffset()
    {
        const string guidText = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        const string dateText = "2024-01-02T03:04:05+00:00";

        var guidParsed = DbType.Object.Parse(guidText);
        var dateParsed = DbType.Object.Parse(dateText);

        Assert.Equal(Guid.Parse(guidText), Assert.IsType<Guid>(guidParsed));
        Assert.Equal(DateTimeOffset.Parse(dateText, CultureInfo.InvariantCulture), Assert.IsType<DateTimeOffset>(dateParsed));
    }

    /// <summary>
    /// EN: Ensures object parsing infers DateTime and TimeSpan literals when no offset information is present.
    /// PT: Garante que o parsing de object infira literais DateTime e TimeSpan quando não há informação de offset.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferDateTimeAndTimeSpan()
    {
        const string dateText = "2024-01-02 03:04:05";
        const string spanText = "01:30:00";

        var dateParsed = DbType.Object.Parse(dateText);
        var spanParsed = DbType.Object.Parse(spanText);

        Assert.Equal(DateTime.Parse(dateText, CultureInfo.InvariantCulture), Assert.IsType<DateTime>(dateParsed));
        Assert.Equal(TimeSpan.Parse(spanText, CultureInfo.InvariantCulture), Assert.IsType<TimeSpan>(spanParsed));
    }

    /// <summary>
    /// EN: Ensures quoted NULL tokens are interpreted as null for object parsing.
    /// PT: Garante que tokens NULL entre aspas sejam interpretados como null no parsing de object.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_QuotedNull_ShouldReturnNull()
    {
        Assert.Null(DbType.Object.Parse("'null'"));
        Assert.Null(DbType.Object.Parse("\"NULL\""));
    }

    /// <summary>
    /// EN: Ensures quoted NULL tokens remain textual for string DbType parsing.
    /// PT: Garante que tokens NULL entre aspas permaneçam textuais no parsing de DbType string.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_String_QuotedNull_ShouldRemainText()
    {
        Assert.Equal("null", DbType.String.Parse("'null'"));
        Assert.Equal("NULL", DbType.AnsiString.Parse("\"NULL\""));
    }

    /// <summary>
    /// EN: Ensures boolean parser supports compact aliases used by some dialects.
    /// PT: Garante que o parser booleano suporte aliases compactos usados por alguns dialetos.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData("t", true)]
    [InlineData("T", true)]
    [InlineData("f", false)]
    [InlineData("F", false)]
    public void DbTypeParser_BooleanCompactAliases_ShouldParse(string literal, bool expected)
    {
        var parsed = DbType.Boolean.Parse(literal);

        Assert.Equal(expected, Assert.IsType<bool>(parsed));
    }

    private enum SqlExtensionsEnumShort : short
    {
        A = 1
    }

    private enum SqlExtensionsEnumInt
    {
        A = 1
    }
}
