namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers DbType parsing and .NET type mapping contracts used by core coercion paths.
/// PT-br: Cobre contratos de parsing de DbType e mapeamento de tipos .NET usados pelos caminhos centrais de coerção.
/// </summary>
public sealed class DbTypeCoercionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures DbType.Time literals parse to TimeSpan.
    /// PT-br: Garante que literais de DbType.Time sejam convertidos para TimeSpan.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_TimeLiteral_ShouldParseToTimeSpan()
    {
        var parsed = DbType.Time.Parse("'13:45:12'");

        var value = parsed.Should().BeOfType<TimeSpan>().Which;
        value.Should().Be(new TimeSpan(13, 45, 12));
    }

    /// <summary>
    /// EN: Ensures boolean aliases commonly used in SQL payloads are accepted.
    /// PT-br: Garante que aliases booleanos comuns em payloads SQL sejam aceitos.
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

        parsed.Should().BeOfType<bool>().Which.Should().Be(expected);
    }

    /// <summary>
    /// EN: Ensures DateTimeOffset type mapping is preserved in DbType conversion helpers.
    /// PT-br: Garante que o mapeamento de DateTimeOffset seja preservado nos helpers de conversão de DbType.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_DateTimeOffsetMapping_ShouldRoundTrip()
    {
        DbType.DateTimeOffset.ConvertDbTypeToType().Should().Be(typeof(DateTimeOffset));
        typeof(DateTimeOffset).ConvertTypeToDbType().Should().Be(DbType.DateTimeOffset);
    }

    /// <summary>
    /// EN: Ensures ConvertTypeToDbType unwraps Nullable and maps using the underlying scalar type.
    /// PT-br: Garante que ConvertTypeToDbType desembrulhe Nullable e mapeie usando o tipo escalar subjacente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_NullableType_ShouldMapFromUnderlyingType()
    {
        typeof(int?).ConvertTypeToDbType().Should().Be(DbType.Int32);
        typeof(DateTimeOffset?).ConvertTypeToDbType().Should().Be(DbType.DateTimeOffset);
    }

    /// <summary>
    /// EN: Ensures ConvertTypeToDbType maps enums through their integral backing type.
    /// PT-br: Garante que ConvertTypeToDbType mapeie enums pelo tipo integral subjacente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_EnumType_ShouldMapFromUnderlyingIntegralType()
    {
        typeof(SqlExtensionsEnumShort).ConvertTypeToDbType().Should().Be(DbType.Int16);
        typeof(SqlExtensionsEnumInt).ConvertTypeToDbType().Should().Be(DbType.Int32);
    }

    /// <summary>
    /// EN: Ensures parser supports byte and sbyte conversion paths.
    /// PT-br: Garante que o parser suporte caminhos de conversão para byte e sbyte.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_ByteAndSByte_ShouldParse()
    {
        DbType.Byte.Parse("255").Should().BeOfType<byte>().Which.Should().Be((byte)255);
        DbType.SByte.Parse("-12").Should().BeOfType<sbyte>().Which.Should().Be((sbyte)-12);
    }

    /// <summary>
    /// EN: Ensures parser accepts fixed and ANSI string DbType flavors as plain text.
    /// PT-br: Garante que o parser aceite variações de DbType string fixed/ANSI como texto simples.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_AnsiAndFixedString_ShouldParseAsString()
    {
        DbType.AnsiString.Parse("'abc'").Should().BeOfType<string>().Which.Should().Be("abc");
        DbType.StringFixedLength.Parse("'xyz'").Should().BeOfType<string>().Which.Should().Be("xyz");
    }

    /// <summary>
    /// EN: Ensures DbType->CLR mapping covers signed/unsigned integral families used by parser/executor coercion.
    /// PT-br: Garante que o mapeamento DbType->CLR cubra famílias integrais signed/unsigned usadas pela coerção de parser/executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_IntegralFamilies_ShouldMapToClrTypes()
    {
        DbType.SByte.ConvertDbTypeToType().Should().Be(typeof(sbyte));
        DbType.UInt16.ConvertDbTypeToType().Should().Be(typeof(ushort));
        DbType.UInt32.ConvertDbTypeToType().Should().Be(typeof(uint));
        DbType.UInt64.ConvertDbTypeToType().Should().Be(typeof(ulong));
    }

    /// <summary>
    /// EN: Ensures VarNumeric keeps numeric semantics across parser and type mapping helpers.
    /// PT-br: Garante que VarNumeric mantenha semântica numérica entre parser e helpers de mapeamento de tipo.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeExtension_AndParser_VarNumeric_ShouldRemainNumeric()
    {
        DbType.VarNumeric.ConvertDbTypeToType().Should().Be(typeof(decimal));
        DbType.VarNumeric.Parse("123.45").Should().BeOfType<decimal>().Which.Should().Be(123.45m);
    }

    /// <summary>
    /// EN: Ensures object parsing infers common scalar/JSON forms for broader literal coverage.
    /// PT-br: Garante que o parsing de object infira formas comuns escalares/JSON para maior cobertura de literais.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferCommonLiteralForms()
    {
        var json = DbType.Object.Parse("{\"a\":1}");
        var boolean = DbType.Object.Parse("on");
        var number = DbType.Object.Parse("12.5");
        var text = DbType.Object.Parse("'hello'");

        json.Should().BeOfType<JsonDocument>();
        boolean.Should().BeOfType<bool>().Which.Should().BeTrue();
        number.Should().BeOfType<decimal>().Which.Should().Be(12.5m);
        text.Should().BeOfType<string>().Which.Should().Be("hello");
    }

    /// <summary>
    /// EN: Ensures binary parser accepts hexadecimal SQL-style literals with 0x prefix.
    /// PT-br: Garante que o parser binário aceite literais estilo SQL hexadecimal com prefixo 0x.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryHexLiteral_ShouldParseBytes()
    {
        var parsed = DbType.Binary.Parse("0x0A0B0C").Should().BeOfType<byte[]>().Which;
        parsed.Should().Equal([0x0A, 0x0B, 0x0C]);
    }

    /// <summary>
    /// EN: Ensures binary parser accepts SQL quoted-hex format X'ABCD'.
    /// PT-br: Garante que o parser binário aceite o formato SQL hex quoted X'ABCD'.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryQuotedHexLiteral_ShouldParseBytes()
    {
        var parsedUpper = DbType.Binary.Parse("X'0A0B0C'").Should().BeOfType<byte[]>().Which;
        var parsedLower = DbType.Binary.Parse("x'0A0B0C'").Should().BeOfType<byte[]>().Which;

        parsedUpper.Should().Equal([0x0A, 0x0B, 0x0C]);
        parsedLower.Should().Equal([0x0A, 0x0B, 0x0C]);
    }

    /// <summary>
    /// EN: Ensures binary parser accepts PostgreSQL bytea hexadecimal format with \x prefix.
    /// PT-br: Garante que o parser binário aceite formato hexadecimal bytea do PostgreSQL com prefixo \x.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_BinaryPostgreSqlHexLiteral_ShouldParseBytes()
    {
        var parsed = DbType.Binary.Parse("\\x0A0B0C").Should().BeOfType<byte[]>().Which;

        parsed.Should().Equal([0x0A, 0x0B, 0x0C]);
    }

    /// <summary>
    /// EN: Ensures object parsing infers Guid and DateTimeOffset literals for richer coercion coverage.
    /// PT-br: Garante que o parsing de object infira literais de Guid e DateTimeOffset para maior cobertura de coerção.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferGuidAndDateTimeOffset()
    {
        const string guidText = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
        const string dateText = "2024-01-02T03:04:05+00:00";

        var guidParsed = DbType.Object.Parse(guidText);
        var dateParsed = DbType.Object.Parse(dateText);

        guidParsed.Should().BeOfType<Guid>().Which.Should().Be(Guid.Parse(guidText));
        dateParsed.Should().BeOfType<DateTimeOffset>().Which.Should().Be(DateTimeOffset.Parse(dateText, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures object parsing infers DateTime and TimeSpan literals when no offset information is present.
    /// PT-br: Garante que o parsing de object infira literais DateTime e TimeSpan quando não há informação de offset.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_ShouldInferDateTimeAndTimeSpan()
    {
        const string dateText = "2024-01-02 03:04:05";
        const string spanText = "01:30:00";

        var dateParsed = DbType.Object.Parse(dateText);
        var spanParsed = DbType.Object.Parse(spanText);

        dateParsed.Should().BeOfType<DateTime>().Which.Should().Be(DateTime.Parse(dateText, CultureInfo.InvariantCulture));
        spanParsed.Should().BeOfType<TimeSpan>().Which.Should().Be(TimeSpan.Parse(spanText, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures quoted NULL tokens are interpreted as null for object parsing.
    /// PT-br: Garante que tokens NULL entre aspas sejam interpretados como null no parsing de object.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_Object_QuotedNull_ShouldReturnNull()
    {
        DbType.Object.Parse("'null'").Should().BeNull();
        DbType.Object.Parse("\"NULL\"").Should().BeNull();
    }

    /// <summary>
    /// EN: Ensures quoted NULL tokens remain textual for string DbType parsing.
    /// PT-br: Garante que tokens NULL entre aspas permaneçam textuais no parsing de DbType string.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbTypeParser_String_QuotedNull_ShouldRemainText()
    {
        DbType.String.Parse("'null'").Should().Be("null");
        DbType.AnsiString.Parse("\"NULL\"").Should().Be(SqlConst.NULL);
    }

    /// <summary>
    /// EN: Ensures boolean parser supports compact aliases used by some dialects.
    /// PT-br: Garante que o parser booleano suporte aliases compactos usados por alguns dialetos.
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

        parsed.Should().BeOfType<bool>().Which.Should().Be(expected);
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
