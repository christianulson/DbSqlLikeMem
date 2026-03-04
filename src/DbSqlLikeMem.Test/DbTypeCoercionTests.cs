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

    private enum SqlExtensionsEnumShort : short
    {
        A = 1
    }

    private enum SqlExtensionsEnumInt
    {
        A = 1
    }
}
