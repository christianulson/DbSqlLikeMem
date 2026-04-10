namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies database generation rules used by the extension core.
/// PT: Verifica as regras de geracao de banco usadas pelo core da extensao.
/// </summary>
public sealed class GenerationRuleSetTests
{
    /// <summary>
    /// EN: Verifies PascalCase conversion for common identifier formats.
    /// PT: Verifica a conversao para PascalCase em formatos comuns de identificador.
    /// </summary>
    [Theory]
    [Trait("Category", "GenerationRuleSet")]
    [InlineData("customer_order", "CustomerOrder")]
    [InlineData("customer-order", "CustomerOrder")]
    [InlineData("customer.order", "CustomerOrder")]
    [InlineData("customer order", "CustomerOrder")]
    [InlineData("123-name", "123Name")]
    [InlineData("", "Object")]
    [InlineData("___", "Object")]
    public void ToPascalCase_TransformsCommonInputs(string input, string expected)
    {
        var value = GenerationRuleSet.ToPascalCase(input);
        Assert.Equal(expected, value);
    }

    /// <summary>
    /// EN: Verifies CLR type mapping for database column types and provider rules.
    /// PT: Verifica o mapeamento de tipos CLR para tipos de coluna e regras do provedor.
    /// </summary>
    [Theory]
    [Trait("Category", "GenerationRuleSet")]
    [InlineData("tinyint", null, 8, "Flags", "MySql", "Byte")]
    [InlineData("tinyint", 1L, null, "IsEnabled", "MySql", "Boolean")]
    [InlineData("tinyint", null, 1, "IsEnabled", "MySql", "Boolean")]
    [InlineData("bit", null, 1, "IsDeleted", "MySql", "Boolean")]
    [InlineData("bit", null, 8, "Mask", "MySql", "UInt64")]
    [InlineData("tinyint", null, 1, "IsEnabled", "SqlServer", "Byte")]
    [InlineData("uniqueidentifier", null, null, "Id", "SqlServer", "Guid")]
    [InlineData("7", null, null, "Count", "Firebird", "Int16")]
    [InlineData("8", null, null, "Count", "Firebird", "Int32")]
    [InlineData("8", null, 5, "Amount", "Firebird", "Decimal")]
    [InlineData("23", null, null, "IsEnabled", "Firebird", "Boolean")]
    [InlineData("37", 50L, null, "Name", "Firebird", "String")]
    public void MapDbType_ResolvesStrategiesAndRules(
        string dataType,
        long? charMaxLen,
        int? numPrecision,
        string column,
        string databaseType,
        string expected)
    {
        var type = GenerationRuleSet.MapDbType(dataType, charMaxLen, numPrecision, column, databaseType);
        Assert.Equal(expected, type);
    }

    /// <summary>
    /// EN: Verifies GUID heuristics for binary and character columns.
    /// PT: Verifica heuristicas de GUID para colunas binaria e de caractere.
    /// </summary>
    [Theory]
    [Trait("Category", "GenerationRuleSet")]
    [InlineData("binary", 16L, null, "Token", "Guid")]
    [InlineData("varbinary", 16L, null, "Token", "Guid")]
    [InlineData("char", 36L, null, "OrderGuid", "Guid")]
    [InlineData("char", 36L, null, "OrderUuid", "Guid")]
    [InlineData("char", 36L, null, "OrderCode", "String")]
    public void MapDbType_AppliesGuidHeuristics(string dataType, long? charMaxLen, int? precision, string column, string expected)
    {
        var type = GenerationRuleSet.MapDbType(dataType, charMaxLen, precision, column, "SqlServer");
        Assert.Equal(expected, type);
    }

    /// <summary>
    /// EN: Verifies literal default detection used by the generator.
    /// PT: Verifica a deteccao de literais padrao usada pela geracao.
    /// </summary>
    [Theory]
    [Trait("Category", "GenerationRuleSet")]
    [InlineData("(now())", false)]
    [InlineData("current_timestamp", false)]
    [InlineData("null", false)]
    [InlineData("'abc'", true)]
    [InlineData("42", true)]
    public void IsSimpleLiteralDefault_ReturnsExpectedValue(string value, bool expected)
    {
        Assert.Equal(expected, GenerationRuleSet.IsSimpleLiteralDefault(value));
    }

    /// <summary>
    /// EN: Verifies default literal formatting for boolean, string, and numeric values.
    /// PT: Verifica a formatacao de literais padrao para valores booleanos, string e numericos.
    /// </summary>
    [Theory]
    [Trait("Category", "GenerationRuleSet")]
    [InlineData("('0')", "Boolean", "false")]
    [InlineData("('1')", "Boolean", "true")]
    [InlineData("('abc')", "String", "\"abc\"")]
    [InlineData("(123)", "Int32", "123")]
    public void FormatDefaultLiteral_FormatsByTargetType(string input, string dbType, string expected)
    {
        var value = GenerationRuleSet.FormatDefaultLiteral(input, dbType);
        Assert.Equal(expected, value);
    }

    /// <summary>
    /// EN: Verifies enum and set parsing returns the expected discrete values.
    /// PT: Verifica se o parser de enum e set retorna os valores discretos esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "GenerationRuleSet")]
    public void TryParseEnumValues_ReturnsEntriesForEnumAndSet()
    {
        var enumValues = GenerationRuleSet.TryParseEnumValues("enum('A','B','C')");
        var setValues = GenerationRuleSet.TryParseEnumValues("set('x','y')");

        Assert.Equal(["A", "B", "C"], enumValues);
        Assert.Equal(["x", "y"], setValues);
    }

    /// <summary>
    /// EN: Verifies enum parsing returns no values for non-enum inputs.
    /// PT: Verifica se o parser de enum nao retorna valores para entradas nao enum.
    /// </summary>
    [Fact]
    [Trait("Category", "GenerationRuleSet")]
    public void TryParseEnumValues_ReturnsEmptyForNonEnumType()
    {
        var values = GenerationRuleSet.TryParseEnumValues("varchar(100)");
        Assert.Empty(values);
    }

    /// <summary>
    /// EN: Verifies the generated lambda for SQL IS NULL expressions.
    /// PT: Verifica a lambda gerada para expressoes SQL IS NULL.
    /// </summary>
    [Fact]
    [Trait("Category", "GenerationRuleSet")]
    public void TryConvertIfIsNull_ReturnsLambdaWhenPatternMatches()
    {
        var success = GenerationRuleSet.TryConvertIfIsNull("if((`deleted` is null), 1, null)", out var code);

        Assert.True(success);
        Assert.Contains("tb.Columns[\"deleted\"].Index", code, StringComparison.Ordinal);
        Assert.Contains("(byte?)1", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// EN: Verifies the fallback behavior when the SQL pattern does not match.
    /// PT: Verifica o comportamento de fallback quando o padrao SQL nao corresponde.
    /// </summary>
    [Fact]
    [Trait("Category", "GenerationRuleSet")]
    public void TryConvertIfIsNull_ReturnsFalseWhenPatternDoesNotMatch()
    {
        var success = GenerationRuleSet.TryConvertIfIsNull("coalesce(`deleted`, 1)", out var code);

        Assert.False(success);
        Assert.Equal(string.Empty, code);
    }

    /// <summary>
    /// EN: Verifies string escaping for generated C# literals.
    /// PT: Verifica o escaping de string para literais C# gerados.
    /// </summary>
    [Fact]
    [Trait("Category", "GenerationRuleSet")]
    public void Literal_EscapesSlashAndQuotes()
    {
        var value = GenerationRuleSet.Literal("c:\\temp\\\"file\"");
        Assert.Equal("\"c:\\\\temp\\\\\\\"file\\\"\"", value);
    }
}
