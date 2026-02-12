namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class GenerationRuleSetTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
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
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [InlineData("tinyint", null, 8, "Flags", "MySql", "Byte")]
    [InlineData("tinyint", 1, null, "IsEnabled", "MySql", "Boolean")]
    [InlineData("tinyint", null, 1, "IsEnabled", "MySql", "Boolean")]
    [InlineData("bit", null, 1, "IsDeleted", "MySql", "Boolean")]
    [InlineData("bit", null, 8, "Mask", "MySql", "UInt64")]
    [InlineData("tinyint", null, 1, "IsEnabled", "SqlServer", "Byte")]
    [InlineData("uniqueidentifier", null, null, "Id", "SqlServer", "Guid")]
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
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [InlineData("binary", 16, null, "Token", "Guid")]
    [InlineData("varbinary", 16, null, "Token", "Guid")]
    [InlineData("char", 36, null, "OrderGuid", "Guid")]
    [InlineData("char", 36, null, "OrderUuid", "Guid")]
    [InlineData("char", 36, null, "OrderCode", "String")]
    public void MapDbType_AppliesGuidHeuristics(string dataType, long? charMaxLen, int? precision, string column, string expected)
    {
        var type = GenerationRuleSet.MapDbType(dataType, charMaxLen, precision, column, "SqlServer");
        Assert.Equal(expected, type);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
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
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
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
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void TryParseEnumValues_ReturnsEntriesForEnumAndSet()
    {
        var enumValues = GenerationRuleSet.TryParseEnumValues("enum('A','B','C')");
        var setValues = GenerationRuleSet.TryParseEnumValues("set('x','y')");

        Assert.Equal(["A", "B", "C"], enumValues);
        Assert.Equal(["x", "y"], setValues);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void TryParseEnumValues_ReturnsEmptyForNonEnumType()
    {
        var values = GenerationRuleSet.TryParseEnumValues("varchar(100)");
        Assert.Empty(values);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void TryConvertIfIsNull_ReturnsLambdaWhenPatternMatches()
    {
        var success = GenerationRuleSet.TryConvertIfIsNull("if((`deleted` is null), 1, null)", out var code);

        Assert.True(success);
        Assert.Contains("tb.Columns[\"deleted\"].Index", code, StringComparison.Ordinal);
        Assert.Contains("(byte?)1", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void TryConvertIfIsNull_ReturnsFalseWhenPatternDoesNotMatch()
    {
        var success = GenerationRuleSet.TryConvertIfIsNull("coalesce(`deleted`, 1)", out var code);

        Assert.False(success);
        Assert.Equal(string.Empty, code);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
    public void Literal_EscapesSlashAndQuotes()
    {
        var value = GenerationRuleSet.Literal("c:\\temp\\\"file\"");
        Assert.Equal("\"c:\\\\temp\\\\\\\"file\\\"\"", value);
    }
}
