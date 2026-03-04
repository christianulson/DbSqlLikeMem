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
}

