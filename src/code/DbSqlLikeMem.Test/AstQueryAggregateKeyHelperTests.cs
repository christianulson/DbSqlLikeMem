namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers all type branches of AstQueryAggregateKeyHelper for string aggregate conversion.
/// PT-br: Cobre todos os ramos de tipo do AstQueryAggregateKeyHelper para conversão de agregados de string.
/// </summary>
public sealed class AstQueryAggregateKeyHelperTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    // ─── TryGetStringAggregateText ───────────────────────────────────────────

    /// <summary>
    /// EN: Verifies null and DBNull inputs return false with an empty text.
    /// PT-br: Verifica que entradas null e DBNull retornam false com texto vazio.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_NullLike_ShouldReturnFalse()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(null, out var t1).Should().BeFalse();
        t1.Should().BeEmpty();

        AstQueryAggregateKeyHelper.TryGetStringAggregateText(DBNull.Value, out var t2).Should().BeFalse();
        t2.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Verifies string values are returned as-is.
    /// PT-br: Verifica que valores string são retornados sem modificação.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_String_ShouldReturnSameText()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText("hello", out var text).Should().BeTrue();
        text.Should().Be("hello");
    }

    /// <summary>
    /// EN: Verifies decimal values are formatted with invariant culture.
    /// PT-br: Verifica que valores decimal são formatados com cultura invariante.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_Decimal_ShouldFormatInvariant()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(123.45m, out var text).Should().BeTrue();
        text.Should().Be("123.45");
    }

    /// <summary>
    /// EN: Verifies double values are formatted with round-trip specifier.
    /// PT-br: Verifica que valores double são formatados com especificador round-trip.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_Double_ShouldFormatRoundTrip()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(3.14, out var text).Should().BeTrue();
        text.Should().Be(((double)3.14).ToString("R", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies float values are formatted with round-trip specifier.
    /// PT-br: Verifica que valores float são formatados com especificador round-trip.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_Float_ShouldFormatRoundTrip()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(1.5f, out var text).Should().BeTrue();
        text.Should().Be(((float)1.5f).ToString("R", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies DateTime values are converted to UTC ISO-8601 round-trip format.
    /// PT-br: Verifica que valores DateTime são convertidos para formato ISO-8601 round-trip em UTC.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_DateTime_ShouldFormatUtcIso()
    {
        var dt = new DateTime(2024, 3, 15, 10, 30, 0, DateTimeKind.Utc);
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(dt, out var text).Should().BeTrue();
        text.Should().Be(dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies bool true maps to "1" and false maps to "0".
    /// PT-br: Verifica que bool true mapeia para "1" e false para "0".
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData(true, "1")]
    [InlineData(false, "0")]
    public void TryGetStringAggregateText_Bool_ShouldMapToOneOrZero(bool value, string expected)
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(value, out var text).Should().BeTrue();
        text.Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies other types fall through to ToString().
    /// PT-br: Verifica que outros tipos usam ToString() como fallback.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateText_OtherType_ShouldUsToString()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateText(42, out var text).Should().BeTrue();
        text.Should().Be("42");
    }

    // ─── TryGetStringAggregateKeyAndText ────────────────────────────────────

    /// <summary>
    /// EN: Verifies null inputs return false for both text and distinctKey.
    /// PT-br: Verifica que entradas null retornam false para text e distinctKey.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_NullLike_ShouldReturnFalse()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(null, useOrdinalTextComparison: true, out var t, out var k).Should().BeFalse();
        t.Should().BeEmpty();
        k.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Verifies string produces text and key equal to the original value.
    /// PT-br: Verifica que string produz text e key iguais ao valor original.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_String_ShouldMatchTextAndKey()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText("world", useOrdinalTextComparison: false, out var text, out var key).Should().BeTrue();
        text.Should().Be("world");
        key.Should().Be("world");
    }

    /// <summary>
    /// EN: Verifies decimal produces the invariant-formatted text as both text and key.
    /// PT-br: Verifica que decimal produz o texto formatado invariante como text e key.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_Decimal_ShouldMatchTextAndKey()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(9.99m, useOrdinalTextComparison: true, out var text, out var key).Should().BeTrue();
        text.Should().Be("9.99");
        key.Should().Be(text);
    }

    /// <summary>
    /// EN: Verifies double round-trip text is produced as both text and key.
    /// PT-br: Verifica que o texto round-trip de double é produzido como text e key.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_Double_ShouldMatchTextAndKey()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(2.71, useOrdinalTextComparison: true, out var text, out var key).Should().BeTrue();
        key.Should().Be(text);
        text.Should().Be(((double)2.71).ToString("R", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies float round-trip text is produced correctly.
    /// PT-br: Verifica que o texto round-trip de float é produzido corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_Float_ShouldMatchTextAndKey()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(0.5f, useOrdinalTextComparison: false, out var text, out var key).Should().BeTrue();
        key.Should().Be(text);
    }

    /// <summary>
    /// EN: Verifies DateTime is converted to UTC ISO-8601 for both text and key.
    /// PT-br: Verifica que DateTime é convertido para UTC ISO-8601 como text e key.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_DateTime_ShouldProduceUtcIsoKey()
    {
        var dt = new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(dt, useOrdinalTextComparison: true, out var text, out var key).Should().BeTrue();
        key.Should().Be(text);
        text.Should().Contain("2025");
    }

    /// <summary>
    /// EN: Verifies bool true and false map to "1" and "0" for both text and key.
    /// PT-br: Verifica que bool true e false mapeiam para "1" e "0" como text e key.
    /// </summary>
    [Theory]
    [Trait("Category", "Core")]
    [InlineData(true, "1")]
    [InlineData(false, "0")]
    public void TryGetStringAggregateKeyAndText_Bool_ShouldMapToOneOrZero(bool value, string expected)
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison: true, out var text, out var key).Should().BeTrue();
        text.Should().Be(expected);
        key.Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies other types fall through to ToString() for both text and key.
    /// PT-br: Verifica que outros tipos usam ToString() como fallback para text e key.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void TryGetStringAggregateKeyAndText_OtherType_ShouldUseToString()
    {
        AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(99L, useOrdinalTextComparison: false, out var text, out var key).Should().BeTrue();
        text.Should().Be("99");
        key.Should().Be("99");
    }
}
