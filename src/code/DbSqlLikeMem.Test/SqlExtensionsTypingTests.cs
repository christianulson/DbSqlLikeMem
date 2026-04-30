namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers core coercion/comparison behaviors from SqlExtensions used by AST execution.
/// PT: Cobre comportamentos centrais de coerção/comparação de SqlExtensions usados pela execução AST.
/// </summary>
public sealed class SqlExtensionsTypingTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures ToBool parses decimal text using invariant culture.
    /// PT: Garante que ToBool faça parse de texto decimal usando cultura invariável.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_DecimalText_ShouldUseInvariantCulture()
    {
        "1.50".ToBool().Should().BeTrue();
        "0.00".ToBool().Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures ToBool handles common textual boolean aliases.
    /// PT: Garante que ToBool trate aliases textuais comuns de booleano.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_TextAliases_ShouldSupportOnOffYesNo()
    {
        "yes".ToBool().Should().BeTrue();
        "on".ToBool().Should().BeTrue();
        "no".ToBool().Should().BeFalse();
        "off".ToBool().Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures ToBool handles signed/unsigned integral families consistently.
    /// PT: Garante que ToBool trate famílias integrais signed/unsigned de forma consistente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_IntegralFamilies_ShouldUseZeroAsFalse()
    {
        ((sbyte)0).ToBool().Should().BeFalse();
        ((sbyte)-1).ToBool().Should().BeTrue();
        ((ushort)0).ToBool().Should().BeFalse();
        ((ushort)1).ToBool().Should().BeTrue();
        ((uint)0).ToBool().Should().BeFalse();
        ((uint)2).ToBool().Should().BeTrue();
        ((ulong)0).ToBool().Should().BeFalse();
        ((ulong)3).ToBool().Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures ToBool does not overflow for ulong values above Int64.MaxValue.
    /// PT: Garante que ToBool não estoure para valores ulong acima de Int64.MaxValue.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_UlongAboveInt64Max_ShouldNotOverflow()
    {
        ulong.MaxValue.ToBool().Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures ToDec maps boolean values to numeric SQL-like semantics.
    /// PT: Garante que ToDec mapeie valores booleanos para semântica numérica estilo SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToDec_BooleanValues_ShouldMapToOneOrZero()
    {
        true.ToDec().Should().Be(1m);
        false.ToDec().Should().Be(0m);
    }

    /// <summary>
    /// EN: Ensures ToDec handles signed/unsigned integral families without textual fallback ambiguity.
    /// PT: Garante que ToDec trate famílias integrais signed/unsigned sem ambiguidade de fallback textual.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToDec_IntegralFamilies_ShouldMapDeterministically()
    {
        ((sbyte)-5).ToDec().Should().Be(-5m);
        ((ushort)7).ToDec().Should().Be(7m);
        ((uint)11).ToDec().Should().Be(11m);
        ((ulong)13).ToDec().Should().Be(13m);
    }

    /// <summary>
    /// EN: Ensures ToDec keeps temporal numeric comparability for DateTimeOffset and TimeSpan.
    /// PT: Garante que ToDec mantenha comparabilidade numérica temporal para DateTimeOffset e TimeSpan.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToDec_TemporalValues_ShouldMapToTicks()
    {
        var offset = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(-3));
        var span = TimeSpan.FromMinutes(90);

        offset.ToDec().Should().Be(offset.Ticks);
        span.ToDec().Should().Be(span.Ticks);
    }

    /// <summary>
    /// EN: Ensures mixed numeric/boolean values can be compared through implicit numeric coercion.
    /// PT: Garante que valores numéricos/booleanos mistos possam ser comparados via coerção numérica implícita.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void EqualsSql_BooleanAndNumericString_WithImplicitNumericComparison_ShouldMatch()
    {
        true.EqualsSql("1").Should().BeTrue();
        false.EqualsSql("0").Should().BeTrue();
        1.EqualsSql(true).Should().BeTrue();
        0.EqualsSql(false).Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures Compare uses numeric coercion for mixed boolean/numeric values when enabled.
    /// PT: Garante que Compare use coerção numérica para valores booleanos/numéricos mistos quando habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Compare_BooleanAndNumericString_WithImplicitNumericComparison_ShouldUseNumericOrdering()
    {
        (true.Compare("0") > 0).Should().BeTrue();
        (false.Compare("1") < 0).Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures temporal mixed types are comparable when implicit numeric coercion is enabled.
    /// PT: Garante que tipos temporais mistos sejam comparáveis quando a coerção numérica implícita está habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Compare_DateTimeAndDateTimeOffset_WithImplicitNumericComparison_ShouldUseTicks()
    {
        var dateTime = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var offset = new DateTimeOffset(dateTime);

        dateTime.Compare(offset).Should().Be(0);
    }

    /// <summary>
    /// EN: Ensures temporal mixed types use textual fallback when implicit numeric coercion is disabled.
    /// PT: Garante que tipos temporais mistos usem fallback textual quando a coerção numérica implícita está desabilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void EqualsSql_DateTimeAndDateTimeOffset_WithImplicitNumericDisabled_ShouldNotUseTickCoercion()
    {
        var dialect = new SqlExtensionsTestDialect(supportsImplicitNumericStringComparison: false);
        var dateTime = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var offset = new DateTimeOffset(dateTime);

        dateTime.EqualsSql(offset, dialect).Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures binary values compare by content equality instead of reference identity.
    /// PT: Garante que valores binários comparem por igualdade de conteúdo em vez de identidade de referência.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void EqualsSql_ByteArrays_ShouldUseContentEquality()
    {
        var left = new byte[] { 1, 2, 3 };
        var rightSame = new byte[] { 1, 2, 3 };
        var rightDifferent = new byte[] { 1, 2, 4 };

        left.EqualsSql(rightSame).Should().BeTrue();
        left.EqualsSql(rightDifferent).Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures binary comparison ordering is deterministic and lexicographical.
    /// PT: Garante que a ordenação de comparação binária seja determinística e lexicográfica.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Compare_ByteArrays_ShouldUseLexicographicalOrdering()
    {
        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 4 };
        var c = new byte[] { 1, 2, 3, 0 };

        (a.Compare(b) < 0).Should().BeTrue();
        (b.Compare(a) > 0).Should().BeTrue();
        (a.Compare(c) < 0).Should().BeTrue();
        a.Compare(new byte[] { 1, 2, 3 }).Should().Be(0);
    }

    /// <summary>
    /// EN: Ensures implicit numeric coercion can be disabled by dialect policy.
    /// PT: Garante que a coerção numérica implícita possa ser desabilitada pela política do dialeto.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void EqualsSql_NumericAndString_WithImplicitNumericDisabled_ShouldFallbackToText()
    {
        var dialect = new SqlExtensionsTestDialect(supportsImplicitNumericStringComparison: false);

        2.EqualsSql("2", dialect).Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures Compare follows textual ordering when implicit numeric coercion is disabled.
    /// PT: Garante que Compare siga ordenação textual quando a coerção numérica implícita está desabilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Compare_NumericAndString_WithImplicitNumericDisabled_ShouldUseTextOrdering()
    {
        var dialect = new SqlExtensionsTestDialect(supportsImplicitNumericStringComparison: false);

        // Text compare: "10" < "2"
        (dialect.Compare(10, "2") < 0).Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures an explicit ESCAPE expression makes LIKE treat wildcard tokens as literals.
    /// PT: Garante que uma expressão ESCAPE explícita faça o LIKE tratar curingas como literais.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Like_WithExplicitEscapeExpression_ShouldTreatEscapedWildcardAsLiteral()
    {
        var dialect = new SqlExtensionsTestDialect(
            supportsImplicitNumericStringComparison: true,
            likeDefaultEscapeCharacter: null);

        dialect.Like("Jo_n", "Jo#_%", "#").Should().BeTrue();
        dialect.Like("Joan", "Jo#_%", "#").Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures the default LIKE escape behavior follows the configured dialect policy.
    /// PT: Garante que o escape padrão de LIKE siga a política configurada no dialeto.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Like_DefaultEscapeBehavior_ShouldFollowDialectPolicy()
    {
        var mysqlLikeDialect = new SqlExtensionsTestDialect(
            supportsImplicitNumericStringComparison: true,
            likeDefaultEscapeCharacter: '\\');
        var ansiLikeDialect = new SqlExtensionsTestDialect(
            supportsImplicitNumericStringComparison: true,
            likeDefaultEscapeCharacter: null);

        mysqlLikeDialect.Like("Jo_n", @"Jo\_%").Should().BeTrue();
        ansiLikeDialect.Like("Jo_n", @"Jo\_%").Should().BeFalse();
    }

    /// <summary>
    /// EN: Ensures invalid explicit ESCAPE values are rejected when the dialect requires a single character.
    /// PT: Garante que valores ESCAPE explícitos inválidos sejam rejeitados quando o dialeto exige um único caractere.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Like_WithMultiCharacterExplicitEscape_ShouldThrow()
    {
        var dialect = new SqlExtensionsTestDialect(
            supportsImplicitNumericStringComparison: true,
            likeDefaultEscapeCharacter: null);

        var action = () => dialect.Like("Jo_n", "Jo#_%", "##");
        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*single character*");
    }

    private sealed class SqlExtensionsTestDialect : SqlDialectBase
    {
        private readonly bool _supportsImplicitNumericStringComparison;
        private readonly char? _likeDefaultEscapeCharacter;

        /// <summary>
        /// EN: Initializes a lightweight dialect stub for SqlExtensions behavior tests.
        /// PT: Inicializa um stub de dialeto leve para testes de comportamento de SqlExtensions.
        /// </summary>
        public SqlExtensionsTestDialect(
            bool supportsImplicitNumericStringComparison,
            char? likeDefaultEscapeCharacter = '\\')
            : base(
                name: "test",
                version: 1,
                keywords: [],
                binOps: [],
                operators: [])
        {
            _supportsImplicitNumericStringComparison = supportsImplicitNumericStringComparison;
            _likeDefaultEscapeCharacter = likeDefaultEscapeCharacter;
        }

        /// <summary>
        /// EN: Gets whether implicit numeric/string comparison is enabled.
        /// PT: Obtém se a comparação implícita número/string está habilitada.
        /// </summary>
        public override bool SupportsImplicitNumericStringComparison => _supportsImplicitNumericStringComparison;

        /// <summary>
        /// EN: Gets the default LIKE escape character used by this lightweight dialect stub.
        /// PT: Obtém o caractere padrão de escape do LIKE usado por este stub leve de dialeto.
        /// </summary>
        public override char? LikeDefaultEscapeCharacter => _likeDefaultEscapeCharacter;
    }
}
