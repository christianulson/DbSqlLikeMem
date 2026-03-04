namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Covers core coercion/comparison behaviors from SqlExtensions used by AST execution.
/// PT: Cobre comportamentos centrais de coerção/comparação de SqlExtensions usados pela execução AST.
/// </summary>
public sealed class SqlExtensionsTypingTests
{
    /// <summary>
    /// EN: Ensures ToBool parses decimal text using invariant culture.
    /// PT: Garante que ToBool faça parse de texto decimal usando cultura invariável.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_DecimalText_ShouldUseInvariantCulture()
    {
        Assert.True("1.50".ToBool());
        Assert.False("0.00".ToBool());
    }

    /// <summary>
    /// EN: Ensures ToBool handles common textual boolean aliases.
    /// PT: Garante que ToBool trate aliases textuais comuns de booleano.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_TextAliases_ShouldSupportOnOffYesNo()
    {
        Assert.True("yes".ToBool());
        Assert.True("on".ToBool());
        Assert.False("no".ToBool());
        Assert.False("off".ToBool());
    }

    /// <summary>
    /// EN: Ensures ToBool handles signed/unsigned integral families consistently.
    /// PT: Garante que ToBool trate famílias integrais signed/unsigned de forma consistente.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_IntegralFamilies_ShouldUseZeroAsFalse()
    {
        Assert.False(((sbyte)0).ToBool());
        Assert.True(((sbyte)-1).ToBool());
        Assert.False(((ushort)0).ToBool());
        Assert.True(((ushort)1).ToBool());
        Assert.False(((uint)0).ToBool());
        Assert.True(((uint)2).ToBool());
        Assert.False(((ulong)0).ToBool());
        Assert.True(((ulong)3).ToBool());
    }

    /// <summary>
    /// EN: Ensures ToBool does not overflow for ulong values above Int64.MaxValue.
    /// PT: Garante que ToBool não estoure para valores ulong acima de Int64.MaxValue.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToBool_UlongAboveInt64Max_ShouldNotOverflow()
    {
        Assert.True(ulong.MaxValue.ToBool());
    }

    /// <summary>
    /// EN: Ensures ToDec maps boolean values to numeric SQL-like semantics.
    /// PT: Garante que ToDec mapeie valores booleanos para semântica numérica estilo SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToDec_BooleanValues_ShouldMapToOneOrZero()
    {
        Assert.Equal(1m, true.ToDec());
        Assert.Equal(0m, false.ToDec());
    }

    /// <summary>
    /// EN: Ensures ToDec handles signed/unsigned integral families without textual fallback ambiguity.
    /// PT: Garante que ToDec trate famílias integrais signed/unsigned sem ambiguidade de fallback textual.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void ToDec_IntegralFamilies_ShouldMapDeterministically()
    {
        Assert.Equal(-5m, ((sbyte)-5).ToDec());
        Assert.Equal(7m, ((ushort)7).ToDec());
        Assert.Equal(11m, ((uint)11).ToDec());
        Assert.Equal(13m, ((ulong)13).ToDec());
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

        Assert.Equal(offset.Ticks, offset.ToDec());
        Assert.Equal(span.Ticks, span.ToDec());
    }

    /// <summary>
    /// EN: Ensures mixed numeric/boolean values can be compared through implicit numeric coercion.
    /// PT: Garante que valores numéricos/booleanos mistos possam ser comparados via coerção numérica implícita.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void EqualsSql_BooleanAndNumericString_WithImplicitNumericComparison_ShouldMatch()
    {
        Assert.True(true.EqualsSql("1"));
        Assert.True(false.EqualsSql("0"));
        Assert.True(1.EqualsSql(true));
        Assert.True(0.EqualsSql(false));
    }

    /// <summary>
    /// EN: Ensures Compare uses numeric coercion for mixed boolean/numeric values when enabled.
    /// PT: Garante que Compare use coerção numérica para valores booleanos/numéricos mistos quando habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Compare_BooleanAndNumericString_WithImplicitNumericComparison_ShouldUseNumericOrdering()
    {
        Assert.True(true.Compare("0") > 0);
        Assert.True(false.Compare("1") < 0);
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

        Assert.Equal(0, dateTime.Compare(offset));
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

        Assert.False(dateTime.EqualsSql(offset, dialect));
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

        Assert.True(left.EqualsSql(rightSame));
        Assert.False(left.EqualsSql(rightDifferent));
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

        Assert.True(a.Compare(b) < 0);
        Assert.True(b.Compare(a) > 0);
        Assert.True(a.Compare(c) < 0);
        Assert.Equal(0, a.Compare(new byte[] { 1, 2, 3 }));
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

        Assert.False(2.EqualsSql("2", dialect));
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
        Assert.True(10.Compare("2", dialect) < 0);
    }

    private sealed class SqlExtensionsTestDialect : SqlDialectBase
    {
        private readonly bool _supportsImplicitNumericStringComparison;

        /// <summary>
        /// EN: Initializes a lightweight dialect stub for SqlExtensions behavior tests.
        /// PT: Inicializa um stub de dialeto leve para testes de comportamento de SqlExtensions.
        /// </summary>
        public SqlExtensionsTestDialect(bool supportsImplicitNumericStringComparison)
            : base(
                name: "test",
                version: 1,
                keywords: [],
                binOps: [],
                operators: [])
        {
            _supportsImplicitNumericStringComparison = supportsImplicitNumericStringComparison;
        }

        /// <summary>
        /// EN: Gets whether implicit numeric/string comparison is enabled.
        /// PT: Obtém se a comparação implícita número/string está habilitada.
        /// </summary>
        public override bool SupportsImplicitNumericStringComparison => _supportsImplicitNumericStringComparison;
    }
}
