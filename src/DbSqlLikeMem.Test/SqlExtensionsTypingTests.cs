namespace DbSqlLikeMem.Test;

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
