namespace DbSqlLikeMem.Npgsql;

internal sealed class NpgsqlDialect : SqlDialectBase
{
    internal const string DialectName = "postgresql";

    internal NpgsqlDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: ["ILIKE"],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>("AND", SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>("OR", SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("=", SqlBinaryOp.Eq),
            new KeyValuePair<string, SqlBinaryOp>("<>", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>("!=", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>(">", SqlBinaryOp.Greater),
            new KeyValuePair<string, SqlBinaryOp>(">=", SqlBinaryOp.GreaterOrEqual),
            new KeyValuePair<string, SqlBinaryOp>("<", SqlBinaryOp.Less),
            new KeyValuePair<string, SqlBinaryOp>("<=", SqlBinaryOp.LessOrEqual),
        ],
        operators:
        [
            "->>", "->",
            "#>>", "#>",
            "::",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }


    // NOTE: in this project the Npgsql "version" axis starts at 6 and
    // parser feature tests expect WITH/CTE support across all tested versions.
    internal const int WithCteMinVersion = 6;
    internal const int MergeMinVersion = 15;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool AllowsDoubleQuoteIdentifiers => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    /// <summary>
    /// EN: Uses case-insensitive textual comparisons in the in-memory executor for deterministic tests.
    /// PT: Usa comparações textuais case-insensitive no executor em memória para testes determinísticos.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Enables implicit numeric/string comparison only when both values are numeric-convertible.
    /// PT: Habilita comparação implícita numérica/string apenas quando ambos os valores são conversíveis para número.
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Keeps LIKE case-insensitive by default in the mock provider.
    /// PT: Mantém LIKE case-insensitive por padrão no provider mock.
    /// </summary>
    public override bool LikeIsCaseInsensitive => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDollarQuotedStrings => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsLimitOffset => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsFetchFirst => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOffsetFetch => true;
    public override bool SupportsOrderByNullsModifier => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsOnConflictClause => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsReturning => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsJsonArrowOperators => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    public override bool SupportsWithMaterializedHint => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => [];
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = tableName;
        if (string.IsNullOrWhiteSpace(schemaName)) return TemporaryTableScope.None;
        return schemaName!.StartsWith("pg_temp", StringComparison.OrdinalIgnoreCase)
            ? TemporaryTableScope.Connection
            : TemporaryTableScope.None;
    }

    public override bool SupportsDateAddFunction(string functionName)
        => false;
}
