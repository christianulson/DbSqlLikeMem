namespace DbSqlLikeMem;

/// <summary>
/// EN: String escaping styles supported by the parser.
/// PT: Estilos de escape de string suportados pelo parser.
/// </summary>
internal enum SqlStringEscapeStyle { backslash, doubled_quote }
/// <summary>
/// EN: Identifier escaping styles supported by the parser.
/// PT: Estilos de escape de identificador suportados pelo parser.
/// </summary>
internal enum SqlIdentifierEscapeStyle { double_quote, backtick, bracket }

internal readonly record struct SqlQuotePair(char Begin, char End);

/// <summary>
/// EN: Defines escape rules and behavior for a SQL dialect.
/// PT: Define regras de escape e comportamento de um dialeto SQL.
/// </summary>
internal interface ISqlDialect
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int Version { get; }
    string Name { get; }

    // Identifier quoting
    bool AllowsBacktickIdentifiers { get; }
    bool AllowsDoubleQuoteIdentifiers { get; }
    bool AllowsBracketIdentifiers { get; }
    SqlIdentifierEscapeStyle IdentifierEscapeStyle { get; }

    // Quote pairs (dialect-aware scanning helpers)
    IReadOnlyList<SqlQuotePair> IdentifierQuotes { get; }
    IReadOnlyList<SqlQuotePair> StringQuotes { get; }
    bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair);
    bool TryGetStringQuote(char begin, out SqlQuotePair pair);

    // String quoting
    bool IsStringQuote(char ch);
    SqlStringEscapeStyle StringEscapeStyle { get; }
    bool SupportsDollarQuotedStrings { get; }

    // Parameters
    bool IsParameterPrefix(char ch);

    // Keywords
    bool IsKeyword(string text);

    // Operators (must be ordered by length desc to support greedy match)
    IReadOnlyList<string> Operators { get; }

    // Comments
    bool SupportsHashLineComment { get; }

    // Capabilities
    bool SupportsLimitOffset { get; }
    bool SupportsFetchFirst { get; }
    bool SupportsTop { get; }
    bool SupportsOnDuplicateKeyUpdate { get; }
    bool SupportsOnConflictClause { get; }
    bool SupportsReturning { get; }
    bool SupportsMerge { get; }

    // Pagination
    bool SupportsOffsetFetch { get; }
    bool RequiresOrderByForOffsetFetch { get; }

    // DML variations
    bool SupportsDeleteWithoutFrom { get; }
    bool SupportsDeleteTargetAlias { get; }


    // CTE (WITH ...)
    bool SupportsWithCte { get; }
    bool SupportsWithRecursive { get; }
    bool SupportsWithMaterializedHint { get; }
    // Features
    bool SupportsNullSafeEq { get; }
    bool SupportsJsonArrowOperators { get; }

    // Parser-only compatibility toggles (keep runtime rules separated)
    bool AllowsParserCrossDialectQuotedIdentifiers { get; }
    bool AllowsParserCrossDialectJsonOperators { get; }
    bool AllowsParserInsertSelectUpsertSuffix { get; }
    bool AllowsParserDeleteWithoutFromCompatibility { get; }
    bool AllowsParserLimitOffsetCompatibility { get; }

    // Table hints
    bool SupportsSqlServerTableHints { get; }
    bool SupportsMySqlIndexHints { get; }

    // Temporary table naming
    bool AllowsHashIdentifiers { get; }
    TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName);

    // Operator mapping
    bool TryMapBinaryOperator(string token, out SqlBinaryOp op);

    // Comparison semantics
    StringComparison TextComparison { get; }
    bool SupportsImplicitNumericStringComparison { get; }
    bool LikeIsCaseInsensitive { get; }
}

internal abstract class SqlDialectBase : ISqlDialect
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    protected SqlDialectBase(
        string name,
        int version,
        IEnumerable<string> keywords,
        IEnumerable<KeyValuePair<string, SqlBinaryOp>> binOps,
        IEnumerable<string> operators)
    {
        Name = name;
        Version = version;
        _keywords = new HashSet<string>(keywords, StringComparer.OrdinalIgnoreCase);
        _binOps = new Dictionary<string, SqlBinaryOp>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in binOps)
            _binOps[kv.Key] = kv.Value;

        Operators = [.. operators
            .Concat(["*", "/", "+", "-"])
            .Concat(binOps.Select(kv => kv.Key))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(s => s.Length)
            .ThenBy(s => s, StringComparer.Ordinal)];
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsBacktickIdentifiers => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsDoubleQuoteIdentifiers => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsBracketIdentifiers => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> IdentifierQuotes
    {
        get
        {
            // Double quote can be either string or identifier depending on dialect.
            bool dqIsString = IsStringQuote('"');
            var list = new List<SqlQuotePair>(3);
            if (AllowsBacktickIdentifiers) list.Add(new SqlQuotePair('`', '`'));
            if (AllowsBracketIdentifiers) list.Add(new SqlQuotePair('[', ']'));
            if (AllowsDoubleQuoteIdentifiers && !dqIsString) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> StringQuotes
    {
        get
        {
            var list = new List<SqlQuotePair>(2);
            if (IsStringQuote('\'')) list.Add(new SqlQuotePair('\'', '\''));
            if (IsStringQuote('"')) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in IdentifierQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool TryGetStringQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in StringQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool IsStringQuote(char ch) => ch == '\'';

    /// <summary>
    /// EN: String comparison mode used by textual operators (=, &lt;&gt;, ORDER BY fallback, etc.).
    /// PT: Modo de comparação textual usado por operadores textuais (=, &lt;&gt;, ORDER BY fallback, etc.).
    /// </summary>
    public virtual StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Enables controlled implicit cast between numeric and numeric-string values in comparisons.
    /// PT: Habilita cast implícito controlado entre números e strings numéricas em comparações.
    /// </summary>
    public virtual bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Controls LIKE case sensitivity in the mock when no explicit collation is available.
    /// PT: Controla sensibilidade de maiúsculas/minúsculas no LIKE do mock quando não há collation explícita.
    /// </summary>
    public virtual bool LikeIsCaseInsensitive => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsDollarQuotedStrings => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool IsParameterPrefix(char ch) => ch is '@' or ':' or '?';

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool IsKeyword(string text)
        => SqlKeywords.IsKeyword(text) || _keywords.Contains(text);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public IReadOnlyList<string> Operators { get; }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsHashLineComment => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsLimitOffset => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsFetchFirst => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsTop => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsOnDuplicateKeyUpdate => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsOnConflictClause => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsReturning => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsMerge => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsOffsetFetch => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool RequiresOrderByForOffsetFetch => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsDeleteWithoutFrom => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsDeleteTargetAlias => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsWithCte => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsWithRecursive => true;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsWithMaterializedHint => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsNullSafeEq => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsJsonArrowOperators => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsParserCrossDialectQuotedIdentifiers => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsParserCrossDialectJsonOperators => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsParserInsertSelectUpsertSuffix => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsParserDeleteWithoutFromCompatibility => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsParserLimitOffsetCompatibility => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsSqlServerTableHints => false;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool SupportsMySqlIndexHints => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual bool AllowsHashIdentifiers => false;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public virtual TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        _ = tableName;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public bool TryMapBinaryOperator(string token, out SqlBinaryOp op)
        => _binOps.TryGetValue(token, out op);
}
