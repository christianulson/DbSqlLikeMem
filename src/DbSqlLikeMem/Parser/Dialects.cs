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
    bool SupportsReturning { get; }
    bool SupportsMerge { get; }

    // Pagination
    bool SupportsOffsetFetch { get; }

    // DML variations
    bool SupportsDeleteWithoutFrom { get; }
    bool SupportsDeleteTargetAlias { get; }


    // CTE (WITH ...)
    bool SupportsWithCte { get; }
    // Features
    bool SupportsNullSafeEq { get; }
    bool SupportsJsonArrowOperators { get; }

    // Operator mapping
    bool TryMapBinaryOperator(string token, out SqlBinaryOp op);
}

internal abstract class SqlDialectBase : ISqlDialect
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;

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

    public string Name { get; }
    public int Version { get; }

    public virtual bool AllowsBacktickIdentifiers => false;
    public virtual bool AllowsDoubleQuoteIdentifiers => true;
    public virtual bool AllowsBracketIdentifiers => false;
    public virtual SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

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

    public virtual bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in IdentifierQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    public virtual bool TryGetStringQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in StringQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    public virtual bool IsStringQuote(char ch) => ch == '\'';
    public virtual SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    public virtual bool SupportsDollarQuotedStrings => false;

    public virtual bool IsParameterPrefix(char ch) => ch is '@' or ':' or '?';

    public virtual bool IsKeyword(string text)
        => SqlKeywords.IsKeyword(text) || _keywords.Contains(text);

    public IReadOnlyList<string> Operators { get; }

    public virtual bool SupportsHashLineComment => false;
    public virtual bool SupportsLimitOffset => false;
    public virtual bool SupportsFetchFirst => false;
    public virtual bool SupportsTop => false;
    public virtual bool SupportsOnDuplicateKeyUpdate => false;
    public virtual bool SupportsReturning => false;
    public virtual bool SupportsMerge => false;
    public virtual bool SupportsOffsetFetch => false;
    public virtual bool SupportsDeleteWithoutFrom => false;
    public virtual bool SupportsDeleteTargetAlias => false;
    public virtual bool SupportsWithCte => false;
    public virtual bool SupportsNullSafeEq => false;
    public virtual bool SupportsJsonArrowOperators => false;

    public bool TryMapBinaryOperator(string token, out SqlBinaryOp op)
        => _binOps.TryGetValue(token, out op);
}
