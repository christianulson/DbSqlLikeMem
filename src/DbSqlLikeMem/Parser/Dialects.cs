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
    /// EN: Gets or sets Version.
    /// PT: Obtém ou define Version.
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
    bool SupportsTriggers { get; }

    // Pagination
    bool SupportsOffsetFetch { get; }
    bool RequiresOrderByForOffsetFetch { get; }
    bool SupportsOrderByNullsModifier { get; }

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
    bool SupportsJsonExtractFunction { get; }
    bool SupportsJsonValueFunction { get; }
    bool SupportsOpenJsonFunction { get; }

    // Parser-only compatibility toggles (keep runtime rules separated)
    bool AllowsParserCrossDialectQuotedIdentifiers { get; }
    bool AllowsParserCrossDialectJsonOperators { get; }
    bool AllowsParserInsertSelectUpsertSuffix { get; }
    bool AllowsParserDeleteWithoutFromCompatibility { get; }
    bool AllowsParserLimitOffsetCompatibility { get; }

    // Table hints
    bool SupportsSqlServerTableHints { get; }
    bool SupportsSqlServerQueryHints { get; }
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
    bool SupportsIfFunction { get; }
    bool SupportsIifFunction { get; }
    IReadOnlyCollection<string> NullSubstituteFunctionNames { get; }
    bool ConcatReturnsNullOnNullInput { get; }
    // Dialect-specific runtime semantics
    bool RegexInvalidPatternEvaluatesToFalse { get; }
    bool AreUnionColumnTypesCompatible(DbType first, DbType second);
    bool IsIntegerCastTypeName(string typeName);
    bool SupportsDateAddFunction(string functionName);
    bool SupportsWindowFunctions { get; }
    bool SupportsWindowFrameClause { get; }
    bool SupportsLikeEscapeClause { get; }
    bool IsRowNumberWindowFunction(string functionName);
    bool SupportsWindowFunction(string functionName);
    bool RequiresOrderByInWindowFunction(string functionName);
    bool TryGetWindowFunctionArgumentArity(string functionName, out int minArgs, out int maxArgs);
    bool SupportsPivotClause { get; }
    DbType InferWindowFunctionDbType(WindowFunctionExpr windowFunctionExpr, Func<SqlExpr, DbType> inferArgDbType);
}

internal abstract class SqlDialectBase : ISqlDialect
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;

    /// <summary>
    /// EN: Implements SqlDialectBase.
    /// PT: Implementa SqlDialectBase.
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
    /// EN: Gets or sets Name.
    /// PT: Obtém ou define Name.
    /// </summary>
    public string Name { get; }
    /// <summary>
    /// EN: Gets or sets Version.
    /// PT: Obtém ou define Version.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// EN: Gets or sets AllowsBacktickIdentifiers.
    /// PT: Obtém ou define AllowsBacktickIdentifiers.
    /// </summary>
    public virtual bool AllowsBacktickIdentifiers => false;
    /// <summary>
    /// EN: Gets or sets AllowsDoubleQuoteIdentifiers.
    /// PT: Obtém ou define AllowsDoubleQuoteIdentifiers.
    /// </summary>
    public virtual bool AllowsDoubleQuoteIdentifiers => true;
    /// <summary>
    /// EN: Gets or sets AllowsBracketIdentifiers.
    /// PT: Obtém ou define AllowsBracketIdentifiers.
    /// </summary>
    public virtual bool AllowsBracketIdentifiers => false;
    /// <summary>
    /// EN: Gets or sets IdentifierEscapeStyle.
    /// PT: Obtém ou define IdentifierEscapeStyle.
    /// </summary>
    public virtual SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Implements this member.
    /// PT: Implementa este membro.
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
    /// EN: Implements this member.
    /// PT: Implementa este membro.
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
    /// EN: Implements TryGetIdentifierQuote.
    /// PT: Implementa TryGetIdentifierQuote.
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
    /// EN: Implements TryGetStringQuote.
    /// PT: Implementa TryGetStringQuote.
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
    /// EN: Implements IsStringQuote.
    /// PT: Implementa IsStringQuote.
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
    /// PT: Controla sensibilidade de maiúsculas/minúsculas no LIKE do simulado quando não há collation explícita.
    /// </summary>
    public virtual bool LikeIsCaseInsensitive => true;
    public virtual bool SupportsIfFunction => true;
    public virtual bool SupportsIifFunction => true;
    public virtual bool SupportsWindowFunctions => true;
    public virtual bool SupportsWindowFrameClause => false;
    public virtual bool SupportsPivotClause => false;
    public virtual IReadOnlyCollection<string> NullSubstituteFunctionNames
        => ["IFNULL", "ISNULL", "NVL"];
    public virtual bool ConcatReturnsNullOnNullInput => true;
    public virtual bool RegexInvalidPatternEvaluatesToFalse => false;

    public virtual bool AreUnionColumnTypesCompatible(DbType first, DbType second)
    {
        if (first == second)
            return true;

        static bool IsNumeric(DbType t)
            => t is DbType.Byte or DbType.SByte
            or DbType.Int16 or DbType.UInt16
            or DbType.Int32 or DbType.UInt32
            or DbType.Int64 or DbType.UInt64
            or DbType.Decimal or DbType.Double
            or DbType.Single or DbType.VarNumeric;

        static bool IsText(DbType t)
            => t is DbType.AnsiString or DbType.String
            or DbType.AnsiStringFixedLength or DbType.StringFixedLength;

        if (IsNumeric(first) && IsNumeric(second))
            return true;

        if (IsText(first) && IsText(second))
            return true;

        return false;
    }

    public virtual bool IsIntegerCastTypeName(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        return typeName.Equals("SIGNED", StringComparison.OrdinalIgnoreCase)
            || typeName.Equals("UNSIGNED", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("INT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("INTEGER", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("BIGINT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("SMALLINT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("TINYINT", StringComparison.OrdinalIgnoreCase);
    }

    public virtual bool SupportsDateAddFunction(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase);
    }

    public virtual bool SupportsLikeEscapeClause => true;

    public virtual bool IsRowNumberWindowFunction(string functionName)
        => functionName.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Indicates whether a specific window function name is supported by the current dialect/version.
    /// PT: Indica se um nome específico de função de janela é suportado pelo dialeto/versão atual.
    /// </summary>
    public virtual bool SupportsWindowFunction(string functionName)
    {
        if (!SupportsWindowFunctions || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (IsRowNumberWindowFunction(functionName))
            return true;

        return functionName.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Indicates whether a specific window function requires ORDER BY inside OVER clause.
    /// PT: Indica se uma função de janela específica exige ORDER BY dentro da cláusula OVER.
    /// </summary>
    public virtual bool RequiresOrderByInWindowFunction(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return IsRowNumberWindowFunction(functionName)
            || functionName.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Gets accepted argument arity range for a supported window function.
    /// PT: Obtém o intervalo de aridade aceito para uma função de janela suportada.
    /// </summary>
    public virtual bool TryGetWindowFunctionArgumentArity(string functionName, out int minArgs, out int maxArgs)
    {
        minArgs = 0;
        maxArgs = 0;

        if (!SupportsWindowFunction(functionName))
            return false;

        if (IsRowNumberWindowFunction(functionName)
            || functionName.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase))
        {
            minArgs = 0;
            maxArgs = 0;
            return true;
        }

        if (functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            minArgs = 1;
            maxArgs = 1;
            return true;
        }

        if (functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase))
        {
            minArgs = 1;
            maxArgs = 3;
            return true;
        }

        if (functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            minArgs = 2;
            maxArgs = 2;
            return true;
        }

        return false;
    }

    public virtual DbType InferWindowFunctionDbType(
        WindowFunctionExpr windowFunctionExpr,
        Func<SqlExpr, DbType> inferArgDbType)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunctionExpr, nameof(windowFunctionExpr));
        ArgumentNullExceptionCompatible.ThrowIfNull(inferArgDbType, nameof(inferArgDbType));

        if (IsRowNumberWindowFunction(windowFunctionExpr.Name)
            || windowFunctionExpr.Name.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("NTILE", StringComparison.OrdinalIgnoreCase))
            return DbType.Int64;

        if (windowFunctionExpr.Name.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase))
            return DbType.Double;

        if (windowFunctionExpr.Name.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("LEAD", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            if (windowFunctionExpr.Args.Count > 0)
                return inferArgDbType(windowFunctionExpr.Args[0]);
        }

        return DbType.Object;
    }
    /// <summary>
    /// EN: Gets or sets StringEscapeStyle.
    /// PT: Obtém ou define StringEscapeStyle.
    /// </summary>
    public virtual SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    /// <summary>
    /// EN: Gets or sets SupportsDollarQuotedStrings.
    /// PT: Obtém ou define SupportsDollarQuotedStrings.
    /// </summary>
    public virtual bool SupportsDollarQuotedStrings => false;

    /// <summary>
    /// EN: Implements IsParameterPrefix.
    /// PT: Implementa IsParameterPrefix.
    /// </summary>
    public virtual bool IsParameterPrefix(char ch) => ch is '@' or ':' or '?';

    /// <summary>
    /// EN: Implements IsKeyword.
    /// PT: Implementa IsKeyword.
    /// </summary>
    public virtual bool IsKeyword(string text)
        => SqlKeywords.IsKeyword(text) || _keywords.Contains(text);

    /// <summary>
    /// EN: Gets or sets Operators.
    /// PT: Obtém ou define Operators.
    /// </summary>
    public IReadOnlyList<string> Operators { get; }

    /// <summary>
    /// EN: Gets or sets SupportsHashLineComment.
    /// PT: Obtém ou define SupportsHashLineComment.
    /// </summary>
    public virtual bool SupportsHashLineComment => false;
    /// <summary>
    /// EN: Gets or sets SupportsLimitOffset.
    /// PT: Obtém ou define SupportsLimitOffset.
    /// </summary>
    public virtual bool SupportsLimitOffset => false;
    /// <summary>
    /// EN: Gets or sets SupportsFetchFirst.
    /// PT: Obtém ou define SupportsFetchFirst.
    /// </summary>
    public virtual bool SupportsFetchFirst => false;
    /// <summary>
    /// EN: Gets or sets SupportsTop.
    /// PT: Obtém ou define SupportsTop.
    /// </summary>
    public virtual bool SupportsTop => false;
    /// <summary>
    /// EN: Gets or sets SupportsOnDuplicateKeyUpdate.
    /// PT: Obtém ou define SupportsOnDuplicateKeyUpdate.
    /// </summary>
    public virtual bool SupportsOnDuplicateKeyUpdate => false;
    /// <summary>
    /// EN: Gets or sets SupportsOnConflictClause.
    /// PT: Obtém ou define SupportsOnConflictClause.
    /// </summary>
    public virtual bool SupportsOnConflictClause => false;
    /// <summary>
    /// EN: Gets or sets SupportsReturning.
    /// PT: Obtém ou define SupportsReturning.
    /// </summary>
    public virtual bool SupportsReturning => false;
    /// <summary>
    /// EN: Gets or sets SupportsMerge.
    /// PT: Obtém ou define SupportsMerge.
    /// </summary>
    public virtual bool SupportsMerge => false;
    public virtual bool SupportsTriggers => true;
    /// <summary>
    /// EN: Gets or sets SupportsOffsetFetch.
    /// PT: Obtém ou define SupportsOffsetFetch.
    /// </summary>
    public virtual bool SupportsOffsetFetch => false;
    /// <summary>
    /// EN: Gets or sets RequiresOrderByForOffsetFetch.
    /// PT: Obtém ou define RequiresOrderByForOffsetFetch.
    /// </summary>
    public virtual bool RequiresOrderByForOffsetFetch => false;

    public virtual bool SupportsOrderByNullsModifier => false;
    /// <summary>
    /// EN: Gets or sets SupportsDeleteWithoutFrom.
    /// PT: Obtém ou define SupportsDeleteWithoutFrom.
    /// </summary>
    public virtual bool SupportsDeleteWithoutFrom => false;
    /// <summary>
    /// EN: Gets or sets SupportsDeleteTargetAlias.
    /// PT: Obtém ou define SupportsDeleteTargetAlias.
    /// </summary>
    public virtual bool SupportsDeleteTargetAlias => true;
    /// <summary>
    /// EN: Gets or sets SupportsWithCte.
    /// PT: Obtém ou define SupportsWithCte.
    /// </summary>
    public virtual bool SupportsWithCte => false;
    /// <summary>
    /// EN: Gets or sets SupportsWithRecursive.
    /// PT: Obtém ou define SupportsWithRecursive.
    /// </summary>
    public virtual bool SupportsWithRecursive => true;
    /// <summary>
    /// EN: Gets or sets SupportsWithMaterializedHint.
    /// PT: Obtém ou define SupportsWithMaterializedHint.
    /// </summary>
    public virtual bool SupportsWithMaterializedHint => false;
    /// <summary>
    /// EN: Gets or sets SupportsNullSafeEq.
    /// PT: Obtém ou define SupportsNullSafeEq.
    /// </summary>
    public virtual bool SupportsNullSafeEq => false;
    /// <summary>
    /// EN: Gets or sets SupportsJsonArrowOperators.
    /// PT: Obtém ou define SupportsJsonArrowOperators.
    /// </summary>
    public virtual bool SupportsJsonArrowOperators => false;
    public virtual bool SupportsJsonExtractFunction => false;
    public virtual bool SupportsJsonValueFunction => false;
    public virtual bool SupportsOpenJsonFunction => false;
    /// <summary>
    /// EN: Gets or sets AllowsParserCrossDialectQuotedIdentifiers.
    /// PT: Obtém ou define AllowsParserCrossDialectQuotedIdentifiers.
    /// </summary>
    public virtual bool AllowsParserCrossDialectQuotedIdentifiers => false;
    /// <summary>
    /// EN: Gets or sets AllowsParserCrossDialectJsonOperators.
    /// PT: Obtém ou define AllowsParserCrossDialectJsonOperators.
    /// </summary>
    public virtual bool AllowsParserCrossDialectJsonOperators => false;
    /// <summary>
    /// EN: Gets or sets AllowsParserInsertSelectUpsertSuffix.
    /// PT: Obtém ou define AllowsParserInsertSelectUpsertSuffix.
    /// </summary>
    public virtual bool AllowsParserInsertSelectUpsertSuffix => false;
    /// <summary>
    /// EN: Gets or sets AllowsParserDeleteWithoutFromCompatibility.
    /// PT: Obtém ou define AllowsParserDeleteWithoutFromCompatibility.
    /// </summary>
    public virtual bool AllowsParserDeleteWithoutFromCompatibility => false;
    /// <summary>
    /// EN: Gets or sets AllowsParserLimitOffsetCompatibility.
    /// PT: Obtém ou define AllowsParserLimitOffsetCompatibility.
    /// </summary>
    public virtual bool AllowsParserLimitOffsetCompatibility => false;
    /// <summary>
    /// EN: Gets or sets SupportsSqlServerTableHints.
    /// PT: Obtém ou define SupportsSqlServerTableHints.
    /// </summary>
    public virtual bool SupportsSqlServerTableHints => false;
    /// <summary>
    /// EN: Gets or sets SupportsSqlServerQueryHints.
    /// PT: Obtém ou define SupportsSqlServerQueryHints.
    /// </summary>
    public virtual bool SupportsSqlServerQueryHints => false;
    /// <summary>
    /// EN: Gets or sets SupportsMySqlIndexHints.
    /// PT: Obtém ou define SupportsMySqlIndexHints.
    /// </summary>
    public virtual bool SupportsMySqlIndexHints => false;

    /// <summary>
    /// EN: Gets or sets AllowsHashIdentifiers.
    /// PT: Obtém ou define AllowsHashIdentifiers.
    /// </summary>
    public virtual bool AllowsHashIdentifiers => false;

    /// <summary>
    /// EN: Implements GetTemporaryTableScope.
    /// PT: Implementa GetTemporaryTableScope.
    /// </summary>
    public virtual TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        _ = tableName;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// EN: Implements TryMapBinaryOperator.
    /// PT: Implementa TryMapBinaryOperator.
    /// </summary>
    public bool TryMapBinaryOperator(string token, out SqlBinaryOp op)
        => _binOps.TryGetValue(token, out op);
}
