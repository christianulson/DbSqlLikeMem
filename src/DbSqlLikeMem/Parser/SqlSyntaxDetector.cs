namespace DbSqlLikeMem;

[Flags]
internal enum AutoSqlSyntaxFeatures
{
    None = 0,
    Top = 1 << 0,
    Limit = 1 << 1,
    Fetch = 1 << 2,
    Offset = 1 << 3,
    Rownum = 1 << 4,
    Identity = 1 << 5,
    Concat = 1 << 6,
    Sequence = 1 << 7,
    JsonArrow = 1 << 8,
    JsonFunction = 1 << 9,
    Temporal = 1 << 10,
    DateAdd = 1 << 11,
    StringAggregate = 1 << 12,
    RowCount = 1 << 13,
    SqlCalcFoundRows = 1 << 14,
    NullSafeEq = 1 << 15,
    Ilike = 1 << 16,
    MatchAgainst = 1 << 17,
    ConditionalNullFunctions = 1 << 18,
    WindowFunctions = 1 << 19,
    Pivot = 1 << 20,
    WithCte = 1 << 21,
    Returning = 1 << 22,
    OrderByNulls = 1 << 23
}

/// <summary>
/// EN: Detects low-cost syntax markers used by the automatic SQL dialect mode.
/// PT: Detecta marcadores de sintaxe de baixo custo usados pelo modo automatico de dialeto SQL.
/// </summary>
internal static class SqlSyntaxDetector
{
    private const AutoSqlSyntaxFeatures AllKnownFeatures =
        AutoSqlSyntaxFeatures.Top
        | AutoSqlSyntaxFeatures.Limit
        | AutoSqlSyntaxFeatures.Fetch
        | AutoSqlSyntaxFeatures.Offset
        | AutoSqlSyntaxFeatures.Rownum
        | AutoSqlSyntaxFeatures.Identity
        | AutoSqlSyntaxFeatures.Concat
        | AutoSqlSyntaxFeatures.Sequence
        | AutoSqlSyntaxFeatures.JsonArrow
        | AutoSqlSyntaxFeatures.JsonFunction
        | AutoSqlSyntaxFeatures.Temporal
        | AutoSqlSyntaxFeatures.DateAdd
        | AutoSqlSyntaxFeatures.StringAggregate
        | AutoSqlSyntaxFeatures.RowCount
        | AutoSqlSyntaxFeatures.SqlCalcFoundRows
        | AutoSqlSyntaxFeatures.NullSafeEq
        | AutoSqlSyntaxFeatures.Ilike
        | AutoSqlSyntaxFeatures.MatchAgainst
        | AutoSqlSyntaxFeatures.ConditionalNullFunctions
        | AutoSqlSyntaxFeatures.WindowFunctions
        | AutoSqlSyntaxFeatures.Pivot
        | AutoSqlSyntaxFeatures.WithCte
        | AutoSqlSyntaxFeatures.Returning
        | AutoSqlSyntaxFeatures.OrderByNulls;

    /// <summary>
    /// EN: Scans tokenized SQL once and returns the syntax markers found for Auto mode.
    /// PT: Faz uma unica varredura no SQL tokenizado e retorna os marcadores de sintaxe encontrados para o modo Auto.
    /// </summary>
    /// <param name="tokens">EN: Tokenized SQL sequence. PT: Sequencia de tokens do SQL.</param>
    /// <returns>EN: Combined syntax feature flags. PT: Flags combinadas de recursos de sintaxe.</returns>
    public static AutoSqlSyntaxFeatures Detect(IReadOnlyList<SqlToken> tokens)
        => Detect(null, tokens);

    /// <summary>
    /// EN: Scans tokenized SQL once and returns the syntax markers found for Auto mode, preserving access to the original text when needed to filter quoted identifiers.
    /// PT: Faz uma unica varredura no SQL tokenizado e retorna os marcadores de sintaxe encontrados para o modo Auto, preservando acesso ao texto original quando necessario para filtrar identificadores quoted.
    /// </summary>
    /// <param name="sql">EN: Optional original SQL text. PT: Texto SQL original opcional.</param>
    /// <param name="tokens">EN: Tokenized SQL sequence. PT: Sequencia de tokens do SQL.</param>
    /// <returns>EN: Combined syntax feature flags. PT: Flags combinadas de recursos de sintaxe.</returns>
    public static AutoSqlSyntaxFeatures Detect(string? sql, IReadOnlyList<SqlToken> tokens)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tokens, nameof(tokens));

        var features = AutoSqlSyntaxFeatures.None;
        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            features |= DetectFeature(sql, tokens, i, token);

            if (features == AllKnownFeatures)
                break;
        }

        return features;
    }

    private static AutoSqlSyntaxFeatures DetectFeature(
        string? sql,
        IReadOnlyList<SqlToken> tokens,
        int index,
        SqlToken token)
    {
        if (token.Kind is SqlTokenKind.Keyword or SqlTokenKind.Identifier)
            return DetectWordLikeFeature(sql, tokens, index, token);

        if (token.Kind == SqlTokenKind.Operator && token.Text == "<=>")
            return AutoSqlSyntaxFeatures.NullSafeEq;

        if (IsConcatOperator(tokens, index))
            return AutoSqlSyntaxFeatures.Concat;

        if (IsJsonArrowOperator(token))
            return AutoSqlSyntaxFeatures.JsonArrow;

        return AutoSqlSyntaxFeatures.None;
    }

    private static AutoSqlSyntaxFeatures DetectWordLikeFeature(
        string? sql,
        IReadOnlyList<SqlToken> tokens,
        int index,
        SqlToken token)
    {
        if (token.Kind == SqlTokenKind.Identifier && IsQuotedIdentifier(sql, token))
            return AutoSqlSyntaxFeatures.None;

        if (DirectWordLikeFeatures.TryGetValue(token.Text, out var feature))
            return feature;

        return DetectComputedWordLikeFeature(tokens, index, token);
    }

    private static AutoSqlSyntaxFeatures DetectComputedWordLikeFeature(
        IReadOnlyList<SqlToken> tokens,
        int index,
        SqlToken token)
    {
        foreach (var rule in ComputedWordLikeRules)
        {
            if (rule.IsMatch(tokens, index, token))
                return rule.Feature;
        }

        return AutoSqlSyntaxFeatures.None;
    }

    private static readonly Dictionary<string, AutoSqlSyntaxFeatures> DirectWordLikeFeatures =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["TOP"] = AutoSqlSyntaxFeatures.Top,
            ["LIMIT"] = AutoSqlSyntaxFeatures.Limit,
            ["FETCH"] = AutoSqlSyntaxFeatures.Fetch,
            ["OFFSET"] = AutoSqlSyntaxFeatures.Offset,
            ["ROWNUM"] = AutoSqlSyntaxFeatures.Rownum,
            ["SQL_CALC_FOUND_ROWS"] = AutoSqlSyntaxFeatures.SqlCalcFoundRows,
            ["ILIKE"] = AutoSqlSyntaxFeatures.Ilike,
            ["PIVOT"] = AutoSqlSyntaxFeatures.Pivot,
            ["UNPIVOT"] = AutoSqlSyntaxFeatures.Pivot,
            ["WITH"] = AutoSqlSyntaxFeatures.WithCte,
            ["RETURNING"] = AutoSqlSyntaxFeatures.Returning
        };

    private static readonly ComputedWordLikeRule[] ComputedWordLikeRules =
    [
        new(AutoSqlSyntaxFeatures.Identity, static (_, _, token) => IsIdentityMarker(token)),
        new(AutoSqlSyntaxFeatures.Concat, static (tokens, index, _) => IsConcatFunction(tokens, index)),
        new(AutoSqlSyntaxFeatures.Sequence, static (tokens, index, _) => IsSequenceMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.JsonFunction, static (tokens, index, _) => IsJsonFunctionMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.Temporal, static (tokens, index, _) => IsTemporalMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.DateAdd, static (tokens, index, _) => IsDateAddMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.StringAggregate, static (tokens, index, _) => IsStringAggregateMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.RowCount, static (tokens, index, _) => IsRowCountMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.MatchAgainst, static (tokens, index, _) => IsMatchAgainstMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.ConditionalNullFunctions, static (tokens, index, _) => IsConditionalNullFunctionMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.WindowFunctions, static (tokens, index, _) => IsWindowFunctionMarker(tokens, index)),
        new(AutoSqlSyntaxFeatures.OrderByNulls, static (tokens, index, _) => IsOrderByNullsMarker(tokens, index))
    ];

    private readonly record struct ComputedWordLikeRule(
        AutoSqlSyntaxFeatures Feature,
        Func<IReadOnlyList<SqlToken>, int, SqlToken, bool> IsMatch);

    private static bool IsIdentityMarker(SqlToken token)
        => token.Text.Equals("IDENTITY", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("AUTO_INCREMENT", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SERIAL", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("BIGSERIAL", StringComparison.OrdinalIgnoreCase);

    private static bool IsConcatFunction(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("CONCAT", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsConcatOperator(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (tokens[index].Kind != SqlTokenKind.Symbol || tokens[index].Text != "|")
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "|";
    }

    private static bool IsSequenceMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (token.Text.Equals("SEQUENCE", StringComparison.OrdinalIgnoreCase))
            return true;

        if (token.Text.Equals("NEXT", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("PREVIOUS", StringComparison.OrdinalIgnoreCase))
        {
            var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
            var next2 = index + 2 < tokens.Count ? tokens[index + 2] : SqlToken.EOF;
            return next.Text.Equals("VALUE", StringComparison.OrdinalIgnoreCase)
                && next2.Text.Equals("FOR", StringComparison.OrdinalIgnoreCase);
        }

        if (token.Text.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase))
        {
            var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
            var previous = index > 0 ? tokens[index - 1] : SqlToken.EOF;
            return (next.Kind == SqlTokenKind.Symbol && next.Text == "(")
                || (previous.Kind == SqlTokenKind.Symbol && previous.Text == ".");
        }

        if (token.Text.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SETVAL", StringComparison.OrdinalIgnoreCase))
        {
            var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
            return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
        }

        return false;
    }

    private static bool IsJsonArrowOperator(SqlToken token)
        => token.Kind == SqlTokenKind.Operator
            && (token.Text == "->"
                || token.Text == "->>"
                || token.Text == "#>"
                || token.Text == "#>>");

    private static bool IsJsonFunctionMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("OPENJSON", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsTemporalMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (token.Text.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SYSTEMDATE", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SYSDATE", StringComparison.OrdinalIgnoreCase))
            return true;

        if (token.Text.Equals("NOW", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("GETDATE", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SYSDATETIME", StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals("SYSTIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
            return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
        }

        return false;
    }

    private static bool IsDateAddMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsStringAggregateMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("GROUP_CONCAT", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("LISTAGG", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsRowCountMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (token.Text.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase))
            return true;

        if (!token.Text.Equals("FOUND_ROWS", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("ROW_COUNT", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("CHANGES", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("ROWCOUNT", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsMatchAgainstMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("MATCH", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsConditionalNullFunctionMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("IF", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("IIF", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("IFNULL", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("ISNULL", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("NVL", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("COALESCE", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsWindowFunctionMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("NTILE", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("LEAD", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            && !token.Text.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Kind == SqlTokenKind.Symbol && next.Text == "(";
    }

    private static bool IsOrderByNullsMarker(IReadOnlyList<SqlToken> tokens, int index)
    {
        var token = tokens[index];
        if (!token.Text.Equals("NULLS", StringComparison.OrdinalIgnoreCase))
            return false;

        var next = index + 1 < tokens.Count ? tokens[index + 1] : SqlToken.EOF;
        return next.Text.Equals("FIRST", StringComparison.OrdinalIgnoreCase)
            || next.Text.Equals("LAST", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsQuotedIdentifier(string? sql, SqlToken token)
    {
        if (string.IsNullOrEmpty(sql)
            || token.Position < 0
            || token.Position >= sql!.Length)
            return false;

        var first = sql[token.Position];
        return first is '`' or '[' or '"';
    }
}
