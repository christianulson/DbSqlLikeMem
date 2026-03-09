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
    Ilike = 1 << 16
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
        | AutoSqlSyntaxFeatures.Ilike;

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

            if (token.Kind is SqlTokenKind.Keyword or SqlTokenKind.Identifier)
            {
                if (token.Kind == SqlTokenKind.Identifier && IsQuotedIdentifier(sql, token))
                    continue;

                if (token.Text.Equals("TOP", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Top;
                }
                else if (token.Text.Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Limit;
                }
                else if (token.Text.Equals("FETCH", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Fetch;
                }
                else if (token.Text.Equals("OFFSET", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Offset;
                }
                else if (token.Text.Equals("ROWNUM", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Rownum;
                }
                else if (IsIdentityMarker(token))
                {
                    features |= AutoSqlSyntaxFeatures.Identity;
                }
                else if (IsConcatFunction(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.Concat;
                }
                else if (IsSequenceMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.Sequence;
                }
                else if (IsJsonFunctionMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.JsonFunction;
                }
                else if (IsTemporalMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.Temporal;
                }
                else if (IsDateAddMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.DateAdd;
                }
                else if (IsStringAggregateMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.StringAggregate;
                }
                else if (IsRowCountMarker(tokens, i))
                {
                    features |= AutoSqlSyntaxFeatures.RowCount;
                }
                else if (token.Text.Equals("SQL_CALC_FOUND_ROWS", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.SqlCalcFoundRows;
                }
                else if (token.Text.Equals("ILIKE", StringComparison.OrdinalIgnoreCase))
                {
                    features |= AutoSqlSyntaxFeatures.Ilike;
                }
            }
            else if (token.Kind == SqlTokenKind.Operator && token.Text == "<=>")
            {
                features |= AutoSqlSyntaxFeatures.NullSafeEq;
            }
            else if (IsConcatOperator(tokens, i))
            {
                features |= AutoSqlSyntaxFeatures.Concat;
            }
            else if (IsJsonArrowOperator(token))
            {
                features |= AutoSqlSyntaxFeatures.JsonArrow;
            }

            if (features == AllKnownFeatures)
                break;
        }

        return features;
    }

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
            && !token.Text.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase))
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

    private static bool IsQuotedIdentifier(string? sql, SqlToken token)
    {
        if (string.IsNullOrEmpty(sql)
            || token.Position < 0
            || token.Position >= sql.Length)
            return false;

        var first = sql[token.Position];
        return first is '`' or '[' or '"';
    }
}
