namespace DbSqlLikeMem;

internal static class SqlStatementSplitter
{
    internal static List<string> SplitStatementsTopLevel(string sql, ISqlDialect dialect)
    {
        var res = new List<string>();
        if (string.IsNullOrWhiteSpace(sql))
            return res;

        var options = new StatementSplitOptions(
            dialect.SupportsDollarQuotedStrings,
            dialect.StringEscapeStyle,
            dialect.IsStringQuote('"'),
            dialect.AllowsDoubleQuoteIdentifiers,
            dialect.AllowsBacktickIdentifiers,
            dialect.AllowsBracketIdentifiers);
        var start = 0;
        var state = new StatementSplitState();

        for (int i = 0; i < sql.Length; i++)
        {
            if (TryConsumeStatementQuotedChar(sql, options, ref i, ref state))
                continue;

            if (TryBeginStatementQuotedChar(sql, options, ref i, ref state))
                continue;

            var ch = sql[i];
            if (ch == '(')
            {
                state.Depth++;
                continue;
            }

            if (ch == ')')
            {
                state.Depth = Math.Max(0, state.Depth - 1);
                continue;
            }

            if (ch == ';' && state.Depth == 0)
            {
                AddTopLevelStatement(res, sql, start, i);
                start = i + 1;
            }
        }

        AddTopLevelStatement(res, sql, start, sql.Length);
        return res;
    }

    private static void AddTopLevelStatement(List<string> statements, string sql, int start, int endExclusive)
    {
        var stmt = sql[start..endExclusive].Trim();
        if (stmt.Length > 0)
            statements.Add(stmt);
    }

    private static bool TryConsumeStatementQuotedChar(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        if (state.DollarTag is not null)
        {
            if (MatchesDollarTag(sql, index, state.DollarTag))
            {
                index += state.DollarTag.Length - 1;
                state.DollarTag = null;
            }

            return true;
        }

        var ch = sql[index];
        if (state.InSingle)
            return ConsumeStringQuotedChar(sql, options.EscapeStyle, '\'', ref index, ref state.InSingle);

        if (state.InStringDouble)
            return ConsumeStringQuotedChar(sql, options.EscapeStyle, '"', ref index, ref state.InStringDouble);

        if (state.InIdentDouble)
            return ConsumeIdentifierQuotedChar(sql, '"', ref index, ref state.InIdentDouble);

        if (state.InBacktick)
            return ConsumeIdentifierQuotedChar(sql, '`', ref index, ref state.InBacktick);

        if (!state.InBracket)
            return false;

        if (ch == ']')
        {
            if (index + 1 < sql.Length && sql[index + 1] == ']')
                index++;
            else
                state.InBracket = false;
        }

        return true;
    }

    private static bool TryBeginStatementQuotedChar(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        var ch = sql[index];
        if (TryBeginDollarQuotedString(sql, options, ref index, ref state))
            return true;

        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DoubleQuoteIsString)
                state.InStringDouble = true;
            else if (options.AllowDoubleQuoteIdentifiers)
                state.InIdentDouble = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktickIdentifiers)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == '[' && options.AllowBracketIdentifiers)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool TryBeginDollarQuotedString(
        string sql,
        StatementSplitOptions options,
        ref int index,
        ref StatementSplitState state)
    {
        if (!options.SupportsDollarQuotedStrings || sql[index] != '$')
            return false;

        var closingTagIndex = index + 1;
        while (closingTagIndex < sql.Length
            && (char.IsLetterOrDigit(sql[closingTagIndex]) || sql[closingTagIndex] == '_'))
        {
            closingTagIndex++;
        }

        if (closingTagIndex >= sql.Length || sql[closingTagIndex] != '$')
            return false;

        state.DollarTag = sql[index..(closingTagIndex + 1)];
        index = closingTagIndex;
        return true;
    }

    private static bool MatchesDollarTag(string sql, int index, string dollarTag)
        => index + dollarTag.Length <= sql.Length
           && sql.AsSpan(index, dollarTag.Length).SequenceEqual(dollarTag);

    private static bool ConsumeStringQuotedChar(
        string sql,
        SqlStringEscapeStyle escapeStyle,
        char quote,
        ref int index,
        ref bool inQuote)
    {
        var ch = sql[index];
        if (escapeStyle == SqlStringEscapeStyle.doubled_quote
            && ch == quote
            && index + 1 < sql.Length
            && sql[index + 1] == quote)
        {
            index++;
            return true;
        }

        if (escapeStyle == SqlStringEscapeStyle.backslash
            && ch == quote
            && index > 0
            && sql[index - 1] == '\\')
        {
            return true;
        }

        if (ch == quote)
            inQuote = false;

        return true;
    }

    private static bool ConsumeIdentifierQuotedChar(
        string sql,
        char quote,
        ref int index,
        ref bool inQuote)
    {
        if (sql[index] == quote && index + 1 < sql.Length && sql[index + 1] == quote)
        {
            index++;
            return true;
        }

        if (sql[index] == quote)
            inQuote = false;

        return true;
    }

    private readonly record struct StatementSplitOptions(
        bool SupportsDollarQuotedStrings,
        SqlStringEscapeStyle EscapeStyle,
        bool DoubleQuoteIsString,
        bool AllowDoubleQuoteIdentifiers,
        bool AllowBacktickIdentifiers,
        bool AllowBracketIdentifiers);

    private struct StatementSplitState
    {
        public int Depth;
        public bool InSingle;
        public bool InStringDouble;
        public bool InIdentDouble;
        public bool InBacktick;
        public bool InBracket;
        public string? DollarTag;
    }
}
