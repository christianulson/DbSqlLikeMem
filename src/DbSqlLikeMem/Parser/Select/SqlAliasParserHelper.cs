namespace DbSqlLikeMem;

internal static class SqlAliasParserHelper
{
    internal static (string Expr, string? Alias) SplitTrailingAsAliasTopLevel(string raw, ISqlDialect dialect)
    {
        raw = raw.Trim();
        if (raw.Length == 0)
            return (raw, null);

        var options = new AliasSplitOptions(
            dialect.IsStringQuote('"'),
            dialect.AllowsBacktickIdentifiers,
            dialect.AllowsDoubleQuoteIdentifiers && !dialect.IsStringQuote('"'),
            dialect.AllowsBracketIdentifiers);

        var explicitAlias = TrySplitExplicitAliasTopLevel(raw, dialect, options);
        if (explicitAlias is not null)
            return explicitAlias.Value;

        var implicitAlias = TrySplitImplicitAliasTopLevel(raw, dialect, options);
        if (implicitAlias is not null)
            return implicitAlias.Value;

        return (raw, null);
    }

    private static (string Expr, string? Alias)? TrySplitExplicitAliasTopLevel(
        string raw,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var state = new AliasForwardScanState();
        for (int i = 0; i + 4 <= raw.Length; i++)
        {
            if (TryConsumeAliasForwardQuotedChar(raw, ref i, ref state))
                continue;

            var ch = raw[i];
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

            if (state.Depth != 0)
                continue;

            if (TryBeginAliasForwardQuotedChar(ch, options, ref state))
                continue;

            if (!IsExplicitAliasKeyword(raw, i))
                continue;

            var expr = raw[..i].Trim();
            var aliasRaw = raw[(i + 2)..].Trim();
            if (aliasRaw.Length == 0)
                return null;

            return (expr, NormalizeAlias(aliasRaw, dialect, options));
        }

        return null;
    }

    private static (string Expr, string? Alias)? TrySplitImplicitAliasTopLevel(
        string raw,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var state = new AliasBackwardScanState();
        for (int i = raw.Length - 1; i >= 0; i--)
        {
            if (TryConsumeAliasBackwardQuotedChar(raw[i], ref state))
                continue;

            var ch = raw[i];
            if (ch == ')')
            {
                state.Depth++;
                continue;
            }

            if (ch == '(')
            {
                state.Depth = Math.Max(0, state.Depth - 1);
                continue;
            }

            if (state.Depth != 0)
                continue;

            if (TryBeginAliasBackwardQuotedChar(ch, options, ref state))
                continue;

            if (!char.IsWhiteSpace(ch))
                continue;

            var split = TryCreateImplicitAliasSplit(raw, i, dialect, options);
            if (split is not null)
                return split;
        }

        return null;
    }

    private static (string Expr, string Alias)? TryCreateImplicitAliasSplit(
        string raw,
        int separatorIndex,
        ISqlDialect dialect,
        AliasSplitOptions options)
    {
        var left = raw[..separatorIndex].TrimEnd();
        var right = raw[(separatorIndex + 1)..].TrimStart();
        if (left.Length == 0 || right.Length == 0)
            return null;

        ThrowIfUnsupportedAliasQuote(right, dialect, options);

        var lastLeft = left.TrimEnd();
        if (Regex.IsMatch(lastLeft, @"(<=>|<>|!=|>=|<=|=|>|<|\+|-|\*|/|,)\s*$", RegexOptions.CultureInvariant))
            return null;
        if (Regex.IsMatch(lastLeft, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            return null;

        var compositeTemporalIdentifier = $"{lastLeft} {right}";
        if (dialect.AllowsTemporalIdentifier(compositeTemporalIdentifier))
            return null;

        if (!LooksLikeAliasToken(right, options))
            return null;

        var alias = NormalizeAlias(right, dialect, options);
        if (dialect.IsKeyword(alias))
            return null;

        return (left, alias);
    }

    private static bool IsExplicitAliasKeyword(string raw, int index)
    {
        if (index < 0 || index + 1 >= raw.Length)
            return false;

        if (!raw.AsSpan(index).StartsWith("AS", StringComparison.OrdinalIgnoreCase))
            return false;

        var beforeOk = index == 0 || char.IsWhiteSpace(raw[index - 1]);
        var afterIndex = index + 2;
        var afterOk = afterIndex >= raw.Length || char.IsWhiteSpace(raw[afterIndex]) || raw[afterIndex] is '(' or '[' or '"' or '`';
        return beforeOk && afterOk;
    }

    private static void ThrowIfUnsupportedAliasQuote(string aliasRaw, ISqlDialect dialect, AliasSplitOptions options)
    {
        if ((aliasRaw[0] == '`' && !options.AllowBacktick)
            || (aliasRaw[0] == '[' && !options.AllowBracket)
            || (aliasRaw[0] == '"' && !options.AllowDqIdent && !options.DqIsString))
        {
            throw SqlUnsupported.NotSupported(dialect, $"Identificador/alias quoting: '{aliasRaw[0]}'");
        }
    }

    private static bool TryConsumeAliasForwardQuotedChar(string raw, ref int index, ref AliasForwardScanState state)
    {
        var ch = raw[index];
        if (state.InSingle)
        {
            if (ch == '\'' && index + 1 < raw.Length && raw[index + 1] == '\'')
                index++;
            else if (ch == '\'')
                state.InSingle = false;

            return true;
        }

        if (state.InDoubleString)
        {
            if (ch == '"')
                state.InDoubleString = false;

            return true;
        }

        if (state.InDoubleIdent)
        {
            if (ch == '"')
                state.InDoubleIdent = false;

            return true;
        }

        if (state.InBacktick)
        {
            if (ch == '`')
                state.InBacktick = false;

            return true;
        }

        if (!state.InBracket)
            return false;

        if (ch == ']')
        {
            if (index + 1 < raw.Length && raw[index + 1] == ']')
                index++;
            else
                state.InBracket = false;
        }

        return true;
    }

    private static bool TryBeginAliasForwardQuotedChar(char ch, AliasSplitOptions options, ref AliasForwardScanState state)
    {
        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DqIsString)
                state.InDoubleString = true;
            else if (options.AllowDqIdent)
                state.InDoubleIdent = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktick)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == '[' && options.AllowBracket)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool TryConsumeAliasBackwardQuotedChar(char ch, ref AliasBackwardScanState state)
    {
        if (state.InSingle)
        {
            if (ch == '\'')
                state.InSingle = false;

            return true;
        }

        if (state.InDoubleString)
        {
            if (ch == '"')
                state.InDoubleString = false;

            return true;
        }

        if (state.InDoubleIdent)
        {
            if (ch == '"')
                state.InDoubleIdent = false;

            return true;
        }

        if (state.InBacktick)
        {
            if (ch == '`')
                state.InBacktick = false;

            return true;
        }

        if (!state.InBracket)
            return false;

        if (ch == '[')
            state.InBracket = false;

        return true;
    }

    private static bool TryBeginAliasBackwardQuotedChar(char ch, AliasSplitOptions options, ref AliasBackwardScanState state)
    {
        if (ch == '\'')
        {
            state.InSingle = true;
            return true;
        }

        if (ch == '"')
        {
            if (options.DqIsString)
                state.InDoubleString = true;
            else if (options.AllowDqIdent)
                state.InDoubleIdent = true;

            return true;
        }

        if (ch == '`' && options.AllowBacktick)
        {
            state.InBacktick = true;
            return true;
        }

        if (ch == ']' && options.AllowBracket)
        {
            state.InBracket = true;
            return true;
        }

        return false;
    }

    private static bool LooksLikeAliasToken(string rawRight, AliasSplitOptions options)
    {
        rawRight = rawRight.Trim();
        if (rawRight.Length == 0)
            return false;

        if (rawRight[0] == '`')
            return options.AllowBacktick && rawRight.Length >= 2 && rawRight[^1] == '`';

        if (rawRight[0] == '"')
            return options.AllowDqIdent && rawRight.Length >= 2 && rawRight[^1] == '"';

        if (rawRight[0] == '[')
            return options.AllowBracket && rawRight.Length >= 2 && rawRight[^1] == ']';

        for (var i = 0; i < rawRight.Length; i++)
        {
            if (char.IsWhiteSpace(rawRight[i]))
                return false;
        }

        var first = rawRight[0];
        if (!(char.IsLetter(first) || first == '_'))
            return false;

        for (var i = 1; i < rawRight.Length; i++)
        {
            var ch = rawRight[i];
            if (!(char.IsLetterOrDigit(ch) || ch == '_' || ch == '$'))
                return false;
        }

        return true;
    }

    private static string NormalizeAlias(
        string aliasRaw,
        ISqlDialect dialect,
        bool dqIsString,
        bool allowBacktick,
        bool allowDqIdent,
        bool allowBracket)
    {
        aliasRaw = aliasRaw.Trim();

        if (aliasRaw.StartsWith("`") && !allowBacktick)
            throw SqlUnsupported.NotSupported(dialect, "alias/identificadores com '`'");

        if (aliasRaw.StartsWith("[") && !allowBracket)
            throw SqlUnsupported.NotSupported(dialect, "alias/identificadores com '['");

        if (aliasRaw.StartsWith("\"") && !allowDqIdent && !dqIsString)
            throw SqlUnsupported.NotSupported(dialect, "alias/identificadores com '\"'");

        if (allowBacktick && aliasRaw.Length >= 2 && aliasRaw[0] == '`' && aliasRaw[^1] == '`')
        {
            var inner = aliasRaw[1..^1].Replace("``", "`");
            return inner;
        }

        if (allowDqIdent && aliasRaw.Length >= 2 && aliasRaw[0] == '"' && aliasRaw[^1] == '"')
        {
            var inner = aliasRaw[1..^1].Replace("\"\"", "\"");
            return inner;
        }

        if (allowBracket && aliasRaw.Length >= 2 && aliasRaw[0] == '[' && aliasRaw[^1] == ']')
        {
            var inner = aliasRaw[1..^1].Replace("]]", "]");
            return inner;
        }

        return aliasRaw;
    }

    private static string NormalizeAlias(string aliasRaw, ISqlDialect dialect, AliasSplitOptions options)
        => NormalizeAlias(
            aliasRaw,
            dialect,
            options.DqIsString,
            options.AllowBacktick,
            options.AllowDqIdent,
            options.AllowBracket);

    private readonly record struct AliasSplitOptions(
        bool DqIsString,
        bool AllowBacktick,
        bool AllowDqIdent,
        bool AllowBracket);

    private struct AliasForwardScanState
    {
        public int Depth;
        public bool InSingle;
        public bool InDoubleString;
        public bool InDoubleIdent;
        public bool InBacktick;
        public bool InBracket;
    }

    private struct AliasBackwardScanState
    {
        public int Depth;
        public bool InSingle;
        public bool InDoubleString;
        public bool InDoubleIdent;
        public bool InBacktick;
        public bool InBracket;
    }
}
