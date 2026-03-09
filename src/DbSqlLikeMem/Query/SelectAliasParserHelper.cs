namespace DbSqlLikeMem;

internal static class SelectAliasParserHelper
{
    internal static (string expr, string? alias) SplitTrailingAsAlias(
        string raw,
        string? alreadyAlias)
    {
        raw = raw.Trim();
        if (!string.IsNullOrWhiteSpace(alreadyAlias))
            return (raw, alreadyAlias);

        var depth = GetDepthAlias(raw);
        var asPosition = -1;
        FindPositionOfAs(raw, ref depth, ref asPosition);

        if (asPosition < 0)
        {
            if (TrySplitTrailingImplicitAlias(raw, out var expr, out var alias))
            {
                if (raw.IndexOf("<=>", StringComparison.Ordinal) >= 0
                    || Regex.IsMatch(raw, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
                {
                    Console.WriteLine($"[SPLIT-ALIAS-SUSPECT] Raw='{raw}' Expr='{expr}' Alias='{alias}'");
                }

                return (expr, alias);
            }

            return (raw, null);
        }

        var after = raw[(asPosition + 2)..].Trim();
        if (after.Length == 0)
            return (raw, null);

        var match = Regex.Match(after, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?\s*$");
        if (!match.Success)
            return (raw, null);

        var explicitAlias = match.Groups["a"].Value;
        var beforeAs = raw[..asPosition].TrimEnd();
        if (beforeAs.Length == 0)
            return (raw, null);

        return (beforeAs, explicitAlias);
    }

    private static bool TrySplitTrailingImplicitAlias(
        string raw,
        out string expr,
        out string alias)
    {
        expr = raw;
        alias = string.Empty;

        var depth = 0;
        var i = raw.Length - 1;
        while (i >= 0 && char.IsWhiteSpace(raw[i]))
            i--;

        if (i < 0)
            return false;

        var end = i;
        while (i >= 0)
        {
            var ch = raw[i];
            if (ch == ')')
            {
                depth++;
                i--;
                continue;
            }

            if (ch == '(')
            {
                depth = Math.Max(0, depth - 1);
                i--;
                continue;
            }

            if (depth == 0 && char.IsWhiteSpace(ch))
                break;

            i--;
        }

        var start = i + 1;
        if (start > end)
            return false;

        var token = raw[start..(end + 1)].Trim();
        if (token.Length == 0)
            return false;

        var match = Regex.Match(token, @"^`?(?<a>[A-Za-z_][A-Za-z0-9_]*)`?$", RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        var parsedAlias = match.Groups["a"].Value;
        if (IsLikelyKeyword(parsedAlias))
            return false;

        var before = raw[..start].TrimEnd();
        if (before.Length == 0 || before.EndsWith(".", StringComparison.Ordinal))
            return false;

        if (Regex.IsMatch(before, @"(<=>|<>|!=|>=|<=|=|>|<|\+|-|\*|/|,)\s*$", RegexOptions.CultureInvariant))
            return false;

        if (Regex.IsMatch(before, @"\b(NEXT|PREVIOUS)\s+VALUE\s+FOR\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
            return false;

        expr = before;
        alias = parsedAlias;
        return true;
    }

    private static bool IsLikelyKeyword(string value)
        => value.Equals("FROM", StringComparison.OrdinalIgnoreCase)
            || value.Equals("WHERE", StringComparison.OrdinalIgnoreCase)
            || value.Equals("JOIN", StringComparison.OrdinalIgnoreCase)
            || value.Equals("LEFT", StringComparison.OrdinalIgnoreCase)
            || value.Equals("RIGHT", StringComparison.OrdinalIgnoreCase)
            || value.Equals("INNER", StringComparison.OrdinalIgnoreCase)
            || value.Equals("OUTER", StringComparison.OrdinalIgnoreCase)
            || value.Equals("ON", StringComparison.OrdinalIgnoreCase)
            || value.Equals("GROUP", StringComparison.OrdinalIgnoreCase)
            || value.Equals("BY", StringComparison.OrdinalIgnoreCase)
            || value.Equals("HAVING", StringComparison.OrdinalIgnoreCase)
            || value.Equals("ORDER", StringComparison.OrdinalIgnoreCase)
            || value.Equals("LIMIT", StringComparison.OrdinalIgnoreCase)
            || value.Equals("UNION", StringComparison.OrdinalIgnoreCase)
            || value.Equals("ALL", StringComparison.OrdinalIgnoreCase)
            || value.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase)
            || value.Equals("ASC", StringComparison.OrdinalIgnoreCase)
            || value.Equals("DESC", StringComparison.OrdinalIgnoreCase)
            || value.Equals("NULL", StringComparison.OrdinalIgnoreCase)
            || value.Equals("TRUE", StringComparison.OrdinalIgnoreCase)
            || value.Equals("FALSE", StringComparison.OrdinalIgnoreCase);

    private static void FindPositionOfAs(string raw, ref int depth, ref int asPosition)
    {
        for (var i = 0; i < raw.Length - 1; i++)
        {
            UpdateParenDepth(raw[i], ref depth);
            if (depth != 0 || !IsAsAt(raw, i) || !IsWordBoundary(raw, i, 2))
                continue;

            asPosition = i;
        }
    }

    private static void UpdateParenDepth(char ch, ref int depth)
    {
        if (ch == '(')
        {
            depth++;
            return;
        }

        if (ch == ')')
            depth = Math.Max(0, depth - 1);
    }

    private static bool IsAsAt(string value, int index)
        => (value[index] == 'A' || value[index] == 'a')
            && (value[index + 1] == 'S' || value[index + 1] == 's');

    private static bool IsWordBoundary(string value, int start, int length)
    {
        var left = start - 1;
        var right = start + length;

        var leftOk = left < 0 || !IsIdentifierChar(value[left]);
        var rightOk = right >= value.Length || !IsIdentifierChar(value[right]);
        return leftOk && rightOk;
    }

    private static bool IsIdentifierChar(char c)
        => char.IsLetterOrDigit(c) || c == '_';

    private static int GetDepthAlias(string raw)
    {
        var depth = 0;
        for (var i = raw.Length - 1; i >= 0; i--)
        {
            var ch = raw[i];
            if (ch == ')')
                depth++;
            else if (ch == '(')
                depth = Math.Max(0, depth - 1);

            if (depth != 0)
                continue;
        }

        return depth;
    }
}
