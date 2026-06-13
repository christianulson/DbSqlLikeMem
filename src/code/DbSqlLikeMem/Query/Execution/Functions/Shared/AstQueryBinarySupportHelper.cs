using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class AstQueryBinarySupportHelper
{
    private static readonly ConcurrentDictionary<string, Regex> _regexpCache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, IReadOnlyList<string>> _matchQueryCache = new();
    internal static bool EvalSoundLike(object left, object right)
    {
        var leftSoundex = AstQuerySqlServerResolutionHelper.ComputeSoundex(left.ToString() ?? string.Empty);
        var rightSoundex = AstQuerySqlServerResolutionHelper.ComputeSoundex(right.ToString() ?? string.Empty);
        return leftSoundex == rightSoundex;
    }

    internal static bool EvalRegexp(object left, object right, ISqlDialect dialect)
    {
        try
        {
            return _regexpCache.GetOrAdd(right.ToString() ?? string.Empty, static pattern => new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)).IsMatch(left.ToString() ?? string.Empty);
        }
        catch (ArgumentException)
        {
            if (dialect.RegexInvalidPatternEvaluatesToFalse)
                return false;
            throw;
        }
    }

    internal static bool EvalMatchAgainst(object left, object right)
    {
        var leftStr = left.ToString() ?? string.Empty;
        var rightStr = right.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(leftStr) || string.IsNullOrWhiteSpace(rightStr))
            return false;

        var tokens = _matchQueryCache.GetOrAdd(rightStr, static q => TokenizeMatchQuery(q));
        if (tokens.Count == 0)
            return false;

        var haystackTokens = TokenizeMatchHaystack(leftStr);
        var matched = EvaluateMatchQuery(leftStr, haystackTokens, tokens, 0, tokens.Count - 1);
        if (!matched && ContainsBooleanOrOperator(rightStr))
            matched = EvaluateMatchAnyRelevantTerm(leftStr, haystackTokens, tokens);

        return matched;
    }

    private static bool TryEvalNearOperator(string leftStr, string rightStr, out bool result)
    {
        result = false;

        if (!rightStr.StartsWith("NEAR(", StringComparison.OrdinalIgnoreCase))
            return false;

        var closeParen = rightStr.LastIndexOf(')');
        if (closeParen < 0)
            return false;

        var inner = rightStr.Substring(5, closeParen - 5);
        var terms = inner.Split([' ', ','], StringSplitOptions.RemoveEmptyEntries);

        result = terms.Length > 0 && terms.All(t => leftStr.IndexOf(t, StringComparison.OrdinalIgnoreCase) >= 0);
        return true;
    }

    private static bool EvaluateMatchQuery(
        string haystack,
        IReadOnlyList<string> haystackTokens,
        IReadOnlyList<string> tokens,
        int startIndex,
        int endIndex)
    {
        var orParts = SplitOnOperator(tokens, startIndex, endIndex, "OR", "|");
        if (orParts.Count > 1)
        {
            for (var i = 0; i < orParts.Count; i++)
            {
                var part = orParts[i];
                if (EvaluateMatchClause(haystack, haystackTokens, tokens, part.start, part.end))
                    return true;
            }

            return false;
        }

        return EvaluateMatchClause(haystack, haystackTokens, tokens, startIndex, endIndex);
    }

    private static bool EvaluateMatchClause(
        string haystack,
        IReadOnlyList<string> haystackTokens,
        IReadOnlyList<string> tokens,
        int startIndex,
        int endIndex)
    {
        var hasPositiveTerm = false;
        var i = startIndex;

        while (i <= endIndex)
        {
            if (IsKeyword(tokens[i], "AND") || tokens[i] == "&")
            {
                i++;
                continue;
            }

            if (IsKeyword(tokens[i], "NOT") || tokens[i] == "!")
            {
                i++;
                if (i > endIndex)
                    return false;

                var negated = EvaluateMatchTerm(haystack, haystackTokens, tokens, ref i, endIndex);
                if (negated)
                    return false;

                continue;
            }

            var matched = EvaluateMatchTerm(haystack, haystackTokens, tokens, ref i, endIndex);
            hasPositiveTerm |= matched;
            if (!matched)
                return false;
        }

        return hasPositiveTerm;
    }

    private static bool EvaluateMatchTerm(
        string haystack,
        IReadOnlyList<string> haystackTokens,
        IReadOnlyList<string> tokens,
        ref int index,
        int endIndex)
    {
        if (index > endIndex)
            return false;

        var token = tokens[index];

        if (token == "(")
        {
            var closing = FindMatchingParen(tokens, index, endIndex);
            if (closing < 0)
                return false;

            var inner = EvaluateMatchQuery(haystack, haystackTokens, tokens, index + 1, closing - 1);
            index = closing + 1;
            return inner;
        }

        if (token.StartsWith("NEAR(", StringComparison.OrdinalIgnoreCase) && token.EndsWith(')'))
        {
            index++;
            return TryEvalNearOperator(haystack, token, out var result) && result;
        }

        if (token.Length >= 2
            && ((token[0] == '"' && token[^1] == '"')
                || (token[0] == '\'' && token[^1] == '\'')))
        {
            index++;
            return haystack.IndexOf(token.Substring(1, token.Length - 2), StringComparison.OrdinalIgnoreCase) >= 0;
        }

        if (token == "<->")
        {
            index++;
            return false;
        }

        var current = token;
        index++;

        if (index <= endIndex && tokens[index] == "<->")
        {
            var phraseParts = new List<string> { current };
            while (index <= endIndex && tokens[index] == "<->")
            {
                index++;
                if (index > endIndex)
                    return false;

                var next = tokens[index];
                if (next == "(")
                {
                    var closing = FindMatchingParen(tokens, index, endIndex);
                    if (closing < 0)
                        return false;

                    for (var ti = index + 1; ti < closing; ti++)
                        phraseParts.Add(tokens[ti]);
                    index = closing + 1;
                    continue;
                }

                if (next.Length >= 2
                    && ((next[0] == '"' && next[^1] == '"')
                        || (next[0] == '\'' && next[^1] == '\'')))
                {
                    phraseParts.Add(next.Substring(1, next.Length - 2));
                }
                else
                {
                    phraseParts.Add(next);
                }

                index++;
            }

            return MatchesPhraseSequence(haystackTokens, phraseParts);
        }

        if (current.EndsWith('*') && current.Length > 1)
        {
            var prefix = current.Substring(0, current.Length - 1);
            return haystackTokens.Any(word => word.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
        }

        return haystackTokens.Any(word => word.Equals(current, StringComparison.OrdinalIgnoreCase));
    }

    private static bool MatchesPhraseSequence(IReadOnlyList<string> haystackTokens, IReadOnlyList<string> phraseParts)
    {
        if (phraseParts.Count == 0 || haystackTokens.Count == 0 || phraseParts.Count > haystackTokens.Count)
            return false;

        for (var i = 0; i <= haystackTokens.Count - phraseParts.Count; i++)
        {
            var matched = true;
            for (var j = 0; j < phraseParts.Count; j++)
            {
                if (!haystackTokens[i + j].Equals(phraseParts[j], StringComparison.OrdinalIgnoreCase))
                {
                    matched = false;
                    break;
                }
            }

            if (matched)
                return true;
        }

        return false;
    }

    private static int FindMatchingParen(IReadOnlyList<string> tokens, int openIndex, int endIndex)
    {
        var depth = 0;
        for (var i = openIndex; i <= endIndex; i++)
        {
            if (tokens[i] == "(")
            {
                depth++;
                continue;
            }

            if (tokens[i] == ")")
            {
                depth--;
                if (depth == 0)
                    return i;
            }
        }

        return -1;
    }

    private static List<(int start, int end)> SplitOnOperator(
        IReadOnlyList<string> tokens,
        int startIndex,
        int endIndex,
        string keyword,
        string symbol)
    {
        var parts = new List<(int start, int end)>();
        var partStart = startIndex;
        var depth = 0;

        for (var i = startIndex; i <= endIndex; i++)
        {
            if (tokens[i] == "(")
            {
                depth++;
                continue;
            }

            if (tokens[i] == ")")
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (IsKeyword(tokens[i], keyword) || tokens[i] == symbol)
            {
                if (i > partStart)
                    parts.Add((partStart, i - 1));

                partStart = i + 1;
            }
        }

        if (partStart <= endIndex)
            parts.Add((partStart, endIndex));

        return parts;
    }

    private static bool IsKeyword(string token, string keyword)
        => token.Equals(keyword, StringComparison.OrdinalIgnoreCase);

    private static bool ContainsBooleanOrOperator(string query)
        => query.IndexOf(" OR ", StringComparison.OrdinalIgnoreCase) >= 0
            || query.Contains('|');

    private static bool EvaluateMatchAnyRelevantTerm(
        string haystack,
        IReadOnlyList<string> haystackTokens,
        IReadOnlyList<string> tokens)
    {
        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (IsKeyword(token, "AND")
                || IsKeyword(token, "OR")
                || IsKeyword(token, "NOT")
                || token is "&" or "|" or "!" or "(" or ")" or "<->")
            {
                continue;
            }

            if (token.StartsWith("NEAR(", StringComparison.OrdinalIgnoreCase) && token.EndsWith(')'))
            {
                if (TryEvalNearOperator(haystack, token, out var nearResult) && nearResult)
                    return true;

                continue;
            }

            if (token.Length >= 2
                && ((token[0] == '"' && token[^1] == '"')
                    || (token[0] == '\'' && token[^1] == '\'')))
            {
                if (haystack.IndexOf(token.Substring(1, token.Length - 2), StringComparison.OrdinalIgnoreCase) >= 0)
                    return true;

                continue;
            }

            if (token.EndsWith('*') && token.Length > 1)
            {
                var prefix = token.Substring(0, token.Length - 1);
                if (haystackTokens.Any(word => word.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                    return true;

                continue;
            }

            if (haystackTokens.Any(word => word.Equals(token, StringComparison.OrdinalIgnoreCase)))
            {
                return true;
            }
        }

        return false;
    }

    private static IReadOnlyList<string> TokenizeMatchHaystack(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return [];

        var parts = value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        var result = new List<string>(parts.Length);
        foreach (var part in parts)
        {
            var trimmed = TrimMatchToken(part);
            if (trimmed.Length > 0)
                result.Add(trimmed);
        }
        return result;
    }

    private static IReadOnlyList<string> TokenizeMatchQuery(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        var tokens = new List<string>();
        var i = 0;
        while (i < query.Length)
        {
            while (i < query.Length && char.IsWhiteSpace(query[i]))
                i++;

            if (i >= query.Length)
                break;

            var ch = query[i];
            if (ch is '"' or '\'')
            {
                var quote = ch;
                var start = i;
                i++;
                while (i < query.Length)
                {
                    if (query[i] == quote)
                    {
                        if (i + 1 < query.Length && query[i + 1] == quote)
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    i++;
                }

                tokens.Add(query.Substring(start, i - start));
                continue;
            }

            if (i + 2 < query.Length && query.AsSpan(i, 3).SequenceEqual("<->".AsSpan()))
            {
                tokens.Add("<->");
                i += 3;
                continue;
            }

            if (ch is '&' or '|' or '!' or '(' or ')')
            {
                tokens.Add(ch.ToString());
                i++;
                continue;
            }

            var startWord = i;
            while (i < query.Length)
            {
                var current = query[i];
                if (char.IsWhiteSpace(current) || current is '&' or '|' or '!' || current == '<')
                    break;

                if (current == '(')
                {
                    i++;
                    var depth = 1;
                    var inSingleQuote = false;
                    var inDoubleQuote = false;

                    while (i < query.Length && depth > 0)
                    {
                        var inner = query[i];
                        if (inSingleQuote)
                        {
                            if (inner == '\'')
                            {
                                if (i + 1 < query.Length && query[i + 1] == '\'')
                                {
                                    i += 2;
                                    continue;
                                }

                                inSingleQuote = false;
                            }

                            i++;
                            continue;
                        }

                        if (inDoubleQuote)
                        {
                            if (inner == '"')
                            {
                                if (i + 1 < query.Length && query[i + 1] == '"')
                                {
                                    i += 2;
                                    continue;
                                }

                                inDoubleQuote = false;
                            }

                            i++;
                            continue;
                        }

                        if (inner == '\'')
                        {
                            inSingleQuote = true;
                            i++;
                            continue;
                        }

                        if (inner == '"')
                        {
                            inDoubleQuote = true;
                            i++;
                            continue;
                        }

                        if (inner == '(')
                            depth++;
                        else if (inner == ')')
                            depth--;

                        i++;
                    }

                    break;
                }

                i++;
            }

            tokens.Add(query.Substring(startWord, i - startWord));
        }

        return tokens;
    }

    private static string TrimMatchToken(string token)
    {
        return token.Trim(' ', '\t', '\r', '\n', ',', ';', ':', '?', '!', '(', ')', '[', ']', '{', '}', '"', '\'');
    }

    internal static bool IsSqlNullLike(object? value)
        => value is null or DBNull;

    internal static bool HasNullElement(object?[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is null or DBNull)
                return true;
        }

        return false;
    }
}
