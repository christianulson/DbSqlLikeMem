namespace DbSqlLikeMem;

internal static class QueryTextSearchFunctionHelper
{
    private static readonly Regex _matchAgainstTermRegex = new(
        @"(?<sign>[+\-]?)(?:""(?<phrase>[^""]+)""|(?<term>[\p{L}\p{N}_*]+))",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _matchAgainstWordRegex = new(
        @"[\p{L}\p{N}_]+",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static bool TryEvalFindInSetFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("FIND_IN_SET", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var parts = haystack.Split(',').Select(_=>_.Trim()).Where(_=>!string.IsNullOrWhiteSpace(_)).ToArray();
        var index = Array.FindIndex(parts, part => string.Equals(part, needle, StringComparison.OrdinalIgnoreCase));
        result = index >= 0 ? index + 1 : 0;
        return true;
    }

    public static bool TryEvalMatchAgainstFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("MATCH_AGAINST", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.SupportsMatchAgainstPredicate)
            throw SqlUnsupported.ForDialect(dialect, "MATCH ... AGAINST full-text predicate");

        if (fn.Args.Count < 2)
        {
            result = 0;
            return true;
        }

        var haystack = FlattenMatchAgainstTarget(evalArg(0));
        var query = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(haystack) || string.IsNullOrWhiteSpace(query))
        {
            result = 0;
            return true;
        }

        var terms = ExtractMatchAgainstTerms(query);
        if (terms.Count == 0)
        {
            result = 0;
            return true;
        }

        var modeSql = fn.Args.Count > 2
            ? (fn.Args[2] is RawSqlExpr rx ? rx.Sql : evalArg(2)?.ToString() ?? string.Empty)
            : string.Empty;

        result = EvaluateMatchAgainstTerms(haystack, terms, modeSql, dialect.TextComparison);
        return true;
    }

    private static int EvaluateMatchAgainstTerms(
        string haystack,
        IReadOnlyList<MatchAgainstTerm> terms,
        string modeSql,
        StringComparison comparison)
    {
        var isBooleanMode = modeSql.IndexOf("BOOLEAN MODE", StringComparison.OrdinalIgnoreCase) >= 0;
        var haystackWords = ExtractMatchAgainstWords(haystack);
        var score = 0;

        foreach (var term in terms)
        {
            var found = ContainsMatchAgainstTerm(haystack, haystackWords, term, comparison);
            if (isBooleanMode)
            {
                if (term.Prohibited && found)
                    return 0;

                if (term.Required && !found)
                    return 0;
            }

            if (found && !term.Prohibited)
                score++;
        }

        return score;
    }

    private static string FlattenMatchAgainstTarget(object? value)
    {
        if (value is object?[] values)
            return string.Join(" ", values.Where(static v => !IsNullish(v)).Select(v => v?.ToString() ?? string.Empty));

        return value?.ToString() ?? string.Empty;
    }

    private static IReadOnlyList<MatchAgainstTerm> ExtractMatchAgainstTerms(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        return [.. _matchAgainstTermRegex.Matches(query)
            .Cast<Match>()
            .Select(static m =>
            {
                var sign = m.Groups["sign"].Value;
                var phrase = m.Groups["phrase"].Value;
                var token = !string.IsNullOrWhiteSpace(phrase)
                    ? phrase
                    : m.Groups["term"].Value;

                var prefixWildcard = token.EndsWith("*", StringComparison.Ordinal);
                if (prefixWildcard)
                    token = token[..^1];

                return new MatchAgainstTerm(
                    token,
                    Required: sign == "+",
                    Prohibited: sign == "-",
                    PrefixWildcard: prefixWildcard,
                    IsPhrase: !string.IsNullOrWhiteSpace(phrase));
            })
            .Where(static term => !string.IsNullOrWhiteSpace(term.Value))
            .Distinct()];
    }

    private static IReadOnlyList<string> ExtractMatchAgainstWords(string haystack)
    {
        if (string.IsNullOrWhiteSpace(haystack))
            return [];

        return [.. _matchAgainstWordRegex.Matches(haystack)
            .Cast<Match>()
            .Select(static m => m.Value)
            .Where(static value => !string.IsNullOrWhiteSpace(value))];
    }

    private static bool ContainsMatchAgainstTerm(
        string haystack,
        IReadOnlyList<string> haystackWords,
        MatchAgainstTerm term,
        StringComparison comparison)
    {
        if (term.IsPhrase)
            return haystack.IndexOf(term.Value, comparison) >= 0;

        if (term.PrefixWildcard)
            return haystackWords.Any(word => word.StartsWith(term.Value, comparison));

        return haystackWords.Any(word => word.Equals(term.Value, comparison));
    }

    private static bool IsNullish(object? value) => value is null or DBNull;

    private readonly record struct MatchAgainstTerm(
        string Value,
        bool Required,
        bool Prohibited,
        bool PrefixWildcard,
        bool IsPhrase);
}
