namespace DbSqlLikeMem;

internal static class QueryTextSearchFunctionHelper
{
    private static readonly Regex _matchAgainstTermRegex = new(
        @"(?<sign>[+\-]?)(?:""(?<phrase>[^""]+)""|(?<term>[\p{L}\p{N}_*]+))",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _matchAgainstWordRegex = new(
        @"[\p{L}\p{N}_]+",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// EN: Evaluates CONTAINS/FREETEXT returning 1 (match) or 0 (no match).
    /// PT-br: Avalia CONTAINS/FREETEXT retornando 1 (correspondencia) ou 0 (sem correspondencia).
    /// </summary>
    public static bool TryEvalContainsFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!TryEvalMatchAgainstFunction(context, fn, evalArg, out var score))
        {
            result = null;
            return false;
        }

        result = score is int intScore && intScore > 0 ? 1 : 0;
        return true;
    }

    /// <summary>
    /// EN: Evaluates FIND_IN_SET returning the 1-based position of a value in a comma-separated list.
    /// PT-br: Avalia FIND_IN_SET retornando a posicao (base 1) de um valor em uma lista separada por virgula.
    /// </summary>
    public static bool TryEvalFindInSetFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var index = 0;
        var span = haystack.AsSpan();
        while (!span.IsEmpty)
        {
            var separatorIndex = span.IndexOf(',');
            var partSpan = separatorIndex < 0 ? span : span[..separatorIndex];
            var trimmed = partSpan.Trim();
            if (!trimmed.IsEmpty
                && string.Equals(trimmed.ToString(), needle, StringComparison.OrdinalIgnoreCase))
            {
                result = index + 1;
                return true;
            }

            index++;
            span = separatorIndex < 0 ? ReadOnlySpan<char>.Empty : span[(separatorIndex + 1)..];
        }

        result = 0;
        return true;
    }

    /// <summary>
    /// EN: Evaluates MATCH ... AGAINST (MySQL), returning a score based on term matching and boolean mode rules.
    /// PT-br: Avalia MATCH ... AGAINST (MySQL), retornando um score baseado na correspondencia de termos e regras de modo booleano.
    /// </summary>
    public static bool TryEvalMatchAgainstFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
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

        result = EvaluateMatchAgainstTerms(haystack, terms, modeSql, context.Dialect.TextComparison);
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
        {
            var parts = new List<string>(values.Length);
            for (var i = 0; i < values.Length; i++)
            {
                var item = values[i];
                if (IsNullish(item))
                    continue;

                parts.Add(item?.ToString() ?? string.Empty);
            }

            return string.Join(" ", parts);
        }

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
