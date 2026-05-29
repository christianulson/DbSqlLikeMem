namespace DbSqlLikeMem;

internal static class QueryTextSearchFunctionHelper
{
    private static readonly Regex _matchAgainstTermRegex = new(
        @"(?<sign>[+\-]?)(?:""(?<phrase>[^""]+)""|(?<term>[\p{L}\p{N}_*]+))",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex _sqlServerInflectionalFormsRegex = new(
        @"FORMSOF\s*\(\s*INFLECTIONAL\s*,\s*(?<term>(?:""[^""]+""|'[^']+'|[^()]+?))\s*\)",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex _sqlServerNearRegex = new(
        @"^\s*NEAR\s*\(\s*\((?<terms>.*?)\)\s*(?:,\s*(?<distance>\d+)\s*(?:,\s*(?<matchOrder>TRUE|FALSE))?\s*)?\)\s*$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex _oracleFuzzyRegex = new(
        @"\bFUZZY\s*\(\s*(?<term>(?:""[^""]+""|'[^']+'|[^,()]+?))\s*,\s*(?<score>\d+)\s*\)",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex _oracleWithinRegex = new(
        @"\bWITHIN\s+[A-Za-z_][A-Za-z0-9_#$]*",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Compiled);
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
        var normalizedQuery = NormalizeContainsQuery(evalArg(1)?.ToString() ?? string.Empty);
        var haystack = FlattenMatchAgainstTarget(evalArg(0));
        var comparison = StringComparison.OrdinalIgnoreCase;
        if (TryEvalSqlServerNearQuery(haystack, normalizedQuery, comparison, out var nearMatch))
        {
            result = nearMatch ? 1 : 0;
            return true;
        }

        result = EvaluateContainsQuery(haystack, normalizedQuery, comparison) ? 1 : 0;
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

    private static bool TryEvalSqlServerNearQuery(
        string haystack,
        string query,
        StringComparison comparison,
        out bool isMatch)
    {
        isMatch = false;
        if (string.IsNullOrWhiteSpace(haystack) || string.IsNullOrWhiteSpace(query))
            return false;

        var match = _sqlServerNearRegex.Match(query);
        if (!match.Success)
            return false;

        var terms = SplitSqlServerNearTerms(match.Groups["terms"].Value);
        if (terms.Count == 0)
        {
            isMatch = false;
            return true;
        }

        var maxDistance = match.Groups["distance"].Success
            ? int.Parse(match.Groups["distance"].Value, CultureInfo.InvariantCulture)
            : 8;
        if (maxDistance < 0)
        {
            isMatch = false;
            return true;
        }

        var haystackWords = ExtractMatchAgainstWords(haystack);
        var termPositions = new List<IReadOnlyList<int>>(terms.Count);
        foreach (var term in terms)
        {
            var positions = FindNearTermPositions(haystackWords, term, comparison);
            if (positions.Count == 0)
            {
                isMatch = false;
                return true;
            }

            termPositions.Add(positions);
        }

        isMatch = HasNearMatch(termPositions, maxDistance);
        return true;
    }

    private static string NormalizeSqlServerFullTextQuery(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return string.Empty;

        return _sqlServerInflectionalFormsRegex.Replace(query, match =>
        {
            var term = match.Groups["term"].Value.Trim();
            if (term.Length >= 2
                && ((term[0] == '"' && term[^1] == '"')
                    || (term[0] == '\'' && term[^1] == '\'')))
            {
                term = term[1..^1];
            }

            var forms = ExpandInflectionalForms(term);
            return string.Join(" ", forms);
        });
    }

    private static string NormalizeContainsQuery(string query)
    {
        var normalized = NormalizeSqlServerFullTextQuery(query);
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        normalized = _oracleFuzzyRegex.Replace(normalized, match =>
        {
            var term = match.Groups["term"].Value.Trim();
            if (term.Length >= 2
                && ((term[0] == '"' && term[^1] == '"')
                    || (term[0] == '\'' && term[^1] == '\'')))
            {
                term = term[1..^1];
            }

            term = term.Trim();
            if (term.Length == 0)
                return string.Empty;

            return term.EndsWith("*", StringComparison.Ordinal) ? term : term + "*";
        });

        normalized = _oracleWithinRegex.Replace(normalized, string.Empty);
        return normalized;
    }

    private static bool EvaluateContainsQuery(
        string haystack,
        string query,
        StringComparison comparison)
    {
        if (string.IsNullOrWhiteSpace(haystack) || string.IsNullOrWhiteSpace(query))
            return false;

        var andClauses = SplitContainsBooleanClauses(query, "AND");
        if (andClauses.Count > 1)
            return andClauses.All(clause => EvaluateContainsClause(haystack, clause, comparison));

        var orClauses = SplitContainsBooleanClauses(query, "OR");
        if (orClauses.Count > 1)
            return orClauses.Any(clause => EvaluateContainsClause(haystack, clause, comparison));

        return EvaluateContainsClause(haystack, query, comparison);
    }

    private static bool EvaluateContainsClause(
        string haystack,
        string clause,
        StringComparison comparison)
    {
        var trimmed = clause.Trim();
        if (trimmed.Length == 0)
            return false;

        if (trimmed.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase))
            return !EvaluateContainsClause(haystack, trimmed[4..], comparison);

        if (TryEvalSqlServerNearQuery(haystack, trimmed, comparison, out var nearMatch))
            return nearMatch;

        var haystackWords = ExtractMatchAgainstWords(haystack);
        var terms = ExtractMatchAgainstTerms(trimmed);
        if (terms.Count == 0)
            return false;

        foreach (var term in terms)
        {
            if (ContainsMatchAgainstTerm(haystack, haystackWords, term, comparison))
                return true;
        }

        return false;
    }

    private static IReadOnlyList<string> SplitContainsBooleanClauses(string query, string keyword)
    {
        if (string.IsNullOrWhiteSpace(query))
            return [];

        var clauses = new List<string>();
        var start = 0;
        var depth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < query.Length; i++)
        {
            var ch = query[i];
            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < query.Length && query[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '"')
                {
                    if (i + 1 < query.Length && query[i + 1] == '"')
                    {
                        i++;
                        continue;
                    }

                    inDoubleQuote = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth != 0)
                continue;

            if (IsBooleanKeywordAt(query, i, keyword))
            {
                clauses.Add(query[start..i]);
                i += keyword.Length - 1;
                start = i + 1;
            }
        }

        clauses.Add(query[start..]);
        return [.. clauses.Where(static clause => !string.IsNullOrWhiteSpace(clause))];
    }

    private static bool IsBooleanKeywordAt(string input, int index, string keyword)
    {
        if (index < 0 || string.IsNullOrWhiteSpace(keyword) || index + keyword.Length > input.Length)
            return false;

        if (!input.AsSpan(index, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
            return false;

        var beforeOk = index == 0 || char.IsWhiteSpace(input[index - 1]) || input[index - 1] == '(';
        var afterIndex = index + keyword.Length;
        var afterOk = afterIndex >= input.Length || char.IsWhiteSpace(input[afterIndex]) || input[afterIndex] == ')';
        return beforeOk && afterOk;
    }

    private static IReadOnlyList<string> ExpandInflectionalForms(string term)
    {
        if (string.IsNullOrWhiteSpace(term))
            return [];

        var forms = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(string candidate)
        {
            var normalized = candidate.Trim();
            if (normalized.Length == 0 || !seen.Add(normalized))
                return;

            forms.Add(normalized);
        }

        Add(term);

        var stem = term;
        if (stem.EndsWith("ing", StringComparison.OrdinalIgnoreCase) && stem.Length > 4)
        {
            stem = stem[..^3];
            if (stem.Length >= 2 && stem[^1] == stem[^2])
                stem = stem[..^1];
        }
        else if (stem.EndsWith("ed", StringComparison.OrdinalIgnoreCase) && stem.Length > 3)
        {
            stem = stem[..^2];
        }
        else if (stem.EndsWith("es", StringComparison.OrdinalIgnoreCase) && stem.Length > 3)
        {
            stem = stem[..^2];
        }
        else if (stem.EndsWith("s", StringComparison.OrdinalIgnoreCase) && stem.Length > 2)
        {
            stem = stem[..^1];
        }

        if (!string.Equals(stem, term, StringComparison.OrdinalIgnoreCase))
            Add(stem);

        var baseForm = stem;
        if (baseForm.Length > 0)
        {
            Add(baseForm + "s");
            Add(baseForm + "ed");
            Add(baseForm + "ing");

            if (!baseForm.EndsWith("e", StringComparison.OrdinalIgnoreCase))
                Add(baseForm + "es");
        }

        return forms;
    }

    private static IReadOnlyList<string> SplitSqlServerNearTerms(string termsSql)
    {
        if (string.IsNullOrWhiteSpace(termsSql))
            return [];

        var terms = new List<string>();
        var start = 0;
        var depth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < termsSql.Length; i++)
        {
            var ch = termsSql[i];
            if (inSingleQuote)
            {
                if (ch == '\'')
                {
                    if (i + 1 < termsSql.Length && termsSql[i + 1] == '\'')
                    {
                        i++;
                        continue;
                    }

                    inSingleQuote = false;
                }

                continue;
            }

            if (inDoubleQuote)
            {
                if (ch == '"')
                {
                    if (i + 1 < termsSql.Length && termsSql[i + 1] == '"')
                    {
                        i++;
                        continue;
                    }

                    inDoubleQuote = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                AddSqlServerNearTerm(terms, termsSql[start..i]);
                start = i + 1;
            }
        }

        AddSqlServerNearTerm(terms, termsSql[start..]);
        return terms;
    }

    private static void AddSqlServerNearTerm(List<string> terms, string rawTerm)
    {
        var trimmed = rawTerm.Trim();
        if (trimmed.Length == 0)
            return;

        if ((trimmed[0] == '\'' && trimmed[^1] == '\'')
            || (trimmed[0] == '"' && trimmed[^1] == '"'))
        {
            trimmed = trimmed[1..^1];
        }

        var words = ExtractMatchAgainstWords(trimmed);
        if (words.Count == 0)
            return;

        terms.Add(string.Join(" ", words));
    }

    private static IReadOnlyList<int> FindNearTermPositions(
        IReadOnlyList<string> haystackWords,
        string term,
        StringComparison comparison)
    {
        var termWords = term
            .Split(' ')
            .Where(part => !string.IsNullOrWhiteSpace(part))
            .Select(part => part.Trim())
            .ToArray();
        if (termWords.Length == 0)
            return [];

        if (termWords.Length == 1)
        {
            var positions = new List<int>();
            for (var i = 0; i < haystackWords.Count; i++)
            {
                if (haystackWords[i].Equals(termWords[0], comparison))
                    positions.Add(i);
            }

            return positions;
        }

        var matches = new List<int>();
        for (var i = 0; i <= haystackWords.Count - termWords.Length; i++)
        {
            var matched = true;
            for (var j = 0; j < termWords.Length; j++)
            {
                if (!haystackWords[i + j].Equals(termWords[j], comparison))
                {
                    matched = false;
                    break;
                }
            }

            if (matched)
                matches.Add(i);
        }

        return matches;
    }

    private static bool HasNearMatch(IReadOnlyList<IReadOnlyList<int>> termPositions, int maxDistance)
    {
        if (termPositions.Count == 0)
            return false;

        return HasNearMatchCore(termPositions, maxDistance, 0, int.MaxValue, int.MinValue);
    }

    private static bool HasNearMatchCore(
        IReadOnlyList<IReadOnlyList<int>> termPositions,
        int maxDistance,
        int termIndex,
        int minPosition,
        int maxPosition)
    {
        if (termIndex >= termPositions.Count)
            return maxPosition >= minPosition && maxPosition - minPosition <= maxDistance;

        var positions = termPositions[termIndex];
        for (var i = 0; i < positions.Count; i++)
        {
            var position = positions[i];
            var nextMin = minPosition == int.MaxValue ? position : Math.Min(minPosition, position);
            var nextMax = maxPosition == int.MinValue ? position : Math.Max(maxPosition, position);
            if (nextMax - nextMin > maxDistance)
                continue;

            if (HasNearMatchCore(termPositions, maxDistance, termIndex + 1, nextMin, nextMax))
                return true;
        }

        return false;
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
