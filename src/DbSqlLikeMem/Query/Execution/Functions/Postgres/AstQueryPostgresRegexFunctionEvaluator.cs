namespace DbSqlLikeMem;

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

internal static class AstQueryPostgresRegexFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("REGEXP_COUNT" or "REGEXP_INSTR" or "REGEXP_LIKE" or "REGEXP_MATCH" or "REGEXP_REPLACE" or "REGEXP_SPLIT_TO_ARRAY" or "REGEXP_SUBSTR"))
        {
            result = null;
            return false;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString();
        var pattern = evalArg(1)?.ToString();
        if (source is null || pattern is null)
        {
            result = null;
            return true;
        }

        try
        {
            var regexOptions = RegexOptions.CultureInvariant;
            var flags = TryGetPostgresRegexFlags(fn, evalArg, out var start, out var occurrence);
            if (HasRegexFlag(flags, 'i'))
                regexOptions |= RegexOptions.IgnoreCase;
            if (HasRegexFlag(flags, 'm'))
                regexOptions |= RegexOptions.Multiline;
            if (HasRegexFlag(flags, 'n') || HasRegexFlag(flags, 's'))
                regexOptions |= RegexOptions.Singleline;

            var startIndex = Math.Min(source.Length, Math.Max(0, start - 1));
            var segment = source[startIndex..];
            var regex = new Regex(pattern, regexOptions);

            if (name is "REGEXP_REPLACE")
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                var replaceAll = HasRegexFlag(flags, 'g');
                result = regex.Replace(source, replacement, replaceAll ? int.MaxValue : 1, 0);
                return true;
            }

            if (name is "REGEXP_SPLIT_TO_ARRAY")
            {
                result = regex.Split(source);
                return true;
            }

            var matches = regex.Matches(segment);
            if (name is "REGEXP_COUNT")
            {
                result = matches.Count;
                return true;
            }

            if (name is "REGEXP_LIKE")
            {
                result = matches.Count > 0;
                return true;
            }

            if (matches.Count == 0)
            {
                result = name == "REGEXP_INSTR" ? 0 : null;
                return true;
            }

            var index = Math.Min(Math.Max(1, occurrence) - 1, matches.Count - 1);
            var match = matches[index];

            if (name is "REGEXP_INSTR")
            {
                result = startIndex + match.Index + 1;
                return true;
            }

            if (name is "REGEXP_SUBSTR")
            {
                result = match.Value;
                return true;
            }

            var captureValues = match.Groups.Count > 1
                ? match.Groups.Cast<Group>().Skip(1).Select(static g => g.Value).ToArray()
                : [match.Value];
            result = captureValues;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static string TryGetPostgresRegexFlags(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out int start,
        out int occurrence)
    {
        start = 1;
        occurrence = 1;

        if (fn.Args.Count < 3 || AstQueryExecutorBase.IsNullish(evalArg(2)))
            return string.Empty;

        var third = evalArg(2);
        if (third is string flagText && !int.TryParse(flagText, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            return flagText;

        start = Math.Max(1, Convert.ToInt32(third!.ToDec(), CultureInfo.InvariantCulture));

        if (fn.Args.Count >= 4 && !AstQueryExecutorBase.IsNullish(evalArg(3)))
        {
            var fourth = evalArg(3);
            if (fourth is string fourthFlags && !int.TryParse(fourthFlags, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
                return fourthFlags;

            occurrence = Math.Max(1, Convert.ToInt32(fourth!.ToDec(), CultureInfo.InvariantCulture));
        }

        if (fn.Args.Count >= 5 && !AstQueryExecutorBase.IsNullish(evalArg(4)))
            return evalArg(4)?.ToString() ?? string.Empty;

        return string.Empty;
    }

    private static bool HasRegexFlag(string flags, char flag)
    {
        if (string.IsNullOrEmpty(flags))
            return false;

        var upperFlag = char.ToUpperInvariant(flag);
        foreach (var current in flags)
        {
            if (char.ToUpperInvariant(current) == upperFlag)
                return true;
        }

        return false;
    }
}
