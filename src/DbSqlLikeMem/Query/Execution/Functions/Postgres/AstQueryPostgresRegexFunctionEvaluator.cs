namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

internal delegate bool AstQueryTryEvalPostgresRegexFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresRegexFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresRegexFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        return handler(context,fn,  evalArg, out result);
    }

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresRegexFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresRegexFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalRegexCountFunction, "REGEXP_COUNT");
        Register(handlers, TryEvalRegexInstrFunction, "REGEXP_INSTR");
        Register(handlers, TryEvalRegexLikeFunction, "REGEXP_LIKE");
        Register(handlers, TryEvalRegexMatchFunction, "REGEXP_MATCH");
        Register(handlers, TryEvalRegexReplaceFunction, "REGEXP_REPLACE");
        Register(handlers, TryEvalRegexSplitToArrayFunction, "REGEXP_SPLIT_TO_ARRAY");
        Register(handlers, TryEvalRegexSubstrFunction, "REGEXP_SUBSTR");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresRegexFunction> handlers,
        AstQueryTryEvalPostgresRegexFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalRegexCountFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (_, _, _, _) => null, countMatches: true, returnsBool: false);

    private static bool TryEvalRegexInstrFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (_, _, _, _) => null, countMatches: false, returnsBool: false, returnIndex: true);

    private static bool TryEvalRegexLikeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (_, _, _, _) => null, countMatches: false, returnsBool: true);

    private static bool TryEvalRegexMatchFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (_, match, _, _) => match.Groups.Count > 1
            ? match.Groups.Cast<Group>().Skip(1).Select(static g => g.Value).ToArray()
            : [match.Value], countMatches: false, returnsBool: false, returnCapture: true);

    private static bool TryEvalRegexReplaceFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (source, match, replacement, replaceAll) => null, countMatches: false, returnsBool: false, replace: true);

    private static bool TryEvalRegexSplitToArrayFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (source, match, replacement, replaceAll) => null, countMatches: false, returnsBool: false, split: true);

    private static bool TryEvalRegexSubstrFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRegexFunction(context, fn, evalArg, out result, static (_, match, _, _) => match.Value, countMatches: false, returnsBool: false, returnSubstring: true);

    private static bool TryEvalRegexFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result,
        Func<string, Match, string, bool, object?> projector,
        bool countMatches,
        bool returnsBool,
        bool returnIndex = false,
        bool returnCapture = false,
        bool replace = false,
        bool split = false,
        bool returnSubstring = false)
    {
        _ = context;

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

            if (replace)
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                var replaceAll = HasRegexFlag(flags, 'g');
                result = regex.Replace(source, replacement, replaceAll ? int.MaxValue : 1, 0);
                return true;
            }

            if (split)
            {
                result = regex.Split(source);
                return true;
            }

            var matches = regex.Matches(segment);
            if (countMatches)
            {
                result = matches.Count;
                return true;
            }

            if (returnsBool)
            {
                result = matches.Count > 0;
                return true;
            }

            if (matches.Count == 0)
            {
                result = returnIndex ? 0 : null;
                return true;
            }

            var index = Math.Min(Math.Max(1, occurrence) - 1, matches.Count - 1);
            var match = matches[index];

            if (returnIndex)
            {
                result = startIndex + match.Index + 1;
                return true;
            }

            if (returnSubstring)
            {
                result = match.Value;
                return true;
            }

            if (returnCapture)
            {
                result = match.Groups.Count > 1
                    ? match.Groups.Cast<Group>().Skip(1).Select(static g => g.Value).ToArray()
                    : [match.Value];
                return true;
            }

            result = projector(source, match, string.Empty, false);
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
