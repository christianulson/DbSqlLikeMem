namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Text;

internal delegate bool AstQueryTryEvalPostgresUnicodeFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresUnicodeFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresUnicodeFunction> _handlers =
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

        return handler(context, fn, evalArg, out result);
    }

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresUnicodeFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresUnicodeFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalNormalizeFunction, "NORMALIZE");
        Register(handlers, TryEvalToAsciiFunction, "TO_ASCII");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresUnicodeFunction> handlers,
        AstQueryTryEvalPostgresUnicodeFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalNormalizeFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var formName = fn.Args.Count > 1
            ? (evalArg(1)?.ToString() ?? string.Empty).Trim().ToUpperInvariant()
            : "NFC";
        var form = formName switch
        {
            "" or "NFC" => NormalizationForm.FormC,
            "NFD" => NormalizationForm.FormD,
            "NFKC" => NormalizationForm.FormKC,
            "NFKD" => NormalizationForm.FormKD,
            _ => NormalizationForm.FormC
        };

        result = text.Normalize(form);
        return true;
    }

    private static bool TryEvalToAsciiFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = ConvertToAscii(value?.ToString() ?? string.Empty);
        return true;
    }

    private static string ConvertToAscii(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var decomposed = text.Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed)
        {
            var category = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (category is UnicodeCategory.NonSpacingMark
                or UnicodeCategory.SpacingCombiningMark
                or UnicodeCategory.EnclosingMark)
            {
                continue;
            }

            if (ch <= 0x7F)
                builder.Append(ch);
        }

        return builder.ToString().Normalize(NormalizationForm.FormC);
    }
}
