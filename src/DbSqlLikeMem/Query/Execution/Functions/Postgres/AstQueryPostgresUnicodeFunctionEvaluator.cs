namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Text;

internal delegate bool AstQueryTryEvalPostgresUnicodeFunction(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresUnicodeFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresUnicodeFunction> _handlers =
        CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!_handlers.TryGetValue(fn.Name, out var handler))
        {
            result = null;
            return false;
        }

        return handler(fn, context, evalArg, out result);
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
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
        FunctionCallExpr fn,
        QueryExecutionContext context,
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

        result = AstQueryGeneralScalarFunctionEvaluator.ConvertToAscii(value?.ToString() ?? string.Empty);
        return true;
    }
}
