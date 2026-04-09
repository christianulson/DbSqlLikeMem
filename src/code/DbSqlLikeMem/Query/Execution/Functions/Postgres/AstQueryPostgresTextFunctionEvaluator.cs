namespace DbSqlLikeMem;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

internal delegate bool AstQueryTryEvalPostgresTextFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresTextFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresTextFunction> _handlers =
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

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresTextFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresTextFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalBtrimFunction, "BTRIM");
        Register(handlers, TryEvalInitcapFunction, "INITCAP");
        Register(handlers, TryEvalChrFunction, "CHR");
        Register(handlers, TryEvalSplitPartFunction, "SPLIT_PART");
        Register(handlers, TryEvalStringToArrayFunction, "STRING_TO_ARRAY");
        Register(handlers, TryEvalQuoteLiteralFunction, "QUOTE_LITERAL");
        Register(handlers, TryEvalQuoteIdentFunction, "QUOTE_IDENT");
        Register(handlers, TryEvalToHexFunction, "TO_HEX");
        Register(handlers, TryEvalTranslateFunction, "TRANSLATE");
        Register(handlers, TryEvalStartsWithFunction, "STARTS_WITH");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresTextFunction> handlers,
        AstQueryTryEvalPostgresTextFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalBtrimFunction(
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

        result = (value?.ToString() ?? string.Empty).Trim();
        return true;
    }

    private static bool TryEvalInitcapFunction(
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

        var text = value?.ToString() ?? string.Empty;
        result = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(text.ToLowerInvariant());
        return true;
    }

    private static bool TryEvalChrFunction(
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

        try
        {
            var code = Convert.ToInt32(value.ToDec(), CultureInfo.InvariantCulture);
            if (code < 0 || code > 0x10FFFF)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(code);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSplitPartFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("SPLIT_PART() espera texto, separador e indice.");

        var text = evalArg(0)?.ToString() ?? string.Empty;
        var delimiter = evalArg(1)?.ToString() ?? string.Empty;
        var index = Convert.ToInt32(evalArg(2).ToDec());
        if (index <= 0)
        {
            result = string.Empty;
            return true;
        }

        var parts = text.Split([delimiter], StringSplitOptions.None);
        result = index <= parts.Length ? parts[index - 1] : string.Empty;
        return true;
    }

    private static bool TryEvalStringToArrayFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STRING_TO_ARRAY() espera texto e separador.");

        var text = evalArg(0)?.ToString() ?? string.Empty;
        var delimiter = evalArg(1)?.ToString() ?? string.Empty;
        result = text.Split([delimiter], StringSplitOptions.None);
        return true;
    }

    private static bool TryEvalQuoteLiteralFunction(
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

        var text = value?.ToString() ?? string.Empty;
        result = $"'{text.Replace("'", "''")}'";
        return true;
    }

    private static bool TryEvalQuoteIdentFunction(
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

        var text = value?.ToString() ?? string.Empty;
        result = $"\"{text.Replace("\"", "\"\"")}\"";
        return true;
    }

    private static bool TryEvalToHexFunction(
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

        var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
        result = number.ToString("x", CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryEvalTranslateFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var from = evalArg(1)?.ToString() ?? string.Empty;
        var to = evalArg(2)?.ToString() ?? string.Empty;

        var builder = new StringBuilder(source.Length);
        foreach (var ch in source)
        {
            var index = from.IndexOf(ch);
            if (index < 0)
            {
                builder.Append(ch);
                continue;
            }

            if (index < to.Length)
                builder.Append(to[index]);
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalStartsWithFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STARTS_WITH() espera texto e prefixo.");

        var source = evalArg(0)?.ToString();
        var prefix = evalArg(1)?.ToString();
        if (source is null || prefix is null)
        {
            result = null;
            return true;
        }

        result = source.StartsWith(prefix, StringComparison.Ordinal);
        return true;
    }
}
