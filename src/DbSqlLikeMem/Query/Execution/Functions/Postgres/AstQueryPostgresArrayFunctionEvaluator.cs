namespace DbSqlLikeMem;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;

internal delegate bool AstQueryTryEvalPostgresArrayFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal static class AstQueryPostgresArrayFunctionEvaluator
{
    private static readonly IReadOnlyDictionary<string, AstQueryTryEvalPostgresArrayFunction> _handlers =
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

    private static IReadOnlyDictionary<string, AstQueryTryEvalPostgresArrayFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalPostgresArrayFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalArrayToStringFunction, "ARRAY_TO_STRING");
        Register(handlers, TryEvalArrayLengthFunction, "ARRAY_LENGTH");
        Register(handlers, TryEvalArrayUpperFunction, "ARRAY_UPPER");
        Register(handlers, TryEvalArrayLowerFunction, "ARRAY_LOWER");
        Register(handlers, TryEvalArrayDimsFunction, "ARRAY_DIMS");
        Register(handlers, TryEvalArrayNdimsFunction, "ARRAY_NDIMS");
        Register(handlers, TryEvalArrayPositionFunction, "ARRAY_POSITION");
        Register(handlers, TryEvalArrayPositionsFunction, "ARRAY_POSITIONS");
        Register(handlers, TryEvalArrayToJsonFunction, "ARRAY_TO_JSON");
        Register(handlers, TryEvalArrayAppendFunction, "ARRAY_APPEND");
        Register(handlers, TryEvalArrayPrependFunction, "ARRAY_PREPEND");
        Register(handlers, TryEvalArrayCatFunction, "ARRAY_CAT");
        Register(handlers, TryEvalArrayRemoveFunction, "ARRAY_REMOVE");
        Register(handlers, TryEvalArrayReplaceFunction, "ARRAY_REPLACE");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalPostgresArrayFunction> handlers,
        AstQueryTryEvalPostgresArrayFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalArrayToStringFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ARRAY_TO_STRING() espera array e separador.");

        var value = evalArg(0);
        var separator = evalArg(1)?.ToString() ?? string.Empty;
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is string[] stringArray)
        {
            result = string.Join(separator, stringArray.Select(item => item ?? string.Empty));
            return true;
        }

        if (value is IEnumerable enumerable)
        {
            result = string.Join(
                separator,
                enumerable.Cast<object?>().Select(item => item?.ToString() ?? string.Empty));
            return true;
        }

        result = value?.ToString() ?? string.Empty;
        return true;
    }

    private static bool TryEvalArrayLengthFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        return TryEvalArrayCountFunction(evalArg, out result, value => value.Count);
    }

    private static bool TryEvalArrayUpperFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        return TryEvalArrayCountFunction(evalArg, out result, value => value.Count);
    }

    private static bool TryEvalArrayLowerFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        return TryEvalArrayCountFunction(evalArg, out result, _ => 1);
    }

    private static bool TryEvalArrayDimsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        return TryEvalArrayDimensionFunction(evalArg, out result, list => $"[1:{list.Count}]");
    }

    private static bool TryEvalArrayNdimsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        return TryEvalArrayDimensionFunction(evalArg, out result, _ => 1);
    }

    private static bool TryEvalArrayPositionFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ARRAY_POSITION() espera array e valor.");

        var value = evalArg(0);
        var target = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var list = value is IEnumerable enumerable
            ? enumerable.Cast<object?>().ToList()
            : [];
        var index = list.FindIndex(item => Equals(item, target));
        result = index >= 0 ? index + 1 : (object?)null;
        return true;
    }

    private static bool TryEvalArrayPositionsFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ARRAY_POSITIONS() espera array e valor.");

        var value = evalArg(0);
        var target = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var list = value is IEnumerable enumerable
            ? enumerable.Cast<object?>().ToList()
            : [];

        var matches = new List<object?>(list.Count);
        for (var i = 0; i < list.Count; i++)
        {
            if (Equals(list[i], target))
                matches.Add(i + 1);
        }

        result = matches.ToArray();
        return true;
    }

    private static bool TryEvalArrayToJsonFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("ARRAY_TO_JSON() espera array.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var list = value is IEnumerable enumerable
            ? enumerable.Cast<object?>().ToList()
            : [];

        var writeIndented = fn.Args.Count > 1 && Convert.ToBoolean(evalArg(1), CultureInfo.InvariantCulture);
        var options = writeIndented
            ? new JsonSerializerOptions { WriteIndented = true }
            : null;
        result = JsonSerializer.Serialize(list, options);
        return true;
    }

    private static bool TryEvalArrayAppendFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalArrayMutationFunction(context,fn,  evalArg, out result, isCat: false, isPrepend: false, (list, args) => list.Add(args[1]));

    private static bool TryEvalArrayPrependFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalArrayMutationFunction(context, fn, evalArg, out result, isCat: false, isPrepend: true, (list, args) => list.Insert(0, args[0]));

    private static bool TryEvalArrayCatFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalArrayMutationFunction(
            context,
            fn,
            evalArg,
            out result,
            isCat: true,
            isPrepend: false,
            (list, args) =>
            {
                var right = args[1];
                if (!AstQueryExecutorBase.IsNullish(right) && right is IEnumerable rightEnum)
                    list.AddRange(rightEnum.Cast<object?>());
            });

    private static bool TryEvalArrayRemoveFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalArrayMutationFunction(
            context,
            fn,
            evalArg,
            out result,
            isCat: false,
            isPrepend: false,
            (list, args) =>
            {
                var target = args[1];
                list.RemoveAll(item => Equals(item, target));
            });

    private static bool TryEvalArrayReplaceFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalArrayMutationFunction(
            context,
            fn,
            evalArg,
            out result,
            isCat: false,
            isPrepend: false,
            (list, args) =>
            {
                var target = args[1];
                var replacement = args[2];
                for (var i = 0; i < list.Count; i++)
                {
                    if (Equals(list[i], target))
                        list[i] = replacement;
                }
            });

    private static bool TryEvalArrayMutationFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result,
        bool isCat,
        bool isPrepend,
        Action<List<object?>, object?[]> mutate)
    {
        _ = context;

        var args = new object?[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
            args[i] = evalArg(i);

        var left = isPrepend ? args[1] : args[0];
        var leftEnumerable = !AstQueryExecutorBase.IsNullish(left) && left is IEnumerable enumerable ? enumerable : null;
        var list = leftEnumerable is ICollection leftCollection
            ? new List<object?>(leftCollection.Count)
            : new List<object?>();
        if (leftEnumerable is not null)
            list.AddRange(leftEnumerable.Cast<object?>());

        if (isCat)
        {
            var right = args[1];
            if (!AstQueryExecutorBase.IsNullish(right) && right is IEnumerable rightEnum)
            {
                if (rightEnum is ICollection rightCollection
                    && list.Capacity < list.Count + rightCollection.Count)
                {
                    list.Capacity = list.Count + rightCollection.Count;
                }

                list.AddRange(rightEnum.Cast<object?>());
            }

            result = list.ToArray();
            return true;
        }

        mutate(list, args);
        result = list.ToArray();
        return true;
    }

    private static bool TryEvalArrayCountFunction(
        Func<int, object?> evalArg,
        out object? result,
        Func<List<object?>, int> selector)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var list = value is IEnumerable enumerable
            ? enumerable.Cast<object?>().ToList()
            : [];

        if (list.Count == 0)
        {
            result = null;
            return true;
        }

        result = selector(list);
        return true;
    }

    private static bool TryEvalArrayDimensionFunction(
        Func<int, object?> evalArg,
        out object? result,
        Func<List<object?>, object?> selector)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var list = value is IEnumerable enumerable
            ? enumerable.Cast<object?>().ToList()
            : [];

        if (list.Count == 0)
        {
            result = null;
            return true;
        }

        result = selector(list);
        return true;
    }
}
