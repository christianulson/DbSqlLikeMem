using System.Globalization;
using System.Linq;

namespace DbSqlLikeMem;

internal static class AstQueryPostgresScalarUtilityFunctionEvaluator
{
    private delegate bool PostgresScalarUtilityFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result);

    private static readonly IReadOnlyDictionary<string, PostgresScalarUtilityFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, evalArg, out result);

        return false;
    }

    private static Dictionary<string, PostgresScalarUtilityFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, PostgresScalarUtilityFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalNumNullsFunction, "NUM_NULLS");
        Register(handlers, TryEvalNumNonNullsFunction, "NUM_NONNULLS");
        Register(handlers, TryEvalLcmFunction, "LCM");
        Register(handlers, TryEvalMinScaleFunction, "MIN_SCALE");
        Register(handlers, TryEvalParseIdentFunction, "PARSE_IDENT");
        return handlers;
    }

    private static void Register(
        IDictionary<string, PostgresScalarUtilityFunctionHandler> handlers,
        PostgresScalarUtilityFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalNumNullsFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalNumNonNullsFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        result = Enumerable.Range(0, fn.Args.Count).Count(i => !AstQueryExecutorBase.IsNullish(evalArg(i)));
        return true;
    }

    private static bool TryEvalLcmFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var leftValue = evalArg(0);
        var rightValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(leftValue) || AstQueryExecutorBase.IsNullish(rightValue))
        {
            result = null;
            return true;
        }

        var left = Math.Abs(Convert.ToInt64(leftValue.ToDec(), CultureInfo.InvariantCulture));
        var right = Math.Abs(Convert.ToInt64(rightValue.ToDec(), CultureInfo.InvariantCulture));
        if (left == 0 || right == 0)
        {
            result = 0L;
            return true;
        }

        result = checked((left / AstQueryGeneralScalarFunctionEvaluator.ComputeGreatestCommonDivisor(left, right)) * right);
        return true;
    }

    private static bool TryEvalMinScaleFunction(
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

        result = AstQueryGeneralScalarFunctionEvaluator.GetMinimumNumericScale(value!);
        return true;
    }

    private static bool TryEvalParseIdentFunction(
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
        if (!AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresIdentifierParts(text, out var parts))
        {
            result = null;
            return true;
        }

        result = parts.ToArray();
        return true;
    }
}
