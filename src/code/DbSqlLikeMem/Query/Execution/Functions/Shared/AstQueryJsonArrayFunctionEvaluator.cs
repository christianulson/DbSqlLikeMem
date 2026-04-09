namespace DbSqlLikeMem;

internal static class AstQueryJsonArrayFunctionEvaluator
{
    internal static bool TryEvalJsonArrayFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (!string.Equals(fn.Name, "JSON_ARRAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var values = new object?[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
            values[i] = evalArg(i);

        result = JsonSerializer.Serialize(values);
        return true;
    }
}
