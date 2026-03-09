namespace DbSqlLikeMem;

internal static class WindowFunctionSupportValidator
{
    internal static void EnsureSupported(ISqlDialect? dialect, WindowFunctionExpr windowFunction)
    {
        if (!(dialect?.SupportsWindowFunctions ?? true)
            || !(dialect?.SupportsWindowFunction(windowFunction.Name) ?? true))
        {
            throw SqlUnsupported.ForDialect(
                dialect ?? throw new InvalidOperationException("Dialect is required for window function validation."),
                $"window functions ({windowFunction.Name})");
        }

        EnsureArgumentArity(dialect, windowFunction);
    }

    internal static void EnsureArgumentArity(ISqlDialect? dialect, WindowFunctionExpr windowFunction)
    {
        if (dialect is null)
            return;

        if (!dialect.TryGetWindowFunctionArgumentArity(windowFunction.Name, out var minArgs, out var maxArgs))
            return;

        var actualArgs = windowFunction.Args.Count;
        if (actualArgs < minArgs || actualArgs > maxArgs)
        {
            if (minArgs == maxArgs)
            {
                throw new InvalidOperationException(
                    $"Window function '{windowFunction.Name}' expects exactly {minArgs} argument(s), but received {actualArgs}.");
            }

            throw new InvalidOperationException(
                $"Window function '{windowFunction.Name}' expects between {minArgs} and {maxArgs} argument(s), but received {actualArgs}.");
        }
    }
}
