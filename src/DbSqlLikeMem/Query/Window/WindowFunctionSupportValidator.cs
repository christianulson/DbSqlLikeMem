namespace DbSqlLikeMem;

internal static class WindowFunctionSupportValidator
{
    internal static void EnsureSupported(ISqlDialect? dialect, WindowFunctionExpr windowFunction)
    {
        if (!(dialect?.SupportsWindowFunctions ?? true)
            || !(dialect?.TryGetWindowFunctionDefinition(windowFunction, out _) ?? true))
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

        if (!dialect.TryGetWindowFunctionDefinition(windowFunction, out var definition)
            || definition is null)
            return;

        var actualArgs = windowFunction.Args.Count;
        if (!definition.AllowsArgumentCount(actualArgs))
        {
            if (definition.MinArguments == definition.MaxArguments)
            {
                throw new InvalidOperationException(
                    $"Window function '{windowFunction.Name}' expects exactly {definition.MinArguments} argument(s), but received {actualArgs}.");
            }

            throw new InvalidOperationException(
                $"Window function '{windowFunction.Name}' expects between {definition.MinArguments} and {definition.MaxArguments} argument(s), but received {actualArgs}.");
        }
    }
}
