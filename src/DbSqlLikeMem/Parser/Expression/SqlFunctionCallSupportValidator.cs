namespace DbSqlLikeMem;

internal static class SqlFunctionCallSupportValidator
{
    internal static void EnsureSupported(this SqlExpressionParserContext ctx, string name)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        EnsureSupported(ctx.Dialect, name, ctx.CustomFunctionSupported);
    }

    internal static void EnsureSupported(
        ISqlDialect dialect,
        string name,
        Func<string, bool>? isCustomFunctionSupported = null)
    {
        if (dialect.TryGetScalarFunctionDefinition(name, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw SqlUnsupported.ForDialect(dialect, name.ToUpperInvariant());
        }

        if (isCustomFunctionSupported?.Invoke(name) == true)
            return;

        throw SqlUnsupported.ForDialect(dialect, name.ToUpperInvariant());
    }

}
