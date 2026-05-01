namespace DbSqlLikeMem;

internal static class SqlFunctionCallSupportValidator
{
    internal static void EnsureSupported(
        this SqlExpressionParserContext ctx,
        string name)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        if (ctx.CustomFunctionSupported?.Invoke(name) == true)
            return;

        if (ctx.Db.ContainsRuntimeFunction(name))
            return;

        if (ctx.Db.TryGetFunction(name, out _))
            return;

        if (name.Equals("IIF", StringComparison.OrdinalIgnoreCase))
        {
            if (ctx.Dialect.SupportsIifFunction)
                return;

            throw ctx.NotSupported("IIF");
        }

        if (ctx.Dialect.TryGetScalarFunctionDefinition(name, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw ctx.NotSupported(name);
        }

        // EN: MATCH is a special predicate handled by SqlMatchAgainstExpressionParserHelper.
        // PT-br: MATCH é um predicado especial tratado pelo SqlMatchAgainstExpressionParserHelper.
        if (name.Equals("MATCH", StringComparison.OrdinalIgnoreCase))
        {
            if (ctx.Dialect.SupportsMatchAgainstPredicate)
                return;

            throw ctx.NotSupported("MATCH ... AGAINST");
        }

        // EN: Allow JSON_TABLE here so SqlSpecialFunctionCallParserHelper can throw a better error.
        // PT-br: Permite JSON_TABLE aqui para que o SqlSpecialFunctionCallParserHelper possa lançar um erro melhor.
        if (name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
            return;

        if (name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CONVERT", StringComparison.OrdinalIgnoreCase))
            return;

        if (name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase))
            return;

        if (ctx.Dialect.SupportsWindowFunction(name))
            return;

        if (AggregateFunctionCatalog.Contains(name))
        {
            if (name.StartsWith("APPROX_", StringComparison.OrdinalIgnoreCase)
                && !ctx.Dialect.SupportsApproximateAggregateFunction(name))
            {
                throw ctx.NotSupported(name);
            }

            if (name.Equals("RATIO_TO_REPORT", StringComparison.OrdinalIgnoreCase)
                && !ctx.Dialect.SupportsOracleAnalyticsFunction(name))
            {
                throw ctx.NotSupported(name);
            }

            if (name is SqlConst.GROUP_CONCAT or SqlConst.STRING_AGG or SqlConst.LISTAGG
                && !ctx.Dialect.TryGetScalarFunctionDefinition(name, out _)
                && !ctx.Dialect.SupportsWithinGroupForStringAggregates)
            {
                throw ctx.NotSupported(name);
            }

            return;
        }

        throw ctx.NotSupported(name);
    }

}
