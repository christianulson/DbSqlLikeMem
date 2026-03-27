namespace DbSqlLikeMem;

internal static class SqlFunctionCallSupportValidator
{
    internal static void EnsureSupported(
        this SqlExpressionParserContext ctx,
        string name)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(ctx, nameof(ctx));
        if (ctx.Dialect.TryGetScalarFunctionDefinition(name, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw ctx.NotSupported(name.ToUpperInvariant());
        }

        // EN: MATCH is a special predicate handled by SqlMatchAgainstExpressionParserHelper.
        // PT: MATCH é um predicado especial tratado pelo SqlMatchAgainstExpressionParserHelper.
        if (name.Equals("MATCH", StringComparison.OrdinalIgnoreCase))
        {
            if (ctx.Dialect.SupportsMatchAgainstPredicate)
                return;

            throw ctx.NotSupported("MATCH ... AGAINST");
        }

        // EN: Allow JSON_TABLE here so SqlSpecialFunctionCallParserHelper can throw a better error.
        // PT: Permite JSON_TABLE aqui para que o SqlSpecialFunctionCallParserHelper possa lançar um erro melhor.
        if (name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
            return;

        if (ctx.CustomFunctionSupported?.Invoke(name) == true)
            return;

        throw ctx.NotSupported(name.ToUpperInvariant());
    }

}
