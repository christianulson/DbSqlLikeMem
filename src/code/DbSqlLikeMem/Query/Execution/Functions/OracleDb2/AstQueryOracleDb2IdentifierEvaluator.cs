namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2IdentifierEvaluator
{
    internal static bool TryResolveIdentifier(
        this QueryExecutionContext context,
        IdentifierExpr identifier,
        out object? result)
    {
        if (!context.Dialect.SupportsOracleReservedIdentifier(identifier.Name))
        {
            result = null;
            return false;
        }

        if (identifier.Name.Equals("USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Name.Equals("ORA_INVOKING_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = "SYS";
            return true;
        }

        if (identifier.Name.Equals("ORA_INVOKING_USERID", StringComparison.OrdinalIgnoreCase))
        {
            result = 0;
            return true;
        }

        result = null;
        return false;
    }
}
