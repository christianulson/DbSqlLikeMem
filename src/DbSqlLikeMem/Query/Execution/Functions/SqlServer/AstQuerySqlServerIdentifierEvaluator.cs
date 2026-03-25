namespace DbSqlLikeMem;

internal static class AstQuerySqlServerIdentifierEvaluator
{
    internal static bool TryResolveIdentifier(
        QueryExecutionContext context,
        IdentifierExpr identifier,
        DbConnectionMockBase connection,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!dialect.SupportsSqlServerMetadataIdentifier(identifier.Name))
        {
            result = null;
            return false;
        }

        if (identifier.Name.Equals("@@DATEFIRST", StringComparison.OrdinalIgnoreCase))
        {
            result = 7;
            return true;
        }

        if (identifier.Name.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.GetLastInsertId();
            return true;
        }

        if (identifier.Name.Equals("@@MAX_PRECISION", StringComparison.OrdinalIgnoreCase))
        {
            result = 38;
            return true;
        }

        if (identifier.Name.Equals("@@TEXTSIZE", StringComparison.OrdinalIgnoreCase))
        {
            result = 4096;
            return true;
        }

        if (identifier.Name.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase))
        {
            result = connection.GetLastFoundRows();
            return true;
        }

        if (identifier.Name.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Name.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = "dbo";
            return true;
        }

        if (identifier.Name.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = "sa";
            return true;
        }

        result = null;
        return false;
    }
}
