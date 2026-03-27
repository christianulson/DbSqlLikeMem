namespace DbSqlLikeMem;

internal static class AstQueryDialectIdentifierEvaluator
{
    internal static bool TryResolveIdentifierCore(
        QueryExecutionContext context,
        IdentifierExpr identifier,
        DateTime evaluationLocalNow,
        DateTime evaluationUtcNow,
        DbConnectionMockBase connection,
        out object? result)
        => context.TryResolveIdentifier(identifier, evaluationLocalNow, evaluationUtcNow, connection, out result);

    internal static bool TryResolveIdentifier(
        this QueryExecutionContext context,
        IdentifierExpr identifier,
        DateTime evaluationLocalNow,
        DateTime evaluationUtcNow,
        DbConnectionMockBase connection,
        out object? result)
    {
        if (TryResolveBoundScalarFunctionIdentifier(context, identifier, evaluationLocalNow, evaluationUtcNow, out result))
            return true;

        if (AstQuerySqlServerIdentifierEvaluator.TryResolveIdentifier(context, identifier, connection, out result))
            return true;

        if (AstQueryOracleDb2IdentifierEvaluator.TryResolveIdentifier(context, identifier, out result))
            return true;

        if (AstQueryPostgresIdentifierEvaluator.TryResolveIdentifier(context, identifier, out result))
            return true;

        result = null;
        return false;
    }

    private static bool TryResolveBoundScalarFunctionIdentifier(
        QueryExecutionContext context,
        IdentifierExpr identifier,
        DateTime evaluationLocalNow,
        DateTime evaluationUtcNow,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!dialect.TryGetScalarFunctionDefinition(identifier.Name, out var metadataDefinition)
            || metadataDefinition is null)
        {
            result = null;
            return false;
        }

        if (!metadataDefinition.AllowsIdentifier)
        {
            throw SqlUnsupported.NotSupported(dialect, identifier.Name.ToUpperInvariant());
        }

        if (metadataDefinition.AstExecutor is not null
            && metadataDefinition.AstExecutor(
                context,
                new FunctionCallExpr(identifier.Name, []),
                static _ => null,
                out var boundIdentifierValue))
        {
            result = boundIdentifierValue;
            return true;
        }

        if (metadataDefinition.TemporalKind is not null
            && context.TryEvaluateZeroArgIdentifier(
                identifier.Name,
                evaluationLocalNow,
                evaluationUtcNow,
                out var temporalIdentifierValue))
        {
            result = temporalIdentifierValue;
            return true;
        }

        result = null;
        return false;
    }
}
