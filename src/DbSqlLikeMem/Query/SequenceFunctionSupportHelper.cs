namespace DbSqlLikeMem;

internal static class SequenceFunctionSupportHelper
{
    internal static void EnsureSupported(ISqlDialect? dialect, string? functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return;

        var sqlDialect = dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para funções de sequence.");

        if (functionName!.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !sqlDialect.SupportsNextValueForSequenceExpression)
        {
            throw SqlUnsupported.ForDialect(sqlDialect, "NEXT VALUE FOR");
        }

        if (functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !sqlDialect.SupportsPreviousValueForSequenceExpression)
        {
            throw SqlUnsupported.ForDialect(sqlDialect, "PREVIOUS VALUE FOR");
        }

        if ((functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase))
            && !SupportsSequenceFunctionCall(sqlDialect, functionName))
        {
            throw SqlUnsupported.ForDialect(sqlDialect, functionName.ToUpperInvariant());
        }
    }

    private static bool SupportsSequenceFunctionCall(ISqlDialect dialect, string functionName)
    {
        if (dialect.SupportsSequenceFunctionCall(functionName))
            return true;

        if (dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            return functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase);
        }

        if (dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            return (functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                    || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase))
                && dialect.SupportsSequenceDotValueExpression(functionName);
        }

        return false;
    }
}
