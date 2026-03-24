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

        if (!sqlDialect.TryGetScalarFunctionDefinition(functionName, out var definition)
            || definition is null
            || !definition.AllowsCall)
        {
            throw SqlUnsupported.ForDialect(sqlDialect, functionName.ToUpperInvariant());
        }
    }
}
