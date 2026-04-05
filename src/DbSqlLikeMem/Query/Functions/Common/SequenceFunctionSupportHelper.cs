namespace DbSqlLikeMem;

internal static class SequenceFunctionSupportHelper
{
    internal static void EnsureSupported(ISqlDialect? dialect, string? functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return;

        var name = functionName!;
        var sqlDialect = dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para funções de sequence.");

        if (name.Equals("GEN_ID", StringComparison.OrdinalIgnoreCase))
        {
            if (!sqlDialect.SupportsSequenceFunctionCall(name))
                throw SqlUnsupported.NotSupported(sqlDialect, name.ToUpperInvariant());

            return;
        }

        if (name.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !sqlDialect.SupportsNextValueForSequenceExpression)
        {
            throw SqlUnsupported.NotSupported(sqlDialect, "NEXT VALUE FOR");
        }

        if (name.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !sqlDialect.SupportsPreviousValueForSequenceExpression)
        {
            throw SqlUnsupported.NotSupported(sqlDialect, "PREVIOUS VALUE FOR");
        }

        if (!sqlDialect.TryGetScalarFunctionDefinition(name, out var definition)
            || definition is null
            || !definition.AllowsCall)
        {
            throw SqlUnsupported.NotSupported(sqlDialect, name.ToUpperInvariant());
        }
    }
}
