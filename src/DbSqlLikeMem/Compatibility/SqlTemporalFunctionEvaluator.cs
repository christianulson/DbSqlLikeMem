namespace DbSqlLikeMem;

internal static class SqlTemporalFunctionEvaluator
{
    public static bool TryEvaluateZeroArgIdentifier(ISqlDialect dialect, string functionName, out object? value)
    {
        value = null;
        if (dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!dialect.TemporalFunctionIdentifierNames.Any(n => n.Equals(functionName, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (!dialect.TemporalFunctionNames.TryGetValue(functionName, out var kind))
            return false;

        return TryMapKind(kind, out value);
    }

    public static bool TryEvaluateZeroArgCall(ISqlDialect dialect, string functionName, out object? value)
    {
        value = null;
        if (dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!dialect.TemporalFunctionCallNames.Any(n => n.Equals(functionName, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (!dialect.TemporalFunctionNames.TryGetValue(functionName, out var kind))
            return false;

        return TryMapKind(kind, out value);
    }

    private static bool TryMapKind(SqlTemporalFunctionKind kind, out object? value)
    {
        var utcNow = DateTime.UtcNow;
        value = kind switch
        {
            SqlTemporalFunctionKind.Date => utcNow.Date,
            SqlTemporalFunctionKind.Time => utcNow.TimeOfDay,
            _ => utcNow,
        };

        return true;
    }
}
