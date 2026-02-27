namespace DbSqlLikeMem;

internal static class SqlTemporalFunctionEvaluator
{
    public static bool TryEvaluateZeroArgFunction(ISqlDialect dialect, string functionName, out object? value)
    {
        value = null;
        if (dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!dialect.TemporalFunctionNames.TryGetValue(functionName, out var kind))
            return false;

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
