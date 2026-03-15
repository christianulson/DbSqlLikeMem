namespace DbSqlLikeMem;

internal static class SqlTemporalFunctionEvaluator
{
    private static readonly HashSet<string> KnownTemporalFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "NOW",
        "SYSDATE",
        "SYSTEMDATE",
        "GETDATE",
        "GETUTCDATE",
        "SYSDATETIME",
        "SYSDATETIMEOFFSET",
        "SYSUTCDATETIME",
        "SYSTIMESTAMP",
    };

    /// <summary>
    /// EN: Checks whether the provided function name is a known temporal token/call across supported dialects.
    /// PT: Verifica se o nome informado é um token/chamada temporal conhecido entre os dialetos suportados.
    /// </summary>
    /// <param name="functionName">EN: Function/token name to inspect. PT: Nome da função/token a inspecionar.</param>
    /// <returns>EN: True when the name is recognized as temporal in at least one dialect. PT: True quando o nome é reconhecido como temporal em ao menos um dialeto.</returns>
    public static bool IsKnownTemporalFunctionName(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && KnownTemporalFunctionNames.Contains(functionName);

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
            SqlTemporalFunctionKind.DateTimeOffset => DateTimeOffset.Now,
            _ => utcNow,
        };

        return true;
    }
}
