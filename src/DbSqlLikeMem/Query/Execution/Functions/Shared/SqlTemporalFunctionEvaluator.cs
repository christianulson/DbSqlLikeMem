namespace DbSqlLikeMem;

internal static class SqlTemporalFunctionEvaluator
{
    private static readonly HashSet<string> KnownTemporalFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "CURRENT_DATE",
        "CURRENT DATE",
        "CURDATE",
        "CURRENT_TIME",
        "CURRENT TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT TIMESTAMP",
        "CURTIME",
        "NOW",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "UTC_DATE",
        "UTC_TIME",
        "UTC_TIMESTAMP",
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

    /// <summary>
    /// EN: Checks whether the provided temporal name is known by the current dialect or by compatibility fallback.
    /// PT: Verifica se o nome temporal informado é conhecido pelo dialeto atual ou pelo fallback de compatibilidade.
    /// </summary>
    /// <param name="dialect">EN: Dialect used to inspect registry-backed temporal support. PT: Dialeto usado para inspecionar suporte temporal baseado em registry.</param>
    /// <param name="functionName">EN: Function/token name to inspect. PT: Nome da função/token a inspecionar.</param>
    /// <returns>EN: True when the name is recognized as temporal in the dialect or compatibility list. PT: True quando o nome é reconhecido como temporal no dialeto ou na lista de compatibilidade.</returns>
    public static bool IsKnownTemporalFunctionName(ISqlDialect dialect, string functionName)
        => dialect is not null
            && !string.IsNullOrWhiteSpace(functionName)
            && (dialect.AllowsTemporalIdentifier(functionName)
                || dialect.AllowsTemporalCall(functionName)
                || IsKnownTemporalFunctionName(functionName));

    public static bool TryEvaluateZeroArgIdentifier(
        ISqlDialect dialect,
        string functionName,
        DateTime localNow,
        DateTime utcNow,
        out object? value)
    {
        value = null;
        if (dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!dialect.AllowsTemporalIdentifier(functionName))
            return false;

        if (!dialect.TryGetTemporalFunctionKind(functionName, out var kind))
            return false;

        return TryMapKind(kind, localNow, utcNow, out value);
    }

    public static bool TryEvaluateZeroArgIdentifier(ISqlDialect dialect, string functionName, out object? value)
        => TryEvaluateZeroArgIdentifier(dialect, functionName, DateTime.Now, DateTime.UtcNow, out value);

    public static bool TryEvaluateZeroArgCall(
        ISqlDialect dialect,
        string functionName,
        DateTime localNow,
        DateTime utcNow,
        out object? value)
    {
        value = null;
        if (dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!dialect.AllowsTemporalCall(functionName))
            return false;

        if (!dialect.TryGetTemporalFunctionKind(functionName, out var kind))
            return false;

        return TryMapKind(kind, localNow, utcNow, out value);
    }

    public static bool TryEvaluateZeroArgCall(ISqlDialect dialect, string functionName, out object? value)
        => TryEvaluateZeroArgCall(dialect, functionName, DateTime.Now, DateTime.UtcNow, out value);

    private static bool TryMapKind(SqlTemporalFunctionKind kind, DateTime localNow, DateTime utcNow, out object? value)
    {
        value = kind switch
        {
            SqlTemporalFunctionKind.Date => utcNow.Date,
            SqlTemporalFunctionKind.Time => utcNow.TimeOfDay,
            SqlTemporalFunctionKind.DateTimeOffset => new DateTimeOffset(localNow),
            _ => utcNow,
        };

        return true;
    }
}
