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
    /// <returns>EN: True when the name is recognized as temporal in at least one context.Dialect. PT: True quando o nome é reconhecido como temporal em ao menos um dialeto.</returns>
    public static bool IsKnownTemporalFunctionName(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && KnownTemporalFunctionNames.Contains(functionName);

    /// <summary>
    /// EN: Checks whether the provided temporal name is known by the current context.Dialect or by compatibility fallback.
    /// PT: Verifica se o nome temporal informado é conhecido pelo dialeto atual ou pelo fallback de compatibilidade.
    /// </summary>
    /// <param name="context.Dialect">EN: context.Dialect used to inspect registry-backed temporal support. PT: Dialeto usado para inspecionar suporte temporal baseado em registry.</param>
    /// <param name="functionName">EN: Function/token name to inspect. PT: Nome da função/token a inspecionar.</param>
    /// <returns>EN: True when the name is recognized as temporal in the context.Dialect or compatibility list. PT: True quando o nome é reconhecido como temporal no dialeto ou na lista de compatibilidade.</returns>
    public static bool IsKnownTemporalFunctionName(QueryExecutionContext context, string functionName)
        => context.Dialect is not null
            && !string.IsNullOrWhiteSpace(functionName)
            && (context.Dialect.AllowsTemporalIdentifier(functionName)
                || context.Dialect.AllowsTemporalCall(functionName)
                || IsKnownTemporalFunctionName(functionName));

    public static bool TryEvaluateZeroArgIdentifier(
        QueryExecutionContext context,
        string functionName,
        DateTime localNow,
        DateTime utcNow,
        out object? value)
    {
        value = null;
        if (context.Dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!context.Dialect.AllowsTemporalIdentifier(functionName))
            return false;

        if (!context.Dialect.TryGetTemporalFunctionKind(functionName, out var kind))
            return false;

        return TryMapKind(kind, localNow, utcNow, out value);
    }

    public static bool TryEvaluateZeroArgIdentifier(QueryExecutionContext context, string functionName, out object? value)
        => TryEvaluateZeroArgIdentifier(context, functionName, DateTime.Now, DateTime.UtcNow, out value);

    public static bool TryEvaluateZeroArgCall(
        QueryExecutionContext context,
        string functionName,
        DateTime localNow,
        DateTime utcNow,
        out object? value)
    {
        value = null;
        if (context.Dialect is null || string.IsNullOrWhiteSpace(functionName))
            return false;

        if (!context.Dialect.AllowsTemporalCall(functionName))
            return false;

        if (!context.Dialect.TryGetTemporalFunctionKind(functionName, out var kind))
            return false;

        return TryMapKind(kind, localNow, utcNow, out value);
    }

    public static bool TryEvaluateZeroArgCall(QueryExecutionContext context, string functionName, out object? value)
        => TryEvaluateZeroArgCall(context, functionName, DateTime.Now, DateTime.UtcNow, out value);

    internal static bool TryParseOffset(string value, out TimeSpan offset)
    {
        offset = default;

        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return false;

        if (string.Equals(trimmed, "UTC", StringComparison.OrdinalIgnoreCase))
        {
            offset = TimeSpan.Zero;
            return true;
        }

        if (TimeSpan.TryParse(trimmed, CultureInfo.InvariantCulture, out offset))
            return true;

        if (DateTime.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            offset = parsedDate.TimeOfDay;
            return true;
        }

        if (trimmed.Length == 6
            && (trimmed[0] == '+' || trimmed[0] == '-')
            && trimmed[3] == ':')
        {
            if (int.TryParse(trimmed[1..3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours)
                && int.TryParse(trimmed[4..6], NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (trimmed[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        if (trimmed.Length == 5 && (trimmed[0] == '+' || trimmed[0] == '-'))
        {
            if (int.TryParse(trimmed[1..3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours)
                && int.TryParse(trimmed[3..5], NumberStyles.Integer, CultureInfo.InvariantCulture, out var minutes))
            {
                offset = new TimeSpan(hours, minutes, 0);
                if (trimmed[0] == '-')
                    offset = -offset;
                return true;
            }
        }

        return false;
    }

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
