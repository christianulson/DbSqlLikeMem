using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryExecutionRuntimeHelper
{
    private static readonly IReadOnlyDictionary<string, TemporalUnit> _temporalUnits = new Dictionary<string, TemporalUnit>(StringComparer.OrdinalIgnoreCase)
    {
        ["YEAR"] = TemporalUnit.Year,
        ["YEARS"] = TemporalUnit.Year,
        ["YY"] = TemporalUnit.Year,
        ["YYYY"] = TemporalUnit.Year,
        ["MONTH"] = TemporalUnit.Month,
        ["MONTHS"] = TemporalUnit.Month,
        ["MM"] = TemporalUnit.Month,
        ["DAY"] = TemporalUnit.Day,
        ["DAYS"] = TemporalUnit.Day,
        ["DD"] = TemporalUnit.Day,
        ["D"] = TemporalUnit.Day,
        ["WEEK"] = TemporalUnit.Week,
        ["WEEKDAY"] = TemporalUnit.Weekday,
        ["YEARDAY"] = TemporalUnit.Yearday,
        ["HOUR"] = TemporalUnit.Hour,
        ["HOURS"] = TemporalUnit.Hour,
        ["HH"] = TemporalUnit.Hour,
        ["MINUTE"] = TemporalUnit.Minute,
        ["MINUTES"] = TemporalUnit.Minute,
        ["MI"] = TemporalUnit.Minute,
        ["N"] = TemporalUnit.Minute,
        ["SECOND"] = TemporalUnit.Second,
        ["SECONDS"] = TemporalUnit.Second,
        ["SS"] = TemporalUnit.Second,
        ["S"] = TemporalUnit.Second,
        ["MILLISECOND"] = TemporalUnit.Millisecond,
        ["MICROSECOND"] = TemporalUnit.Microsecond,
        ["MICROSECONDS"] = TemporalUnit.Microsecond,
        ["MCS"] = TemporalUnit.Microsecond,
    };

    internal static TemporalUnit ResolveTemporalUnit(string unit)
    {
        if (string.IsNullOrWhiteSpace(unit))
            return TemporalUnit.Unknown;

        if (unit.IndexOfAny([' ', '\t', '\r', '\n']) < 0)
            return _temporalUnits.TryGetValue(unit, out var resolved)
                ? resolved
                : TemporalUnit.Unknown;

        var trimmed = unit.Trim();
        return trimmed.Length > 0 && _temporalUnits.TryGetValue(trimmed, out var resolvedTrimmed)
            ? resolvedTrimmed
            : TemporalUnit.Unknown;
    }

    internal static bool TryGetJsonAndPathArguments(
        Func<int, object?> evalArg,
        out object? json,
        out string? path)
    {
        json = evalArg(0);
        path = evalArg(1)?.ToString();
        return !IsNullish(json) && !string.IsNullOrWhiteSpace(path);
    }

    internal static void LogFunctionEvaluationFailure(Exception exception)
    {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine($"{nameof(AstQueryExecutionRuntimeHelper)}.{nameof(LogFunctionEvaluationFailure)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        Console.WriteLine(exception);
    }

    internal static bool TryCoerceDecimal(object? value, out decimal result)
    {
        result = default;

        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    internal static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
    {
        dt = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        switch (baseVal)
        {
            case DateTime d:
                dt = d;
                return true;
            case DateTimeOffset dto:
                dt = dto.DateTime;
                return true;
        }

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt);
    }

    internal static string GetDateAddUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var unit = expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(unit))
        {
            var evaluated = eval(expr, row, group, ctes);
            unit = evaluated?.ToString() ?? string.Empty;
        }

        return unit!.Trim().ToUpperInvariant();
    }

    internal static TemporalUnit GetTemporalUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
        => ResolveTemporalUnit(GetDateAddUnit(expr, row, group, ctes, eval));
}
