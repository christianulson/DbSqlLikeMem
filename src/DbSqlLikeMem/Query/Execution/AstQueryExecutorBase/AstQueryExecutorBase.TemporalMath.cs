namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    internal static DateTime ApplyDateDelta(DateTime dt, TemporalUnit unit, int amount) => unit switch
    {
        TemporalUnit.Year => dt.AddYears(amount),
        TemporalUnit.Month => dt.AddMonths(amount),
        TemporalUnit.Day => dt.AddDays(amount),
        TemporalUnit.Hour => dt.AddHours(amount),
        TemporalUnit.Minute => dt.AddMinutes(amount),
        TemporalUnit.Second => dt.AddSeconds(amount),
        _ => dt
    };

    internal static DateTime TruncateDateTime(DateTime dateTime, TemporalUnit unit) => unit switch
    {
        TemporalUnit.Year => new DateTime(dateTime.Year, 1, 1),
        TemporalUnit.Month => new DateTime(dateTime.Year, dateTime.Month, 1),
        TemporalUnit.Day => dateTime.Date,
        TemporalUnit.Hour => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, 0, 0),
        TemporalUnit.Minute => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, 0),
        TemporalUnit.Second => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second),
        _ => dateTime
    };

    internal static int? GetTemporalPartValue(DateTime dateTime, TemporalUnit unit) => unit switch
    {
        TemporalUnit.Year => dateTime.Year,
        TemporalUnit.Month => dateTime.Month,
        TemporalUnit.Day => dateTime.Day,
        TemporalUnit.Hour => dateTime.Hour,
        TemporalUnit.Minute => dateTime.Minute,
        TemporalUnit.Second => dateTime.Second,
        _ => null
    };

    internal static bool TryParseDateModifier(string modifier, out TemporalUnit unit, out int amount)
    {
        unit = TemporalUnit.Unknown;
        amount = 0;

        var match = _dateModifierRegex.Match(modifier.Trim());
        if (!match.Success)
            return false;

        if (!int.TryParse(match.Groups["amount"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        unit = ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    private static TimeSpan? TryConvertIntervalToTimeSpan(decimal value, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Day => TimeSpan.FromDays((double)value),
            TemporalUnit.Hour => TimeSpan.FromHours((double)value),
            TemporalUnit.Minute => TimeSpan.FromMinutes((double)value),
            TemporalUnit.Second => TimeSpan.FromSeconds((double)value),
            _ => (TimeSpan?)null
        };
}
