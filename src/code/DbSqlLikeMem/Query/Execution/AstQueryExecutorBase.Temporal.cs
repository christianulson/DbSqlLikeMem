namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private IntervalValue? ParseIntervalValue(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        if (TryParseSplitIntervalArguments(fn, row, group, ctes, out var splitValue, out var splitUnit))
        {
            var splitSpan = TryConvertIntervalToTimeSpan(splitValue, splitUnit);
            return splitSpan is null ? null : new IntervalValue(splitSpan.Value);
        }

        var raw = Eval(fn.Args[0], row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        if (!TryParseIntervalLiteral(raw!, out var value, out var unit))
            return null;

        var span = TryConvertIntervalToTimeSpan(value, unit);
        return span is null ? null : new IntervalValue(span.Value);
    }

    private bool TryParseSplitIntervalArguments(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out decimal value,
        out TemporalUnit unit)
    {
        value = 0m;
        unit = TemporalUnit.Unknown;

        if (fn.Args.Count < 2)
            return false;

        unit = AstQueryExecutionRuntimeHelper.GetTemporalUnit(fn.Args[1], row, group, ctes, Eval);
        if (unit == TemporalUnit.Unknown)
            return false;

        var rawValue = Eval(fn.Args[0], row, group, ctes);
        if (rawValue is null || rawValue is DBNull)
            return false;

        if (rawValue is decimal dec)
        {
            value = dec;
            return true;
        }

        if (!decimal.TryParse(rawValue.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out value))
            return false;

        return true;
    }

    private static bool TryParseIntervalLiteral(string raw, out decimal value, out TemporalUnit unit)
    {
        value = 0;
        unit = TemporalUnit.Unknown;

        var normalized = raw.Trim();
        if (normalized.Contains('\\'))
            normalized = normalized.Replace("\\", string.Empty);

        var match = _intervalLiteralRegex.Match(normalized);
        if (!match.Success)
            return false;

        if (!decimal.TryParse(match.Groups["num"].Value, NumberStyles.Any, CultureInfo.InvariantCulture, out value))
            return false;

        unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    internal static DateTime ApplyDateDelta(DateTime dt, TemporalUnit unit, int amount) => unit switch
    {
        TemporalUnit.Year => dt.AddYears(amount),
        TemporalUnit.Month => dt.AddMonths(amount),
        TemporalUnit.Week => dt.AddDays(amount * 7L),
        TemporalUnit.Day => dt.AddDays(amount),
        TemporalUnit.Weekday => dt.AddDays(amount),
        TemporalUnit.Yearday => dt.AddDays(amount),
        TemporalUnit.Hour => dt.AddHours(amount),
        TemporalUnit.Minute => dt.AddMinutes(amount),
        TemporalUnit.Second => dt.AddSeconds(amount),
        TemporalUnit.Millisecond => dt.AddMilliseconds(amount),
        TemporalUnit.Microsecond => dt.AddTicks(amount * 10L),
        TemporalUnit.Nanosecond => dt.AddTicks((long)Math.Round(amount / 100m, MidpointRounding.AwayFromZero)),
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
        TemporalUnit.Millisecond => new DateTime(dateTime.Year, dateTime.Month, dateTime.Day, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond, dateTime.Kind),
        TemporalUnit.Microsecond => new DateTime(dateTime.Ticks - (dateTime.Ticks % 10), dateTime.Kind),
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
        TemporalUnit.Millisecond => dateTime.Millisecond,
        TemporalUnit.Microsecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) / 10L),
        TemporalUnit.Nanosecond => (int)((dateTime.Ticks % TimeSpan.TicksPerSecond) * 100L),
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

        unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(match.Groups["unit"].Value);
        return unit != TemporalUnit.Unknown;
    }

    private static TimeSpan? TryConvertIntervalToTimeSpan(decimal value, TemporalUnit unit)
        => unit switch
        {
            TemporalUnit.Day => TimeSpan.FromDays((double)value),
            TemporalUnit.Hour => TimeSpan.FromHours((double)value),
            TemporalUnit.Minute => TimeSpan.FromMinutes((double)value),
            TemporalUnit.Second => TimeSpan.FromSeconds((double)value),
            TemporalUnit.Microsecond => TimeSpan.FromTicks((long)decimal.Truncate(value * 10m)),
            TemporalUnit.Nanosecond => TimeSpan.FromTicks((long)Math.Round(value / 100m, MidpointRounding.AwayFromZero)),
            _ => (TimeSpan?)null
        };

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
            && TryParseCachedDateTime(text, DateTimeStyles.AssumeLocal, out dt);
    }

    internal static bool TryCoerceTimeSpan(object? baseVal, out TimeSpan span)
    {
        span = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        if (baseVal is TimeSpan ts)
        {
            span = ts;
            return true;
        }

        if (baseVal is DateTime dt)
        {
            span = dt.TimeOfDay;
            return true;
        }

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && TryParseCachedTimeSpan(text, out span);
    }

    internal static bool TryParseCachedDateTime(string text, DateTimeStyles styles, out DateTime dt)
    {
        var cacheKey = BuildDateTimeParseCacheKey(text, styles);
        if (_dateTimeParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParse(
            text,
            CultureInfo.InvariantCulture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseCachedDateTime(string text, CultureInfo culture, DateTimeStyles styles, out DateTime dt)
    {
        if (string.IsNullOrEmpty(culture.Name))
            return TryParseCachedDateTime(text, styles, out dt);

        var cacheKey = BuildDateTimeParseCacheKey(text, culture.Name, styles);
        if (_dateTimeParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParse(
            text,
            culture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseExactCachedDateTime(string text, string format, DateTimeStyles styles, out DateTime dt)
    {
        var cacheKey = BuildExactDateTimeParseCacheKey(text, format, styles);
        if (_dateTimeExactParseCache.TryGetValue(cacheKey, out var cached))
        {
            dt = cached.Value;
            return cached.Success;
        }

        var success = DateTime.TryParseExact(
            text,
            format,
            CultureInfo.InvariantCulture,
            styles,
            out dt);

        CacheTemporalParseEntry(_dateTimeExactParseCache, cacheKey, new DateTimeParseCacheEntry(success, dt));
        return success;
    }

    internal static bool TryParseCachedDateTimeOffset(string text, DateTimeStyles styles, out DateTimeOffset dto)
    {
        var cacheKey = BuildDateTimeOffsetParseCacheKey(text, styles);
        if (_dateTimeOffsetParseCache.TryGetValue(cacheKey, out var cached))
        {
            dto = cached.Value;
            return cached.Success;
        }

        var success = DateTimeOffset.TryParse(
            text,
            CultureInfo.InvariantCulture,
            styles,
            out dto);

        CacheTemporalParseEntry(_dateTimeOffsetParseCache, cacheKey, new DateTimeOffsetParseCacheEntry(success, dto));
        return success;
    }

    internal static bool TryParseExactCachedDateTimeOffset(string text, string format, DateTimeStyles styles, out DateTimeOffset dto)
    {
        var cacheKey = BuildExactDateTimeOffsetParseCacheKey(text, format, styles);
        if (_dateTimeOffsetExactParseCache.TryGetValue(cacheKey, out var cached))
        {
            dto = cached.Value;
            return cached.Success;
        }

        var success = DateTimeOffset.TryParseExact(
            text,
            format,
            CultureInfo.InvariantCulture,
            styles,
            out dto);

        CacheTemporalParseEntry(_dateTimeOffsetExactParseCache, cacheKey, new DateTimeOffsetParseCacheEntry(success, dto));
        return success;
    }

    private static bool TryParseCachedTimeSpan(string text, out TimeSpan span)
    {
        if (_timeSpanParseCache.TryGetValue(text, out var cached))
        {
            span = cached.Value;
            return cached.Success;
        }

        var success = false;
        span = default;

        if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed))
        {
            span = parsed;
            success = true;
        }
        else if (TryParseCachedDateTime(text, DateTimeStyles.AssumeLocal, out var parsedDate))
        {
            span = parsedDate.TimeOfDay;
            success = true;
        }

        CacheTemporalParseEntry(_timeSpanParseCache, text, new TimeSpanParseCacheEntry(success, span));
        return success;
    }

    private static void CacheTemporalParseEntry<TEntry>(
        System.Collections.Concurrent.ConcurrentDictionary<string, TEntry> cache,
        string text,
        TEntry entry)
    {
        if (cache.Count >= TemporalParseCacheSoftLimit)
        {
            var trimCount = Math.Max(TemporalParseCacheSoftLimit / 4, 1);
            var removed = 0;
            foreach (var key in cache.Keys)
            {
                if (cache.TryRemove(key, out _))
                    removed++;

                if (removed >= trimCount)
                    break;
            }
        }

        cache[text] = entry;
    }

    private static string BuildDateTimeParseCacheKey(string text, DateTimeStyles styles)
        => $"{(int)styles}:{text}";

    private static string BuildDateTimeParseCacheKey(string text, string cultureName, DateTimeStyles styles)
        => $"{cultureName}:{(int)styles}:{text}";

    private static string BuildDateTimeOffsetParseCacheKey(string text, DateTimeStyles styles)
        => $"{(int)styles}:{text}";

    private static string BuildExactDateTimeParseCacheKey(string text, string format, DateTimeStyles styles)
        => $"{(int)styles}:{format}:{text}";

    private static string BuildExactDateTimeOffsetParseCacheKey(string text, string format, DateTimeStyles styles)
        => $"{(int)styles}:{format}:{text}";

    /// <summary>
    /// EN: Detects time-only literals and avoids treating them like date-time values.
    /// PT: Detecta literais apenas de horario e evita trata-los como valores de data e hora.
    /// </summary>
    internal static bool LooksLikeTimeOnly(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
            return false;

        if (trimmed.Contains('T') || trimmed.Contains('t'))
            return false;

        if (trimmed.Contains('-') || trimmed.Contains('/'))
            return false;

        return trimmed.Contains(':');
    }
}
