namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
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
            cache.Clear();

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
