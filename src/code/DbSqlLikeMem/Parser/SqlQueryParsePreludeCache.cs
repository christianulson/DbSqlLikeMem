using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal sealed class SqlQueryParsePreludeCache
{
    private const int PreludeCacheKeyVersion = 4;

    private readonly int _capacity;
    private readonly ConcurrentDictionary<string, Prelude> _entries;

    internal readonly record struct Prelude(IReadOnlyList<SqlToken> Tokens, AutoSqlSyntaxFeatures AutoSyntaxFeatures);

    private SqlQueryParsePreludeCache(int capacity)
    {
        _capacity = capacity;
        _entries = new ConcurrentDictionary<string, Prelude>(StringComparer.Ordinal);
    }

    public static SqlQueryParsePreludeCache CreateFromEnvironment()
    {
        const int defaultCapacity = 256;
        var raw = Environment.GetEnvironmentVariable("DBSQLLIKEMEM_PARSE_PRELUDE_CACHE_SIZE");
        if (string.IsNullOrWhiteSpace(raw))
            return new SqlQueryParsePreludeCache(defaultCapacity);

        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
            return new SqlQueryParsePreludeCache(defaultCapacity);

        return new SqlQueryParsePreludeCache(parsed);
    }

    public static string BuildKey(
        string sql,
        string dialectName,
        int dialectVersion,
        string parserCacheKeySuffix,
        int dbIdentity)
        => string.Concat(
            "t",
            PreludeCacheKeyVersion.ToString(CultureInfo.InvariantCulture),
            "::",
            dialectName,
            "::v",
            dialectVersion.ToString(CultureInfo.InvariantCulture),
            "::",
            parserCacheKeySuffix,
            "::",
            "db",
            dbIdentity.ToString(CultureInfo.InvariantCulture),
            "::",
            SqlQueryAstCache.NormalizeSql(sql));

    public bool TryGet(string key, out Prelude prelude)
    {
        if (_capacity <= 0)
        {
            prelude = default;
            return false;
        }

        return _entries.TryGetValue(key, out prelude);
    }

    public void Set(string key, Prelude prelude)
    {
        if (_capacity <= 0)
            return;

        _entries.TryAdd(key, prelude);

        if (_entries.Count > _capacity)
            TrimExcess();
    }

    private void TrimExcess()
    {
        var removeCount = _entries.Count - _capacity;
        if (removeCount <= 0)
            return;

        foreach (var kvp in _entries)
        {
            if (removeCount <= 0)
                break;

            if (_entries.TryRemove(kvp.Key, out _))
                removeCount--;
        }
    }
}
