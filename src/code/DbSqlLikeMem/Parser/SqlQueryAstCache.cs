using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal sealed class SqlQueryAstCache
{
    private const int ParserCacheKeyVersion = 5;

    private readonly int _capacity;
    private readonly ConcurrentDictionary<string, SqlQueryBase> _entries;

    private SqlQueryAstCache(int capacity)
    {
        _capacity = capacity;
        _entries = new ConcurrentDictionary<string, SqlQueryBase>(StringComparer.Ordinal);
    }

    public static SqlQueryAstCache CreateFromEnvironment()
    {
        const int defaultCapacity = 256;
        var raw = Environment.GetEnvironmentVariable("DBSQLLIKEMEM_AST_CACHE_SIZE");
        if (string.IsNullOrWhiteSpace(raw))
            return new SqlQueryAstCache(defaultCapacity);

        if (!int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
            return new SqlQueryAstCache(defaultCapacity);

        return new SqlQueryAstCache(parsed);
    }

    public static string BuildKey(
        string sql,
        string dialectName,
        int dialectVersion,
        string parserCacheKeySuffix,
        int dbIdentity)
        => string.Concat(
            "p",
            ParserCacheKeyVersion.ToString(CultureInfo.InvariantCulture),
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
            NormalizeSql(sql));

    public bool TryGet(string key, out SqlQueryBase query)
    {
        if (_capacity <= 0)
        {
            query = null!;
            return false;
        }

        if (_entries.TryGetValue(key, out var cached))
        {
            query = cached;
            return true;
        }

        query = null!;
        return false;
    }

    public void Set(string key, SqlQueryBase query)
    {
        if (_capacity <= 0)
            return;

        _entries.TryAdd(key, query);

        if (_entries.Count > _capacity)
            TrimExcess();
    }

    public void Clear()
    {
        if (_capacity <= 0)
            return;

        _entries.Clear();
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

    internal static string NormalizeSql(string sql)
    {
        var trimmed = sql.Trim();
        if (trimmed.Length == 0)
            return string.Empty;

        var sb = new StringBuilder(trimmed.Length);
        var previousWhitespace = false;

        for (var i = 0; i < trimmed.Length; i++)
        {
            var ch = trimmed[i];
            if (char.IsWhiteSpace(ch))
            {
                if (previousWhitespace)
                    continue;

                sb.Append(' ');
                previousWhitespace = true;
                continue;
            }

            sb.Append(ch);
            previousWhitespace = false;
        }

        return sb.ToString();
    }
}
