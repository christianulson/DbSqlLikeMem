namespace DbSqlLikeMem;

internal sealed class SqlQueryParsePreludeCache
{
    private const int PreludeCacheKeyVersion = 1;

    private readonly int _capacity;
    private readonly object _gate = new();
    private readonly Dictionary<string, CacheEntry> _entries;
    private readonly LinkedList<string> _lru = [];

    internal readonly record struct Prelude(IReadOnlyList<SqlToken> Tokens, AutoSqlSyntaxFeatures AutoSyntaxFeatures);

    private sealed class CacheEntry(Prelude prelude, LinkedListNode<string> node)
    {
        public Prelude Prelude { get; set; } = prelude;
        public LinkedListNode<string> Node { get; } = node;
    }

    private SqlQueryParsePreludeCache(int capacity)
    {
        _capacity = capacity;
        _entries = new Dictionary<string, CacheEntry>(capacity <= 0 ? 1 : capacity, StringComparer.Ordinal);
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

    public static string BuildKey(string sql, string dialectName, int dialectVersion)
        => string.Concat(
            "t",
            PreludeCacheKeyVersion.ToString(CultureInfo.InvariantCulture),
            "::",
            dialectName,
            "::v",
            dialectVersion.ToString(CultureInfo.InvariantCulture),
            "::",
            SqlQueryAstCache.NormalizeSql(sql));

    public bool TryGet(string key, out Prelude prelude)
    {
        if (_capacity <= 0)
        {
            prelude = default;
            return false;
        }

        lock (_gate)
        {
            if (!_entries.TryGetValue(key, out var entry))
            {
                prelude = default;
                return false;
            }

            _lru.Remove(entry.Node);
            _lru.AddFirst(entry.Node);
            prelude = entry.Prelude;
            return true;
        }
    }

    public void Set(string key, Prelude prelude)
    {
        if (_capacity <= 0)
            return;

        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                entry.Prelude = prelude;
                _lru.Remove(entry.Node);
                _lru.AddFirst(entry.Node);
                return;
            }

            var node = new LinkedListNode<string>(key);
            _lru.AddFirst(node);
            _entries[key] = new CacheEntry(prelude, node);

            if (_entries.Count <= _capacity)
                return;

            var tail = _lru.Last;
            if (tail is null)
                return;

            _lru.RemoveLast();
            _entries.Remove(tail.Value);
        }
    }
}
