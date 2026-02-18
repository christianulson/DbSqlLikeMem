using System.Globalization;
using System.Collections.Generic;
using System.Text;

namespace DbSqlLikeMem;

internal sealed class SqlQueryAstCache
{
    private readonly int _capacity;
    private readonly object _gate = new();
    private readonly Dictionary<string, CacheEntry> _entries;
    private readonly LinkedList<string> _lru = [];

    private sealed class CacheEntry(SqlQueryBase query, LinkedListNode<string> node)
    {
        public SqlQueryBase Query { get; set; } = query;
        public LinkedListNode<string> Node { get; } = node;
    }

    private SqlQueryAstCache(int capacity)
    {
        _capacity = capacity;
        _entries = new Dictionary<string, CacheEntry>(capacity <= 0 ? 1 : capacity, StringComparer.Ordinal);
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

    public static string BuildKey(string sql, string dialectName)
        => string.Concat(dialectName, "::", NormalizeSql(sql));

    public bool TryGet(string key, out SqlQueryBase query)
    {
        if (_capacity <= 0)
        {
            query = null!;
            return false;
        }

        lock (_gate)
        {
            if (!_entries.TryGetValue(key, out var entry))
            {
                query = null!;
                return false;
            }

            _lru.Remove(entry.Node);
            _lru.AddFirst(entry.Node);
            query = entry.Query;
            return true;
        }
    }

    public void Set(string key, SqlQueryBase query)
    {
        if (_capacity <= 0)
            return;

        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                entry.Query = query;
                _lru.Remove(entry.Node);
                _lru.AddFirst(entry.Node);
                return;
            }

            var node = new LinkedListNode<string>(key);
            _lru.AddFirst(node);
            _entries[key] = new CacheEntry(query, node);

            if (_entries.Count <= _capacity)
                return;

            var tail = _lru.Last;
            if (tail is null)
                return;

            _lru.RemoveLast();
            _entries.Remove(tail.Value);
        }
    }

    private static string NormalizeSql(string sql)
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
