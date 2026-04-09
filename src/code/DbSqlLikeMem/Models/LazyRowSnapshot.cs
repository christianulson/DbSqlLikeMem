namespace DbSqlLikeMem;

/// <summary>
/// EN: Stores a row snapshot in a compact read-only form without allocating a dictionary.
/// PT: Armazena um snapshot de linha em formato compacto somente leitura sem alocar um dicionário.
/// </summary>
internal sealed class LazyRowSnapshot : IReadOnlyDictionary<int, object?>
{
    private static readonly LazyRowSnapshot EmptySnapshot = new(Array.Empty<KeyValuePair<int, object?>>());

    private readonly KeyValuePair<int, object?>[] _entries;

    private LazyRowSnapshot(KeyValuePair<int, object?>[] entries)
    {
        _entries = entries;
    }

    /// <summary>
    /// EN: Creates a read-only snapshot for the provided row.
    /// PT: Cria um snapshot somente leitura para a linha informada.
    /// </summary>
    /// <param name="row">EN: Source row. PT: Linha de origem.</param>
    /// <returns>EN: Compact read-only snapshot. PT: Snapshot compacto somente leitura.</returns>
    public static IReadOnlyDictionary<int, object?> From(IReadOnlyDictionary<int, object?>? row)
    {
        if (row is null || row.Count == 0)
            return EmptySnapshot;

        if (row is LazyRowSnapshot snapshot)
            return snapshot;

        var entries = new KeyValuePair<int, object?>[row.Count];
        var index = 0;
        foreach (var item in row)
            entries[index++] = item;

        if (index == 0)
            return EmptySnapshot;

        if (index != entries.Length)
            Array.Resize(ref entries, index);

        return new LazyRowSnapshot(entries);
    }

    /// <inheritdoc />
    public object? this[int key]
    {
        get
        {
            foreach (var entry in _entries)
            {
                if (entry.Key == key)
                    return entry.Value;
            }

            throw new KeyNotFoundException($"The given key '{key}' was not present in the row snapshot.");
        }
    }

    /// <inheritdoc />
    public IEnumerable<int> Keys
    {
        get
        {
            foreach (var entry in _entries)
                yield return entry.Key;
        }
    }

    /// <inheritdoc />
    public IEnumerable<object?> Values
    {
        get
        {
            foreach (var entry in _entries)
                yield return entry.Value;
        }
    }

    /// <inheritdoc />
    public int Count => _entries.Length;

    /// <inheritdoc />
    public bool ContainsKey(int key)
    {
        foreach (var entry in _entries)
        {
            if (entry.Key == key)
                return true;
        }

        return false;
    }

    /// <inheritdoc />
    public bool TryGetValue(int key, out object? value)
    {
        foreach (var entry in _entries)
        {
            if (entry.Key == key)
            {
                value = entry.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    /// <inheritdoc />
    public IEnumerator<KeyValuePair<int, object?>> GetEnumerator()
        => ((IEnumerable<KeyValuePair<int, object?>>)_entries).GetEnumerator();

    /// <inheritdoc />
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
