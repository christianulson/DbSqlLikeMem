namespace DbSqlLikeMem;

public sealed record Idx(
    string Name,
    IReadOnlyList<string> KeyCols,
    bool Unique,
    IReadOnlyList<string> Include
    );

/// <summary>
/// EN: Represents an index definition, including key columns and uniqueness.
/// PT: Representa a definição de um índice, incluindo colunas-chave e unicidade.
/// </summary>
public class IndexDef : IReadOnlyDictionary<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>
{
    /// <summary>
    /// EN: Initializes the index definition.
    /// PT: Inicializa a definição do índice.
    /// </summary>
    /// <param name="table">EN: Parent table. PT: Tabela pai.</param>
    /// <param name="name">EN: Index name. PT: Nome do índice.</param>
    /// <param name="keyCols">EN: Index key columns. PT: Colunas chave do índice.</param>
    /// <param name="include">EN: Additional included columns. PT: Colunas incluídas adicionais.</param>
    /// <param name="unique">EN: Whether the index is unique. PT: Indica se o índice é único.</param>
    internal IndexDef(
        ITableMock table,
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false)
    {
        Table = table;
        Name = name ?? throw new ArgumentNullException(nameof(name));
        KeyCols = [.. keyCols ?? throw new ArgumentNullException(nameof(keyCols))];
        if (KeyCols.Count == 0) throw new ArgumentException("At least one key column is required", nameof(keyCols));
        Unique = unique;
        Include = include ?? [];
        RebuildIndex();
    }

    /// <summary>
    /// EN: Parent table.
    /// PT: Tabela pai.
    /// </summary>
    public ITableMock Table { get; private set; }

    /// <summary>
    /// EN: Gets the index name.
    /// PT: Obtém o nome do índice.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// EN: Gets the columns that compose the index key.
    /// PT: Obtém as colunas que compõem a chave do índice.
    /// </summary>
    public IReadOnlyList<string> KeyCols { get; }

    /// <summary>
    /// EN: Indicates whether the index is unique.
    /// PT: Indica se o índice é único.
    /// </summary>
    public bool Unique { get; }

    /// <summary>
    /// EN: Gets columns included in the index that are not part of the key.
    /// PT: Obtém colunas incluídas no índice, mas não fazem parte da chave.
    /// </summary>
    public IReadOnlyList<string> Include { get; }


    private readonly Dictionary<string, Dictionary<int, Dictionary<string, object?>>> _items = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BucketReadOnlyView> _readonlyBuckets = new(StringComparer.OrdinalIgnoreCase);

    private IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> AsReadOnlyBucket(
        string key,
        Dictionary<int, Dictionary<string, object?>> bucket)
    {
        if (_readonlyBuckets.TryGetValue(key, out var cached))
            return cached;

        var view = new BucketReadOnlyView(bucket);
        _readonlyBuckets[key] = view;
        return view;
    }

    private void RemoveBucketIfEmpty(string key, Dictionary<int, Dictionary<string, object?>> bucket)
    {
        if (bucket.Count > 0)
            return;

        _items.Remove(key);
        _readonlyBuckets.Remove(key);
    }

    private sealed class BucketReadOnlyView(Dictionary<int, Dictionary<string, object?>> source)
        : IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>
    {
        private readonly Dictionary<int, Dictionary<string, object?>> _source = source;

        public int Count => _source.Count;

        public IEnumerable<int> Keys => _source.Keys;

        public IEnumerable<IReadOnlyDictionary<string, object?>> Values
            => _source.Values.Select(static row => (IReadOnlyDictionary<string, object?>)row);

        public IReadOnlyDictionary<string, object?> this[int key] => _source[key];

        public bool ContainsKey(int key) => _source.ContainsKey(key);

        public bool TryGetValue(int key, out IReadOnlyDictionary<string, object?> value)
        {
            if (_source.TryGetValue(key, out var row))
            {
                value = row;
                return true;
            }

            value = default!;
            return false;
        }

        public IEnumerator<KeyValuePair<int, IReadOnlyDictionary<string, object?>>> GetEnumerator()
            => _source.Select(static item => new KeyValuePair<int, IReadOnlyDictionary<string, object?>>(item.Key, item.Value)).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public IEnumerable<string> Keys => _items.Keys;

    public IEnumerable<IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>> Values
    {
        get
        {
            foreach (var item in _items)
                yield return AsReadOnlyBucket(item.Key, item.Value);
        }
    }

    public int Count => _items.Count;

    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> this[string key]
        => AsReadOnlyBucket(key, _items[key]);

    public bool ContainsKey(string key)
        => _items.ContainsKey(key);

    public bool TryGetValue(string key, out IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> value)
    {
        if (!_items.TryGetValue(key, out var v))
        {
            value = default!;
            return false;
        }
        value = AsReadOnlyBucket(key, v);
        return true;
    }

    public IEnumerator<KeyValuePair<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>> GetEnumerator()
    {
        foreach (var item in _items)
            yield return new KeyValuePair<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>(
                item.Key,
                AsReadOnlyBucket(item.Key, item.Value));
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    internal void RebuildIndex()
    {
        _items.Clear();
        _readonlyBuckets.Clear();
        var tableColumns = Table.Columns;
        var pkColumnsByIndex = tableColumns.ToDictionary(_ => _.Value.Index, _ => _.Key);
        for (int i = 0; i < Table.Count; i++)
        {
            var row = Table[i];
            var key = BuildIndexKey(row);

            if (!_items.TryGetValue(key, out var lstItems))
            {
                lstItems = [];
                _items.Add(key, lstItems);
            }
            else if (Unique)
                throw Table.DuplicateKey(Table.TableName, Name, $"{key} ({string.Join(",", KeyCols)})");

            var idxRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            lstItems.Add(i, idxRow);
            foreach (var item in KeyCols)
            {
                var col = Table.GetColumn(item);
                idxRow.Add(item!, row[col.Index]);
            }
            foreach (var item in Include)
            {
                var col = Table.GetColumn(item);
                idxRow.Add(item!, row[col.Index]);
            }

            foreach (var pkIdx in Table.PrimaryKeyIndexes)
            {
                if (!pkColumnsByIndex.TryGetValue(pkIdx, out var pkName)
                    || idxRow.ContainsKey(pkName))
                    continue;
                idxRow.Add(pkName, row[pkIdx]);
            }
        }
    }

    internal string BuildIndexKey(
        IReadOnlyDictionary<int, object?> row)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(row, nameof(row));
        return string.Concat(KeyCols.Select(colName =>
        {
            var ci = Table.Columns[colName];
            if (ci.GetGenValue != null
                && !ci.PersistComputedValue)
                return SerializeIndexKeyPart(ci.GetGenValue(row, Table));

            var value = row.TryGetValue(ci.Index, out var v) ? v : null;
            return SerializeIndexKeyPart(value);
        }));
    }

    internal string BuildIndexKeyFromValues(
        IReadOnlyDictionary<string, object?> valuesByColumn)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(valuesByColumn, nameof(valuesByColumn));

        return string.Concat(KeyCols.Select(colName =>
        {
            var normalized = colName.NormalizeName();
            valuesByColumn.TryGetValue(normalized, out var value);
            return SerializeIndexKeyPart(value);
        }));
    }

    private static string SerializeIndexKeyPart(object? value)
    {
        if (value is null || value is DBNull)
            return "n;";

        var text = value.ToString() ?? string.Empty;
        return $"s{text.Length}:{text};";
    }

    /// <summary>
    /// EN: Looks up values in the index using the given key.
    /// PT: Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT: Lista de posições ou null.</returns>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(string key)
    {
        // Backward-compatible lookup: callers often pass raw values for single-column
        // _indexes (e.g. "John"). Internally keys are serialized, so try both formats.
        var normalizedKey = key.NormalizeName();
        if (_items.TryGetValue(normalizedKey, out var list))
            return AsReadOnlyBucket(normalizedKey, list);

        var serializedKey = SerializeIndexKeyPart(key);
        return _items.TryGetValue(serializedKey, out list)
            ? AsReadOnlyBucket(serializedKey, list)
            : null;
    }

    internal IReadOnlyDictionary<int, Dictionary<string, object?>>? LookupMutable(string key)
    {
        if (_items.TryGetValue(key.NormalizeName(), out var list))
            return list;

        var serializedKey = SerializeIndexKeyPart(key);
        return _items.TryGetValue(serializedKey, out list)
            ? list
            : null;
    }

    public void UpdateIndexesWithRow(
        int rowIndex,
        IReadOnlyDictionary<int, object?> newRow)
    {
        var tableColumns = Table.Columns;
        var pkColumnsByIndex = tableColumns.ToDictionary(_ => _.Value.Index, _ => _.Key);
        var key = BuildIndexKey(newRow);

        if (!_items.TryGetValue(key, out var lstItems))
        {
            lstItems = [];
            _items.Add(key, lstItems);
        }

        if (lstItems.TryGetValue(rowIndex, out var idxRow))
        {
            foreach (var item in Include)
            {
                var col = Table.GetColumn(item);
                idxRow[item!] = newRow[col.Index];
            }
            return;
        }
        if (Unique && lstItems.Count > 0)
            throw Table.DuplicateKey(Table.TableName, Name, $"{key} ({string.Join(",", KeyCols)})");
        idxRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        lstItems.Add(rowIndex, idxRow);
        foreach (var item in KeyCols)
        {
            var col = Table.GetColumn(item);
            idxRow.Add(item!, newRow[col.Index]);
        }
        foreach (var item in Include)
        {
            var col = Table.GetColumn(item);
            idxRow.Add(item!, newRow[col.Index]);
        }

        foreach (var pkIdx in Table.PrimaryKeyIndexes)
        {
            if (!pkColumnsByIndex.TryGetValue(pkIdx, out var pkName)
                || idxRow.ContainsKey(pkName))
                continue;
            idxRow.Add(pkName, newRow[pkIdx]);
        }
    }

    public void UpdateIndexesWithRow(
        int rowIndex,
        IReadOnlyDictionary<int, object?> oldRow,
        IReadOnlyDictionary<int, object?> newRow)
    {
        var tableColumns = Table.Columns;
        var pkColumnsByIndex = tableColumns.ToDictionary(_ => _.Value.Index, _ => _.Key);
        var oldkey = BuildIndexKey(oldRow);
        var key = BuildIndexKey(newRow);
        if (oldkey != key && _items.TryGetValue(oldkey, out var oldLstItems))
        {
            oldLstItems.Remove(rowIndex);
            RemoveBucketIfEmpty(oldkey, oldLstItems);
        }
        

        if (!_items.TryGetValue(key, out var lstItems))
        {
            lstItems = [];
            _items.Add(key, lstItems);
        }

        if (lstItems.TryGetValue(rowIndex, out var idxRow))
        {
            foreach (var item in Include)
            {
                var col = Table.GetColumn(item);
                idxRow[item!] = newRow[col.Index];
            }
            return;
        }
        if (Unique && lstItems.Count > 0)
            throw Table.DuplicateKey(Table.TableName, Name, $"{key} ({string.Join(",", KeyCols)})");
        idxRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        lstItems.Add(rowIndex, idxRow);
        foreach (var item in KeyCols)
        {
            var col = Table.GetColumn(item);
            idxRow.Add(item!, newRow[col.Index]);
        }
        foreach (var item in Include)
        {
            var col = Table.GetColumn(item);
            idxRow.Add(item!, newRow[col.Index]);
        }

        foreach (var pkIdx in Table.PrimaryKeyIndexes)
        {
            if (!pkColumnsByIndex.TryGetValue(pkIdx, out var pkName)
                || idxRow.ContainsKey(pkName))
                continue;
            idxRow.Add(pkName, newRow[pkIdx]);
        }
    }

    internal void EnsureUniqueBeforeUpdate(
        int rowIndex,
        IReadOnlyDictionary<int, object?> existingRow,
        IReadOnlyDictionary<int, object?> simulatedRow,
        IReadOnlyCollection<string> changedCols)
    {
        if (!KeyCols.Intersect(changedCols, StringComparer.OrdinalIgnoreCase).Any()) return;

        var oldKey = BuildIndexKey(existingRow);
        var newKey = BuildIndexKey(simulatedRow);

        if (oldKey.Equals(newKey, StringComparison.Ordinal))
            return;
        
        var hits = LookupMutable(newKey);
        if (hits is not null && hits.Keys.Any(idx => idx != rowIndex))
        {
            throw Table.DuplicateKey(Table.TableName, Name, $"{newKey} ({string.Join(",", KeyCols)})");
        }
    }
}
