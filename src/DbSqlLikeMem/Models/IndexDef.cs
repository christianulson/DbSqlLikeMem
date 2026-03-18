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
        _keyColumns = [.. KeyCols.Select(Table.GetColumn)];
        _includeColumns = [.. Include.Select(Table.GetColumn)];
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

    private readonly ColumnDef[] _keyColumns;
    private readonly ColumnDef[] _includeColumns;


    private readonly Dictionary<string, Dictionary<int, Dictionary<string, object?>>> _items = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BucketReadOnlyView> _readonlyBuckets = new(StringComparer.OrdinalIgnoreCase);
    private bool _isDirty;

    internal bool IsDirty => _isDirty;

    internal void MarkDirty()
    {
        _isDirty = true;
        _readonlyBuckets.Clear();
    }

    private void EnsureReady()
    {
        if (_isDirty)
            RebuildIndex();
    }

    private static long BeginPerformancePhase(string phase)
    {
        var metrics = DbMetrics.Current;
        if (metrics is null)
            return 0;

        metrics.IncrementPerformancePhaseHit(phase);
        return Stopwatch.GetTimestamp();
    }

    private static void EndPerformancePhase(string phase, long startedAt)
    {
        if (startedAt == 0)
            return;

        DbMetrics.Current?.IncrementPerformancePhaseElapsedTicks(
            phase,
            StopwatchCompatible.GetElapsedTicks(startedAt));
    }

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

    private IReadOnlyDictionary<int, string> GetColumnsByIndex()
        => Table is TableMock tableMock
            ? tableMock.ColumnsByIndex
            : Table.Columns.ToDictionary(_ => _.Value.Index, _ => _.Key);

    private Dictionary<string, object?> CreateIndexRow(IReadOnlyDictionary<int, object?> row)
    {
        var idxRow = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var column in _keyColumns)
            idxRow.Add(column.Name, row[column.Index]);

        foreach (var column in _includeColumns)
            idxRow.Add(column.Name, row[column.Index]);

        return idxRow;
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

    public IEnumerable<string> Keys
    {
        get
        {
            EnsureReady();
            return _items.Keys;
        }
    }

    public IEnumerable<IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>> Values
    {
        get
        {
            EnsureReady();
            foreach (var item in _items)
                yield return AsReadOnlyBucket(item.Key, item.Value);
        }
    }

    public int Count
    {
        get
        {
            EnsureReady();
            return _items.Count;
        }
    }

    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> this[string key]
    {
        get
        {
            EnsureReady();
            return AsReadOnlyBucket(key, _items[key]);
        }
    }

    public bool ContainsKey(string key)
    {
        EnsureReady();
        return _items.ContainsKey(key);
    }

    public bool TryGetValue(string key, out IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> value)
    {
        EnsureReady();
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
        EnsureReady();
        foreach (var item in _items)
            yield return new KeyValuePair<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>(
                item.Key,
                AsReadOnlyBucket(item.Key, item.Value));
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    internal void RemoveRow(int rowIndex, IReadOnlyDictionary<int, object?> row)
    {
        var startedAt = BeginPerformancePhase(DbPerformanceMetricKeys.IndexRemove);
        try
        {
        EnsureReady();
        var key = BuildIndexKey(row);
        if (_items.TryGetValue(key, out var lstItems))
        {
            lstItems.Remove(rowIndex);
            if (lstItems.Count == 0)
            {
                _items.Remove(key);
            }
            _readonlyBuckets.Remove(key);
        }
        }
        finally
        {
            EndPerformancePhase(DbPerformanceMetricKeys.IndexRemove, startedAt);
        }
    }

    internal void ShiftPositionsAfter(int deletedIndex)
    {
        var startedAt = BeginPerformancePhase(DbPerformanceMetricKeys.IndexShift);
        try
        {
        EnsureReady();
        foreach (var bucket in _items.Values)
        {
            var keysToShift = bucket.Keys.Where(k => k > deletedIndex).OrderBy(k => k).ToList();
            if (keysToShift.Count == 0) continue;

            foreach (var oldIdx in keysToShift)
            {
                var rowData = bucket[oldIdx];
                bucket.Remove(oldIdx);
                bucket[oldIdx - 1] = rowData;
            }
        }
        _readonlyBuckets.Clear();
        }
        finally
        {
            EndPerformancePhase(DbPerformanceMetricKeys.IndexShift, startedAt);
        }
    }

    internal void RebuildIndex()
    {
        var startedAt = BeginPerformancePhase(DbPerformanceMetricKeys.IndexRebuild);
        try
        {
        _isDirty = false;
        _items.Clear();
        _readonlyBuckets.Clear();
        var pkColumnsByIndex = GetColumnsByIndex();
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

            var idxRow = CreateIndexRow(row);
            lstItems.Add(i, idxRow);
            AddRowLocatorColumns(idxRow, row, pkColumnsByIndex);
        }
        }
        finally
        {
            EndPerformancePhase(DbPerformanceMetricKeys.IndexRebuild, startedAt);
        }
    }

    internal string BuildIndexKey(
        IReadOnlyDictionary<int, object?> row)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(row, nameof(row));
        return string.Concat(_keyColumns.Select(ci =>
        {
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

    private void AddRowLocatorColumns(
        Dictionary<string, object?> idxRow,
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyDictionary<int, string> columnsByIndex)
    {
        var addedLocator = false;

        foreach (var pkIdx in Table.PrimaryKeyIndexes)
        {
            if (!columnsByIndex.TryGetValue(pkIdx, out var pkName)
                || idxRow.ContainsKey(pkName))
                continue;

            idxRow.Add(pkName, row[pkIdx]);
            addedLocator = true;
        }

        if (addedLocator)
            return;

        if (!Table.Columns.TryGetValue("id", out var idColumn)
            || idxRow.ContainsKey(idColumn.Name))
            return;

        idxRow.Add(idColumn.Name, row[idColumn.Index]);
    }


    /// <summary>
    /// EN: Looks up values in the index using the given key.
    /// PT: Procura valores no índice usando a chave informada.
    /// </summary>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT: Lista de posições ou null.</returns>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(string key)
    {
        EnsureReady();
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
        EnsureReady();
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
        var startedAt = BeginPerformancePhase(DbPerformanceMetricKeys.IndexUpdate);
        try
        {
        EnsureReady();
        var pkColumnsByIndex = GetColumnsByIndex();
        var key = BuildIndexKey(newRow);

        if (!_items.TryGetValue(key, out var lstItems))
        {
            lstItems = [];
            _items.Add(key, lstItems);
        }

        if (lstItems.TryGetValue(rowIndex, out var idxRow))
        {
            foreach (var column in _includeColumns)
                idxRow[column.Name] = newRow[column.Index];
            _readonlyBuckets.Remove(key);
            return;
        }
        if (Unique && lstItems.Count > 0)
            throw Table.DuplicateKey(Table.TableName, Name, $"{key} ({string.Join(",", KeyCols)})");
        idxRow = CreateIndexRow(newRow);
        lstItems.Add(rowIndex, idxRow);
        AddRowLocatorColumns(idxRow, newRow, pkColumnsByIndex);
        _readonlyBuckets.Remove(key);
        }
        finally
        {
            EndPerformancePhase(DbPerformanceMetricKeys.IndexUpdate, startedAt);
        }
    }

    public void UpdateIndexesWithRow(
        int rowIndex,
        IReadOnlyDictionary<int, object?> oldRow,
        IReadOnlyDictionary<int, object?> newRow)
    {
        var startedAt = BeginPerformancePhase(DbPerformanceMetricKeys.IndexUpdate);
        try
        {
        EnsureReady();
        var pkColumnsByIndex = GetColumnsByIndex();
        var oldkey = BuildIndexKey(oldRow);
        var key = BuildIndexKey(newRow);
        if (oldkey != key && _items.TryGetValue(oldkey, out var oldLstItems))
        {
            oldLstItems.Remove(rowIndex);
            RemoveBucketIfEmpty(oldkey, oldLstItems);
            _readonlyBuckets.Remove(oldkey);
        }

        if (!_items.TryGetValue(key, out var lstItems))
        {
            lstItems = [];
            _items.Add(key, lstItems);
        }

        if (lstItems.TryGetValue(rowIndex, out var idxRow))
        {
            foreach (var column in _includeColumns)
                idxRow[column.Name] = newRow[column.Index];
            _readonlyBuckets.Remove(key);
            return;
        }
        if (Unique && lstItems.Count > 0)
            throw Table.DuplicateKey(Table.TableName, Name, $"{key} ({string.Join(",", KeyCols)})");
        idxRow = CreateIndexRow(newRow);
        lstItems.Add(rowIndex, idxRow);
        AddRowLocatorColumns(idxRow, newRow, pkColumnsByIndex);
        _readonlyBuckets.Remove(key);
        }
        finally
        {
            EndPerformancePhase(DbPerformanceMetricKeys.IndexUpdate, startedAt);
        }
    }

    internal void EnsureUniqueBeforeUpdate(
        int rowIndex,
        IReadOnlyDictionary<int, object?> existingRow,
        IReadOnlyDictionary<int, object?> simulatedRow,
        IReadOnlyCollection<string> changedCols)
    {
        EnsureReady();
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
