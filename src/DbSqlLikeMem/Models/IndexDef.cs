using System.Collections.ObjectModel;

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

    public IEnumerable<string> Keys => _items.Keys;

    public IEnumerable<IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>> Values
    {
        get
        {
            foreach (var i in _items.Values)
            {
                var r = new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
                    i.ToDictionary(
                        _ => _.Key,
                        _ => new ReadOnlyDictionary<string, object?>(_.Value)));

                yield return (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)r;
            }
        }
    }

    public int Count => _items.Count;

    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> this[string key]
        => (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
            _items[key].ToDictionary(
                _ => _.Key,
                _ => new ReadOnlyDictionary<string, object?>(_.Value)));

    public bool ContainsKey(string key)
        => _items.ContainsKey(key);

    public bool TryGetValue(string key, out IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>> value)
    {
        if (!_items.TryGetValue(key, out var v))
        {
            value = null;
            return false;
        }
        value = (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
            v.ToDictionary(
                _ => _.Key,
                _ => new ReadOnlyDictionary<string, object?>(_.Value)));
        return true;
    }

    public IEnumerator<KeyValuePair<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>> GetEnumerator()
    {
        foreach (var it in _items)
            yield return new KeyValuePair<string, IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>>(
                it.Key,
                (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
                    it.Value.ToDictionary(
                        _ => _.Key,
                        _ => new ReadOnlyDictionary<string, object?>(_.Value)))
                );
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    internal void RebuildIndex()
    {
        _items.Clear();
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
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null. PT: Lista de posições ou null.</returns>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(string key)
    {
        // Backward-compatible lookup: callers often pass raw values for single-column
        // _indexes (e.g. "John"). Internally keys are serialized, so try both formats.
        if (_items.TryGetValue(key.NormalizeName(), out var list))
            return (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
                list.ToDictionary(
                    _ => _.Key,
                    _ => new ReadOnlyDictionary<string, object?>(_.Value)));

        var serializedKey = SerializeIndexKeyPart(key);
        return _items.TryGetValue(serializedKey, out list)
            ? (IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>)new ReadOnlyDictionary<int, ReadOnlyDictionary<string, object?>>(
                list.ToDictionary(
                    _ => _.Key,
                    _ => new ReadOnlyDictionary<string, object?>(_.Value)))
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
            oldLstItems.Remove(rowIndex);
        

        if (!_items.TryGetValue(key, out var lstItems))
        {
            lstItems = [];
            _items.Add(key, lstItems);
        }

        var pk = string.Join(",", Table.PrimaryKeyIndexes.Select(pkIdx => newRow[pkIdx]));

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
        
        var pk = string.Join(",", Table.PrimaryKeyIndexes.Select(pkIdx => oldKey[pkIdx]));
        if (Lookup(newKey)?.ContainsKey(rowIndex) == true)
        {
            throw Table.DuplicateKey(Table.TableName, Name, $"{newKey} ({string.Join(",", KeyCols)})");
        }
    }
}
