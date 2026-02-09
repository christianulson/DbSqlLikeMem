using System.Collections.Concurrent;

namespace DbSqlLikeMem;

public abstract class TableMock
    : List<Dictionary<int, object?>>,
    ITableMock
{

    protected TableMock(
        string tableName,
        SchemaMock schema,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        TableName = tableName.NormalizeName();
        Schema = schema;
        Columns = columns;
        AddRange(rows ?? []);
    }

    public string TableName { get; }
    public SchemaMock Schema { get; }
    public int NextIdentity { get; set; } = 1;

    public IColumnDictionary Columns { get; }

    // ---------- Wave D : índices ---------------------------------
    public HashSet<int> PrimaryKeyIndexes { get; } = [];

#pragma warning disable CA1002 // Do not expose generic lists
    private readonly List<(string Col, string RefTable, string RefCol)> _foreignKeys = [];
    public IReadOnlyList<(string Col, string RefTable, string RefCol)> ForeignKeys => _foreignKeys;
#pragma warning restore CA1002 // Do not expose generic lists

#pragma warning disable CA1002 // Do not expose generic lists
    public IndexDictionary Indexes { get; } = [];
#pragma warning restore CA1002 // Do not expose generic lists

    // nome-do-índice → chave-derivada(string) → posições (List<int>)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, List<int>>> _ix
        = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Retorna o ColumnDef para <paramref name="columnName"/>
    /// ou lança UnknownColumn se não existir.
    /// </summary>
    public ColumnDef GetColumn(string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);
        if (!Columns.TryGetValue(columnName.NormalizeName(), out var info))
            throw UnknownColumn(columnName);
        return info;
    }

    public void CreateIndex(IndexDef def)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentException.ThrowIfNullOrWhiteSpace(def.Name);
        var name = def.Name.NormalizeName();
        if (Indexes.ContainsKey(name))
            throw new InvalidOperationException($"Índice '{name}' já existe.");
        Indexes.Add(name, def);
        RebuildIndex(def);
    }

    internal void RebuildIndex(IndexDef def)
    {
        ArgumentNullException.ThrowIfNull(def);
        ArgumentException.ThrowIfNullOrWhiteSpace(def.Name);
        var name = def.Name.NormalizeName();
        var map = new ConcurrentDictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < Count; i++)
        {
            var key = BuildKey(this[i], def.KeyCols.Select(c => Columns[c].Index));
            map.AddOrUpdate(key, [i], (_, list) => { list.Add(i); return list; });
        }
        _ix[name] = map;
    }

    public IEnumerable<int>? Lookup(IndexDef def, string key)
    {
        ArgumentNullException.ThrowIfNull(def);
        return _ix.TryGetValue(def.Name.NormalizeName(), out var map)
            && map.TryGetValue(key.NormalizeName(), out var list)
            ? list
            : null;
    }

    public void UpdateIndexesWithRow(int rowIdx)
    {
        foreach (var (name, def) in Indexes)
        {
            var key = BuildKey(this[rowIdx], def.KeyCols.Select(c => Columns[c].Index));
            _ix[name].AddOrUpdate(key, [rowIdx], (_, list) => { list.Add(rowIdx); return list; });
        }
    }

    public void RebuildAllIndexes()
    {
        foreach (var ix in Indexes)
            RebuildIndex(ix.Value);
    }

    private static string BuildKey(
        Dictionary<int, object?> row, IEnumerable<int> ords) =>
        string.Join('|', ords.Select(o => row.TryGetValue(o, out var it) ? it?.ToString() ?? "<null>" : "<null>"));

    public void CreateForeignKey(
        string col,
        string refTable,
        string refCol)
    {
        _foreignKeys.Add((col, refTable, refCol));
    }

    public void AddRangeItems<T>(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            AddItem(item);
    }

    public new void AddRange(IEnumerable<Dictionary<int, object?>> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
            Add(item);
    }

    public void AddItem<T>(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        var row = new Dictionary<int, object?>();

        // pega props públicas de instância (ignorando indexers)
        var t = typeof(T);

        foreach (var p in Columns)
        {
            var prop = t.GetProperty(p.Key);
            if (prop == null)
            {
                row[p.Value.Index] = null;
                continue;
            }
            object? value;
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                value = prop.GetValue(item);
            }
            catch
            {
                value = null;
            }
#pragma warning restore CA1031 // Do not catch general exception types

            row[p.Value.Index] = value;
        }

        // reaproveita sua lógica de unique + index update
        Add(row);
    }

    public new void Add(Dictionary<int, object?> value)
    {
        ApplyDefaultValues(value);
        // Before adding, enforce unique indexes
        foreach (var idx in Indexes.GetUnique())
        {
            var key = BuildKey(value, idx.KeyCols.Select(c => Columns[c].Index));
            if (_ix.TryGetValue(idx.Name, out var map)
                && map.TryGetValue(key, out var existing)
                && existing.Count != 0)
            {
                throw DuplicateKey(TableName, idx.Name, key);
            }
        }
        base.Add(value);
        // Update indexes with the new row
        int newIdx = Count - 1;
        foreach (var idx in Indexes)
            UpdateIndexesWithRow(newIdx);
    }

    private void ApplyDefaultValues(Dictionary<int, object?> value)
    {
        foreach (var (key, col) in Columns)
        {
            if (!value.ContainsKey(col.Index)) continue;
            if (col.Identity) value[col.Index] = NextIdentity++;
            else if (col.DefaultValue != null && value[col.Index] == null) value[col.Index] = col.DefaultValue;
            if (!col.Nullable && value[col.Index] == null) throw ColumnCannotBeNull(key);
        }
    }

    private List<Dictionary<int, object?>>? _backup;

    public void Backup() => _backup = [.. this.Select(row => new Dictionary<int, object?>(row))];

    public void Restore()
    {
        if (_backup == null)
            return;

        Clear();
        foreach (var row in _backup) Add(row);

        foreach (var ix in Indexes)
            RebuildIndex(ix.Value);
    }

    public void ClearBackup() => _backup = null;

    public abstract string? CurrentColumn { get; set; }
    public abstract object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null);

    public abstract Exception UnknownColumn(string columnName);
    public abstract Exception DuplicateKey(string tbl, string key, object? val);
    public abstract Exception ColumnCannotBeNull(string col);
    public abstract Exception ForeignKeyFails(string col, string refTbl);
    public abstract Exception ReferencedRow(string tbl);
}
