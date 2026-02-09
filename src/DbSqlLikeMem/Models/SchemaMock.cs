using System.Collections;

namespace DbSqlLikeMem;

public abstract class SchemaMock 
    : ISchemaMock
    , IEnumerable<KeyValuePair<string, ITableMock>>
{
    protected SchemaMock(
        string schemaName,
        DbMock db,
        IDictionary<string, (IColumnDictionary columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null,
        IDictionary<string, ProcedureDef>? procedures = null/*,
        IDictionary<string, SqlSelectQuery>? views = null*/
    )
    {
        SchemaName = schemaName.NormalizeName();
        Db = db;
        if (tables != null)
            foreach (var (tableName, config) in tables)
                CreateTable(tableName, config.columns, config.rows);
        if (procedures != null)
            foreach (var (procedureName, config) in procedures)
                Procedures.Add(procedureName, config);
        //if (views != null)
        //    foreach (var (viewName, config) in views)
        //        Views.AddTable(viewName, config);
    }

    public string SchemaName { get; }

    public DbMock Db { get; }

    /// <summary>Underlying table map. Names are normalized on access.</summary>
    public TableDictionary tables = new TableDictionary();
    public ITableDictionary Tables => tables;

    // Stored procedure contracts (signature-only; no body execution yet)
    public IDictionary<string, ProcedureDef> Procedures { get; } =
        new Dictionary<string, ProcedureDef>(StringComparer.OrdinalIgnoreCase);

    // Non-materialized views (definition only). Evaluated on demand at execution time.
    internal IDictionary<string, SqlSelectQuery> Views { get; } =
        new Dictionary<string, SqlSelectQuery>(StringComparer.OrdinalIgnoreCase);

    protected abstract TableMock NewTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    public TableMock CreateTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
    {
        var t = NewTable(tableName, columns, rows);
        tables.Add(tableName, t);
        return t;
    }

    #region Tables

    public void Add(string key, ITableMock table)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(table);

        key = key.NormalizeName();

        if (!Db.ThreadSafe)
        {
            AddUnsafe(key, table);
            return;
        }

        lock (Db.SyncRoot)
        {
            AddUnsafe(key, table);
        }
    }

    private void AddUnsafe(string key, ITableMock table)
    {
        if (tables.ContainsKey(key))
            throw new InvalidOperationException($"Table '{key}' already exists.");

        tables.Add(key, table);
    }

    public bool TryGetTable(string key, out ITableMock? value)
        => tables.TryGetValue(key.NormalizeName(), out value);

    public ITableMock this[string key]
    {
        get => tables[key.NormalizeName()];
        set => tables[key.NormalizeName()] = value;
    }

    public IEnumerable<string> Keys => tables.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

    public IEnumerable<ITableMock> Values => tables.Values;

    public IEnumerator<KeyValuePair<string, ITableMock>> GetEnumerator()
        => tables.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => tables.GetEnumerator();

    #region Backup / Restore (best-effort)

    public virtual void BackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Backup();
    }

    public virtual void RestoreAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.Restore();
    }

    public virtual void ClearBackupAllTablesBestEffort()
    {
        foreach (var table in tables.Values)
            table.ClearBackup();
    }

    #endregion

    #endregion
}
