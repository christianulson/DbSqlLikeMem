namespace DbSqlLikeMem;
public interface ITableMock
    : IList<Dictionary<int, object?>>
{
    int NextIdentity { get; set; }

    HashSet<int> PrimaryKeyIndexes { get; }

    IReadOnlyList<(
        string Col, 
        string RefTable,
        string RefCol
        )> ForeignKeys { get; }

    void CreateForeignKey(
        string col,
        string refTable,
        string refCol);

    IColumnDictionary Columns{ get; }

    ColumnDef GetColumn(string columnName);

    IndexDictionary Indexes { get; }

    void CreateIndex(IndexDef def);

    void UpdateIndexesWithRow(int rowIdx);

    void RebuildAllIndexes();
    IEnumerable<int>? Lookup(IndexDef def, string key);

    void AddRangeItems<T>(IEnumerable<T> items);

    void AddItem<T>(T item);

    void AddRange(IEnumerable<Dictionary<int, object?>> items);

    new void Add(Dictionary<int, object?> value);

    void Backup();

    void Restore();

    void ClearBackup();

    string? CurrentColumn { get; set; }

    object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null);

    Exception UnknownColumn(string columnName);
    Exception DuplicateKey(string tbl, string key, object? val);
    Exception ColumnCannotBeNull(string col);
    Exception ForeignKeyFails(string col, string refTbl);
    Exception ReferencedRow(string tbl);
}
