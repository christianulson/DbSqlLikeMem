namespace DbSqlLikeMem;

public interface ISchemaMock
{
    TableMock CreateTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    bool TryGetTable(string key, out ITableMock? value);

    void BackupAllTablesBestEffort();

    void RestoreAllTablesBestEffort();

    void ClearBackupAllTablesBestEffort();
}
