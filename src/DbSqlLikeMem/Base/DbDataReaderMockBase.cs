using System.Collections;

namespace DbSqlLikeMem;

public abstract class DbDataReaderMockBase(
    IList<TableResultMock> tables
    ) : DbDataReader
{
    private readonly List<List<Dictionary<int, object?>>> _resultSets = [.. tables.Select(table => table.ToList())];
    private readonly List<Dictionary<int, (string ColumName, DbType DbType, bool IsNullable)>> _columnsDic = [.. tables
        .Select(table => table.Columns
            .ToDictionary(c => c.ColumIndex, c => (
                c.ColumnAlias,
                c.DbType,
                c.IsNullable)))];

    private int _currentResultSetIndex;
    private int _currentIndex = -1;
    private bool disposedValue;

    public override object this[int i] => _resultSets[_currentResultSetIndex][_currentIndex][i] ?? DBNull.Value;
    public override object this[string name]
    {
        get
        {
            var key = GetOrdinal(name);
            return _resultSets[_currentResultSetIndex][_currentIndex][key] ?? DBNull.Value;
        }
    }

    public override int Depth { get; }
    private bool _isClosed;
    public override bool IsClosed => _isClosed;
    public override int RecordsAffected => _resultSets[_currentResultSetIndex].Count;
    public override int FieldCount => _columnsDic[_currentResultSetIndex].Count;

    public override bool HasRows => _resultSets.Count > 0 && _resultSets[_currentResultSetIndex].Count > 0;

    public override void Close()
        => _isClosed = true;

    public override bool GetBoolean(int ordinal) => (bool)this[ordinal];
    public override byte GetByte(int ordinal) => (byte)this[ordinal];
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
    public override char GetChar(int ordinal) => (char)this[ordinal];
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
    protected override DbDataReader GetDbDataReader(int ordinal) => throw new NotImplementedException();
    public override string GetDataTypeName(int ordinal) => _columnsDic[_currentResultSetIndex][ordinal].DbType.ToString();
    public override DateTime GetDateTime(int ordinal) => (DateTime)this[ordinal];
    public override decimal GetDecimal(int ordinal) => (decimal)this[ordinal];
    public override double GetDouble(int ordinal) => (double)this[ordinal];
    public override Type GetFieldType(int ordinal)
        => _columnsDic[_currentResultSetIndex][ordinal].DbType.ConvertDbTypeToType();
    public override float GetFloat(int ordinal) => (float)this[ordinal];
    public override Guid GetGuid(int ordinal) => (Guid)this[ordinal];
    public override short GetInt16(int ordinal) => (short)this[ordinal];
    public override int GetInt32(int ordinal) => (int)this[ordinal];
    public override long GetInt64(int ordinal) => (long)this[ordinal];
    public override string GetName(int ordinal)
    {
        var n = _columnsDic[_currentResultSetIndex][ordinal].ColumName ?? string.Empty;
        return NormalizeColumnName(n);
    }

    public override int GetOrdinal(string name)
    {
        name = NormalizeColumnName(name);
        var cols = _columnsDic[_currentResultSetIndex];

        // 1) exact/normalized match (case-insensitive)
        foreach (var kv in cols)
        {
            if (NormalizeColumnName(kv.Value.ColumName).Equals(name, StringComparison.OrdinalIgnoreCase))
                return kv.Key;
        }

        // 2) fallback: allow asking for unqualified name when stored is qualified (t.col)
        foreach (var kv in cols)
        {
            var stored = kv.Value.ColumName ?? string.Empty;
            var dot = stored.LastIndexOf('.');
            if (dot > 0)
            {
                var tail = NormalizeColumnName(stored[(dot + 1)..]);
                if (tail.Equals(name, StringComparison.OrdinalIgnoreCase))
                    return kv.Key;
            }
        }

        throw new IndexOutOfRangeException($"Column '{name}' was not found in reader.");
    }

    private static string NormalizeColumnName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return string.Empty;

        // trim spaces and common identifier quoting
        name = name.Trim().Trim('`').Trim('"').Trim();

        // if it still looks qualified, keep only the last part (MySQL typically returns unqualified)
        var dot = name.LastIndexOf('.');
        if (dot > 0 && dot + 1 < name.Length)
            name = name[(dot + 1)..].Trim().Trim('`').Trim('"').Trim();

        return name;
    }
    public override DataTable GetSchemaTable()
    {
        var dt = new DataTable();
        foreach (var col in _columnsDic[_currentResultSetIndex])
            dt.Columns.Add(new DataColumn(
                NormalizeColumnName(col.Value.ColumName ?? string.Empty),
                col.Value.DbType.ConvertDbTypeToType())
            {
                AllowDBNull = col.Value.IsNullable
            });
        return dt;
    }

    public override string GetString(int ordinal) => (string)this[ordinal];
    public override object GetValue(int ordinal)
    {
        var v = this[ordinal];
        return v is HashSet<string> hs ? string.Join(',', hs) : v;
    }
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        for (int i = 0; i < FieldCount; i++)
        {
            values[i] = this[i] ?? DBNull.Value;
        }
        return FieldCount;
    }
    public override bool IsDBNull(int ordinal) => this[ordinal] == null || this[ordinal] == DBNull.Value;
    public override bool NextResult()
    {
        if (_currentResultSetIndex + 1 >= _resultSets.Count) return false;
        _currentResultSetIndex++;
        _currentIndex = -1;
        return true;
    }

    public override bool Read()
    {
        if (_currentIndex + 1 >= _resultSets[_currentResultSetIndex].Count) return false;
        _currentIndex++;
        return true;
    }

    public override IEnumerator GetEnumerator() => _resultSets.GetEnumerator();
    protected override void Dispose(bool disposing)
    {
        if (!disposedValue)
        {
            if (disposing)
#pragma warning disable S1135 // Track uses of "TODO" tags
            {
                // TODO: dispose managed state (managed objects)
            }
#pragma warning restore S1135 // Track uses of "TODO" tags

#pragma warning disable S1135 // Track uses of "TODO" tags
// TODO: free unmanaged resources (unmanaged objects) and override finalizer

#pragma warning disable S1135 // Track uses of "TODO" tags
// TODO: set large fields to null
            disposedValue = true;
#pragma warning restore S1135 // Track uses of "TODO" tags
#pragma warning restore S1135 // Track uses of "TODO" tags
        }
        base.Dispose(disposing);
    }


#pragma warning disable S1135 // Track uses of "TODO" tags
    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~MySqlDataReaderMock()

#pragma warning disable S125 // Sections of code should not be commented out
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }
}