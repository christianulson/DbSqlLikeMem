using System.Collections;

namespace DbSqlLikeMem;

/// <summary>
/// Implementa um leitor de dados em memória para resultados de consulta.
/// </summary>
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

    /// <summary>
    /// Obtém o valor da coluna pelo índice ordinal.
    /// </summary>
    public override object this[int i] => _resultSets[_currentResultSetIndex][_currentIndex][i] ?? DBNull.Value;
    /// <summary>
    /// Obtém o valor da coluna pelo nome.
    /// </summary>
    public override object this[string name]
    {
        get
        {
            var key = GetOrdinal(name);
            return _resultSets[_currentResultSetIndex][_currentIndex][key] ?? DBNull.Value;
        }
    }

    /// <summary>
    /// Profundidade do leitor (não utilizada).
    /// </summary>
    public override int Depth { get; }
    private bool _isClosed;
    /// <summary>
    /// Indica se o leitor está fechado.
    /// </summary>
    public override bool IsClosed => _isClosed;
    /// <summary>
    /// Número de registros afetados no conjunto atual.
    /// </summary>
    public override int RecordsAffected => _resultSets[_currentResultSetIndex].Count;
    /// <summary>
    /// Quantidade de colunas do conjunto atual.
    /// </summary>
    public override int FieldCount => _columnsDic[_currentResultSetIndex].Count;

    /// <summary>
    /// Indica se o conjunto atual possui linhas.
    /// </summary>
    public override bool HasRows => _resultSets.Count > 0 && _resultSets[_currentResultSetIndex].Count > 0;

    /// <summary>
    /// Fecha o leitor.
    /// </summary>
    public override void Close()
        => _isClosed = true;

    /// <summary>
    /// Obtém valor booleano da coluna indicada.
    /// </summary>
    public override bool GetBoolean(int ordinal) => (bool)this[ordinal];
    /// <summary>
    /// Obtém valor byte da coluna indicada.
    /// </summary>
    public override byte GetByte(int ordinal) => (byte)this[ordinal];
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
    /// <summary>
    /// Obtém valor char da coluna indicada.
    /// </summary>
    public override char GetChar(int ordinal) => (char)this[ordinal];
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
    protected override DbDataReader GetDbDataReader(int ordinal) => throw new NotImplementedException();
    /// <summary>
    /// Obtém o nome do tipo de dados da coluna.
    /// </summary>
    public override string GetDataTypeName(int ordinal) => _columnsDic[_currentResultSetIndex][ordinal].DbType.ToString();
    /// <summary>
    /// Obtém valor DateTime da coluna indicada.
    /// </summary>
    public override DateTime GetDateTime(int ordinal) => (DateTime)this[ordinal];
    /// <summary>
    /// Obtém valor decimal da coluna indicada.
    /// </summary>
    public override decimal GetDecimal(int ordinal) => (decimal)this[ordinal];
    /// <summary>
    /// Obtém valor double da coluna indicada.
    /// </summary>
    public override double GetDouble(int ordinal) => (double)this[ordinal];
    /// <summary>
    /// Obtém o tipo .NET da coluna indicada.
    /// </summary>
    public override Type GetFieldType(int ordinal)
        => _columnsDic[_currentResultSetIndex][ordinal].DbType.ConvertDbTypeToType();
    /// <summary>
    /// Obtém valor float da coluna indicada.
    /// </summary>
    public override float GetFloat(int ordinal) => (float)this[ordinal];
    /// <summary>
    /// Obtém valor Guid da coluna indicada.
    /// </summary>
    public override Guid GetGuid(int ordinal) => (Guid)this[ordinal];
    /// <summary>
    /// Obtém valor Int16 da coluna indicada.
    /// </summary>
    public override short GetInt16(int ordinal) => (short)this[ordinal];
    /// <summary>
    /// Obtém valor Int32 da coluna indicada.
    /// </summary>
    public override int GetInt32(int ordinal) => (int)this[ordinal];
    /// <summary>
    /// Obtém valor Int64 da coluna indicada.
    /// </summary>
    public override long GetInt64(int ordinal) => (long)this[ordinal];
    /// <summary>
    /// Obtém o nome da coluna pelo ordinal.
    /// </summary>
    public override string GetName(int ordinal)
    {
        var n = _columnsDic[_currentResultSetIndex][ordinal].ColumName ?? string.Empty;
        return NormalizeColumnName(n);
    }

    /// <summary>
    /// Obtém o ordinal de uma coluna pelo nome.
    /// </summary>
    /// <param name="name">Nome da coluna.</param>
    /// <returns>Índice da coluna.</returns>
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
    /// <summary>
    /// Retorna uma DataTable com metadados das colunas.
    /// </summary>
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

    /// <summary>
    /// Obtém valor string da coluna indicada.
    /// </summary>
    public override string GetString(int ordinal) => (string)this[ordinal];
    /// <summary>
    /// Obtém o valor da coluna indicada.
    /// </summary>
    public override object GetValue(int ordinal)
    {
        var v = this[ordinal];
        return v is HashSet<string> hs ? string.Join(',', hs) : v;
    }
    /// <summary>
    /// Copia os valores da linha atual para o array fornecido.
    /// </summary>
    /// <param name="values">Array de destino.</param>
    /// <returns>Número de colunas copiadas.</returns>
    public override int GetValues(object[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        for (int i = 0; i < FieldCount; i++)
        {
            values[i] = this[i] ?? DBNull.Value;
        }
        return FieldCount;
    }
    /// <summary>
    /// Indica se o valor da coluna é DBNull.
    /// </summary>
    public override bool IsDBNull(int ordinal) => this[ordinal] == null || this[ordinal] == DBNull.Value;
    /// <summary>
    /// Avança para o próximo conjunto de resultados.
    /// </summary>
    public override bool NextResult()
    {
        if (_currentResultSetIndex + 1 >= _resultSets.Count) return false;
        _currentResultSetIndex++;
        _currentIndex = -1;
        return true;
    }

    /// <summary>
    /// Avança para a próxima linha do conjunto atual.
    /// </summary>
    public override bool Read()
    {
        if (_currentIndex + 1 >= _resultSets[_currentResultSetIndex].Count) return false;
        _currentIndex++;
        return true;
    }

    /// <summary>
    /// Retorna um enumerador dos conjuntos de resultados.
    /// </summary>
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
