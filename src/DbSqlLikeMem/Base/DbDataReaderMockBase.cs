namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements an in-memory data reader for query results.
/// PT: Implementa um leitor de dados em memória para resultados de consulta.
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
    /// EN: Gets the column value by ordinal index.
    /// PT: Obtém o valor da coluna pelo índice ordinal.
    /// </summary>
    public override object this[int i] => _resultSets[_currentResultSetIndex][_currentIndex][i] ?? DBNull.Value;
    /// <summary>
    /// EN: Gets the column value by name.
    /// PT: Obtém o valor da coluna pelo nome.
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
    /// EN: Reader depth (not used).
    /// PT: Profundidade do leitor (não utilizada).
    /// </summary>
    public override int Depth { get; }
    private bool _isClosed;
    /// <summary>
    /// EN: Indicates whether the reader is closed.
    /// PT: Indica se o leitor está fechado.
    /// </summary>
    public override bool IsClosed => _isClosed;
    /// <summary>
    /// EN: Number of records affected in the current set.
    /// PT: Número de registros afetados no conjunto atual.
    /// </summary>
    public override int RecordsAffected => _resultSets[_currentResultSetIndex].Count;
    /// <summary>
    /// EN: Number of columns in the current set.
    /// PT: Quantidade de colunas do conjunto atual.
    /// </summary>
    public override int FieldCount => _columnsDic[_currentResultSetIndex].Count;

    /// <summary>
    /// EN: Indicates whether the current set has rows.
    /// PT: Indica se o conjunto atual possui linhas.
    /// </summary>
    public override bool HasRows => _resultSets.Count > 0 && _resultSets[_currentResultSetIndex].Count > 0;

    /// <summary>
    /// EN: Closes the reader.
    /// PT: Fecha o leitor.
    /// </summary>
    public override void Close()
        => _isClosed = true;

    /// <summary>
    /// EN: Gets a boolean value from the specified column.
    /// PT: Obtém valor booleano da coluna indicada.
    /// </summary>
    public override bool GetBoolean(int ordinal) => (bool)this[ordinal];
    /// <summary>
    /// EN: Gets a byte value from the specified column.
    /// PT: Obtém valor byte da coluna indicada.
    /// </summary>
    public override byte GetByte(int ordinal) => (byte)this[ordinal];
    /// <summary>
    /// EN: Implements GetBytes.
    /// PT: Implementa GetBytes.
    /// </summary>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));

        var source = this[ordinal] switch
        {
            DBNull => Array.Empty<byte>(),
            null => Array.Empty<byte>(),
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            _ => throw new InvalidCastException($"Column {ordinal} value cannot be converted to byte[].")
        };

        if (buffer is null)
            return source.Length;

        if (bufferOffset < 0 || bufferOffset > buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(bufferOffset));

        if (dataOffset >= source.Length)
            return 0;

        var available = source.Length - (int)dataOffset;
        var toCopy = Math.Min(length, Math.Min(available, buffer.Length - bufferOffset));
        if (toCopy <= 0)
            return 0;

        Array.Copy(source, (int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }
    /// <summary>
    /// EN: Gets a char value from the specified column.
    /// PT: Obtém valor char da coluna indicada.
    /// </summary>
    public override char GetChar(int ordinal) => (char)this[ordinal];
    /// <summary>
    /// EN: Implements GetChars.
    /// PT: Implementa GetChars.
    /// </summary>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length));

        var source = this[ordinal] switch
        {
            DBNull => string.Empty,
            null => string.Empty,
            string str => str,
            char[] chars => new string(chars),
            _ => throw new InvalidCastException($"Column {ordinal} value cannot be converted to char sequence.")
        };

        if (buffer is null)
            return source.Length;

        if (bufferOffset < 0 || bufferOffset > buffer.Length)
            throw new ArgumentOutOfRangeException(nameof(bufferOffset));

        if (dataOffset >= source.Length)
            return 0;

        var available = source.Length - (int)dataOffset;
        var toCopy = Math.Min(length, Math.Min(available, buffer.Length - bufferOffset));
        if (toCopy <= 0)
            return 0;

        source.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }
    /// <summary>
    /// EN: Gets a nested data reader for the specified ordinal.
    /// PT: Obtém um data leitor aninhado para o ordinal especificado.
    /// </summary>
    /// <param name="ordinal">EN: Column ordinal. PT: Ordinal da coluna.</param>
    /// <returns>EN: Nested data reader. PT: Data reader aninhado.</returns>
    protected override DbDataReader GetDbDataReader(int ordinal)
    {
        var value = this[ordinal];
        if (value is DbDataReader reader)
            return reader;
        throw new InvalidCastException($"Column {ordinal} value cannot be converted to DbDataReader.");
    }
    /// <summary>
    /// EN: Gets the data type name of the column.
    /// PT: Obtém o nome do tipo de dados da coluna.
    /// </summary>
    public override string GetDataTypeName(int ordinal) => _columnsDic[_currentResultSetIndex][ordinal].DbType.ToString();
    /// <summary>
    /// EN: Gets a DateTime value from the specified column.
    /// PT: Obtém valor DateTime da coluna indicada.
    /// </summary>
    public override DateTime GetDateTime(int ordinal) => (DateTime)this[ordinal];
    /// <summary>
    /// EN: Gets a decimal value from the specified column.
    /// PT: Obtém valor decimal da coluna indicada.
    /// </summary>
    public override decimal GetDecimal(int ordinal) => (decimal)this[ordinal];
    /// <summary>
    /// EN: Gets a double value from the specified column.
    /// PT: Obtém valor double da coluna indicada.
    /// </summary>
    public override double GetDouble(int ordinal) => (double)this[ordinal];
    /// <summary>
    /// EN: Gets the .NET type of the specified column.
    /// PT: Obtém o tipo .NET da coluna indicada.
    /// </summary>
    public override Type GetFieldType(int ordinal)
        => _columnsDic[_currentResultSetIndex][ordinal].DbType.ConvertDbTypeToType();
    /// <summary>
    /// EN: Gets a float value from the specified column.
    /// PT: Obtém valor float da coluna indicada.
    /// </summary>
    public override float GetFloat(int ordinal) => (float)this[ordinal];
    /// <summary>
    /// EN: Gets a Guid value from the specified column.
    /// PT: Obtém valor Guid da coluna indicada.
    /// </summary>
    public override Guid GetGuid(int ordinal) => (Guid)this[ordinal];
    /// <summary>
    /// EN: Gets an Int16 value from the specified column.
    /// PT: Obtém valor Int16 da coluna indicada.
    /// </summary>
    public override short GetInt16(int ordinal) => Convert.ToInt16(this[ordinal], System.Globalization.CultureInfo.InvariantCulture);
    /// <summary>
    /// EN: Gets an Int32 value from the specified column.
    /// PT: Obtém valor Int32 da coluna indicada.
    /// </summary>
    public override int GetInt32(int ordinal) => Convert.ToInt32(this[ordinal], System.Globalization.CultureInfo.InvariantCulture);
    /// <summary>
    /// EN: Gets an Int64 value from the specified column.
    /// PT: Obtém valor Int64 da coluna indicada.
    /// </summary>
    public override long GetInt64(int ordinal) => Convert.ToInt64(this[ordinal], System.Globalization.CultureInfo.InvariantCulture);
    /// <summary>
    /// EN: Gets the column name by ordinal.
    /// PT: Obtém o nome da coluna pelo ordinal.
    /// </summary>
    public override string GetName(int ordinal)
    {
        var n = _columnsDic[_currentResultSetIndex][ordinal].ColumName ?? string.Empty;
        return NormalizeColumnName(n);
    }

    /// <summary>
    /// EN: Gets the ordinal of a column by name.
    /// PT: Obtém o ordinal de uma coluna pelo nome.
    /// </summary>
    /// <param name="name">EN: Column name. PT: Nome da coluna.</param>
    /// <returns>EN: Column index. PT: Índice da coluna.</returns>
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
        name = name.Trim().Trim('`').Trim('"').Trim('[').Trim(']').Trim();

        // if it still looks qualified, keep only the last part (MySQL typically returns unqualified)
        var dot = name.LastIndexOf('.');
        if (dot > 0 && dot + 1 < name.Length)
            name = name[(dot + 1)..].Trim().Trim('`').Trim('"').Trim('[').Trim(']').Trim();

        return name;
    }
    /// <summary>
    /// EN: Returns a DataTable with column metadata.
    /// PT: Retorna uma DataTable com metadados das colunas.
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
    /// EN: Gets a string value from the specified column.
    /// PT: Obtém valor string da coluna indicada.
    /// </summary>
    public override string GetString(int ordinal) => (string)this[ordinal];
    /// <summary>
    /// EN: Gets the value of the specified column.
    /// PT: Obtém o valor da coluna indicada.
    /// </summary>
    public override object GetValue(int ordinal)
    {
        var v = this[ordinal];
        return v is HashSet<string> hs ? string.Join(",", hs) : v;
    }
    /// <summary>
    /// EN: Copies the values of the current row into the provided array.
    /// PT: Copia os valores da linha atual para o array fornecido.
    /// </summary>
    /// <param name="values">EN: Destination array. PT: Array de destino.</param>
    /// <returns>EN: Number of copied columns. PT: Número de colunas copiadas.</returns>
    public override int GetValues(object[] values)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(values, nameof(values));

        var copied = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < copied; i++)
        {
            values[i] = this[i] ?? DBNull.Value;
        }

        return copied;
    }
    /// <summary>
    /// EN: Indicates whether the column value is DBNull.
    /// PT: Indica se o valor da coluna é DBNull.
    /// </summary>
    public override bool IsDBNull(int ordinal) => this[ordinal] == null || this[ordinal] == DBNull.Value;
    /// <summary>
    /// EN: Advances to the next result set.
    /// PT: Avança para o próximo conjunto de resultados.
    /// </summary>
    public override bool NextResult()
    {
        if (_currentResultSetIndex + 1 >= _resultSets.Count) return false;
        _currentResultSetIndex++;
        _currentIndex = -1;
        return true;
    }

    /// <summary>
    /// EN: Advances to the next row in the current set.
    /// PT: Avança para a próxima linha do conjunto atual.
    /// </summary>
    public override bool Read()
    {
        if (_currentIndex + 1 >= _resultSets[_currentResultSetIndex].Count) return false;
        _currentIndex++;
        return true;
    }

    /// <summary>
    /// EN: Returns an enumerator of the result sets.
    /// PT: Retorna um enumerador dos conjuntos de resultados.
    /// </summary>
    public override IEnumerator GetEnumerator() => _resultSets.GetEnumerator();
    /// <summary>
    /// EN: Disposes the data reader and associated resources.
    /// PT: Descarta o data leitor e recursos associados.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        if (disposedValue)
        {
            base.Dispose(disposing);
            return;
        }

        if (disposing)
        {
            foreach (var resultSet in _resultSets)
            {
                foreach (var row in resultSet)
                {
                    foreach (var value in row.Values)
                    {
                        if (value is IDisposable disposable)
                            disposable.Dispose();
                    }
                }
            }

            _resultSets.Clear();
            _columnsDic.Clear();
            _currentIndex = -1;
            _currentResultSetIndex = 0;
            _isClosed = true;
        }

        disposedValue = true;
        base.Dispose(disposing);
    }
}
