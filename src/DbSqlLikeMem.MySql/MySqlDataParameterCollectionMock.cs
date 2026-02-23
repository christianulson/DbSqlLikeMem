namespace DbSqlLikeMem.MySql;
/// <summary>
/// EN: Mock parameter collection for MySQL commands.
/// PT: Coleção de parâmetros simulado para comandos MySQL.
/// </summary>
public class MySqlDataParameterCollectionMock
    : DbParameterCollection, IList<MySqlParameter>
{
    internal readonly List<MySqlParameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
    UnsafeIndexOf(NormalizeParameterName(parameterName ?? ""));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? "", out var index) ? index : -1;

    private void AddParameter(MySqlParameter parameter, int index)
    {
        var normalizedParameterName = NormalizeParameterName(parameter.ParameterName);
        if (!string.IsNullOrEmpty(normalizedParameterName) && NormalizedIndexOf(normalizedParameterName) != -1)
            throw new ArgumentException(SqlExceptionMessages.ParameterAlreadyDefined(parameter.ParameterName));
        if (index < Items.Count)
        {
            foreach (var pair in DicItems.ToList())
            {
                if (pair.Value >= index)
                    DicItems[pair.Key] = pair.Value + 1;
            }
        }
        Items.Insert(index, parameter);
        if (!string.IsNullOrEmpty(normalizedParameterName))
            DicItems[normalizedParameterName] = index;
    }

    internal static string NormalizeParameterName(string name) =>
    name.Trim() switch
    {
        ['@' or '?', '`', .. var middle, '`'] => middle.Replace("``", "`"),
        ['@' or '?', '\'', .. var middle, '\''] => middle.Replace("''", "'"),
        ['@' or '?', '"', .. var middle, '"'] => middle.Replace("\"\"", "\""),
        ['@' or '?', .. var rest] => rest,
        { } other => other,
    };

    /// <summary>
    /// EN: Gets a parameter by index.
    /// PT: Obtém um parâmetro pelo índice.
    /// </summary>
    /// <param name="index">EN: Parameter index. PT: Índice do parâmetro.</param>
    /// <returns>EN: Parameter instance. PT: Instância do parâmetro.</returns>
    protected override DbParameter GetParameter(int index) => Items[index];

    /// <summary>
    /// EN: Gets a parameter by name.
    /// PT: Obtém um parâmetro pelo nome.
    /// </summary>
    /// <param name="parameterName">EN: Parameter name. PT: Nome do parâmetro.</param>
    /// <returns>EN: Parameter instance. PT: Instância do parâmetro.</returns>
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException(SqlExceptionMessages.ParameterNotFoundInCollection(parameterName), nameof(parameterName));
        return Items[index];
    }

    /// <summary>
    /// EN: Sets a parameter by index.
    /// PT: Define um parâmetro pelo índice.
    /// </summary>
    /// <param name="index">EN: Parameter index. PT: Índice do parâmetro.</param>
    /// <param name="value">EN: Parameter value. PT: Valor do parâmetro.</param>
    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        var newParameter = (MySqlParameter)value;
        var oldParameter = Items[index];
        var oldNormalizedParameterName = NormalizeParameterName(oldParameter.ParameterName);
        if (oldNormalizedParameterName is not null)
            DicItems.Remove(oldNormalizedParameterName);
        Items[index] = newParameter;
        var newNormalizedParameterName = NormalizeParameterName(newParameter.ParameterName);
        if (newNormalizedParameterName is not null)
            DicItems.Add(newNormalizedParameterName, index);
    }

    /// <summary>
    /// EN: Sets a parameter by name.
    /// PT: Define um parâmetro pelo nome.
    /// </summary>
    /// <param name="parameterName">EN: Parameter name. PT: Nome do parâmetro.</param>
    /// <param name="value">EN: Parameter value. PT: Valor do parâmetro.</param>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    /// <summary>
    /// EN: Gets or sets an item in this collection.
    /// PT: Obtém ou define um item desta coleção.
    /// </summary>
    public new MySqlParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    /// <summary>
    /// EN: Gets or sets an item in this collection.
    /// PT: Obtém ou define um item desta coleção.
    /// </summary>
    public new MySqlParameter this[string name]
    {
        get => (MySqlParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    /// <summary>
    /// EN: Gets or sets Count.
    /// PT: Obtém ou define Count.
    /// </summary>
    public override int Count => Items.Count;

    /// <summary>
    /// EN: Gets or sets SyncRoot.
    /// PT: Obtém ou define SyncRoot.
    /// </summary>
    public override object SyncRoot => true;

    /// <summary>
    /// EN: Implements Add.
    /// PT: Implementa Add.
    /// </summary>
    public MySqlParameter Add(string parameterName, DbType dbType)
    {
        var parameter = new MySqlParameter
        {
            ParameterName = parameterName,
            DbType = dbType,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Implements Add.
    /// PT: Implementa Add.
    /// </summary>
    public override int Add(object value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value,nameof(value));
        AddParameter((MySqlParameter)value, Items.Count);
        return Items.Count - 1;
    }

    /// <summary>
    /// EN: Implements Add.
    /// PT: Implementa Add.
    /// </summary>
    public MySqlParameter Add(MySqlParameter parameter)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(parameter, nameof(parameter));
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Implements Add.
    /// PT: Implementa Add.
    /// </summary>
    public MySqlParameter Add(string parameterName, MySqlDbType mySqlDbType) => Add(new(parameterName, mySqlDbType));
    /// <summary>
    /// EN: Implements Add.
    /// PT: Implementa Add.
    /// </summary>
    public MySqlParameter Add(string parameterName, MySqlDbType mySqlDbType, int size) => Add(new(parameterName, mySqlDbType, size));

    /// <summary>
    /// EN: Implements AddRange.
    /// PT: Implementa AddRange.
    /// </summary>
    public override void AddRange(Array values)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(values, nameof(values));
        foreach (var obj in values)
            Add(obj!);
    }

    /// <summary>
    /// EN: Implements AddWithValue.
    /// PT: Implementa AddWithValue.
    /// </summary>
    public MySqlParameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new MySqlParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Implements Contains.
    /// PT: Implementa Contains.
    /// </summary>
    public override bool Contains(object value)
        => value is MySqlParameter parameter && Items.Contains(parameter);

    /// <summary>
    /// EN: Implements Contains.
    /// PT: Implementa Contains.
    /// </summary>
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <summary>
    /// EN: Implements CopyTo.
    /// PT: Implementa CopyTo.
    /// </summary>
    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    /// <summary>
    /// EN: Implements Clear.
    /// PT: Implementa Clear.
    /// </summary>
    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    /// <summary>
    /// EN: Implements GetEnumerator.
    /// PT: Implementa GetEnumerator.
    /// </summary>
    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<MySqlParameter> IEnumerable<MySqlParameter>.GetEnumerator()
        => Items.GetEnumerator();

    /// <summary>
    /// EN: Implements IndexOf.
    /// PT: Implementa IndexOf.
    /// </summary>
    public override int IndexOf(object value)
        => value is MySqlParameter parameter ? Items.IndexOf(parameter) : -1;

    /// <summary>
    /// EN: Implements IndexOf.
    /// PT: Implementa IndexOf.
    /// </summary>
    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    /// <summary>
    /// EN: Implements Insert.
    /// PT: Implementa Insert.
    /// </summary>
    public override void Insert(int index, object? value)
        => AddParameter((MySqlParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    /// <summary>
    /// EN: Implements Insert.
    /// PT: Implementa Insert.
    /// </summary>
    public void Insert(int index, MySqlParameter item)
        => Items[index] = item;

    /// <summary>
    /// EN: Implements Remove.
    /// PT: Implementa Remove.
    /// </summary>
    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary>
    /// EN: Implements RemoveAt.
    /// PT: Implementa RemoveAt.
    /// </summary>
    public override void RemoveAt(string parameterName)
    => RemoveAt(IndexOf(parameterName));

    /// <summary>
    /// EN: Implements RemoveAt.
    /// PT: Implementa RemoveAt.
    /// </summary>
    public override void RemoveAt(int index)
    {
        var oldParameter = Items[index];
        var normalizedParameterName = NormalizeParameterName(oldParameter.ParameterName);
        if (normalizedParameterName is not null)
            DicItems.Remove(normalizedParameterName);
        Items.RemoveAt(index);

        foreach (var pair in DicItems.ToList())
        {
            if (pair.Value > index)
                DicItems[pair.Key] = pair.Value - 1;
        }
    }

    /// <summary>
    /// EN: Implements IndexOf.
    /// PT: Implementa IndexOf.
    /// </summary>
    public int IndexOf(MySqlParameter item)
        => Items.IndexOf(item);
    void ICollection<MySqlParameter>.Add(MySqlParameter item)
        => AddParameter(item, Items.Count);
    /// <summary>
    /// EN: Implements Contains.
    /// PT: Implementa Contains.
    /// </summary>
    public bool Contains(MySqlParameter item)
        => Items.Contains(item);
    /// <summary>
    /// EN: Implements CopyTo.
    /// PT: Implementa CopyTo.
    /// </summary>
    public void CopyTo(MySqlParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    /// <summary>
    /// EN: Implements Remove.
    /// PT: Implementa Remove.
    /// </summary>
    public bool Remove(MySqlParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}
