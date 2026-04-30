using Microsoft.Data.Sqlite;
using System.Collections;

namespace DbSqlLikeMem.Sqlite;
/// <summary>
/// EN: Represents Sqlite Data Parameter Collection Mock.
/// PT: Representa Sqlite Data Parameter Collection simulado.
/// </summary>
public class SqliteDataParameterCollectionMock
    : DbParameterCollection, IList<SqliteParameter>
{
    internal readonly List<SqliteParameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
    UnsafeIndexOf(NormalizeParameterName(parameterName ?? ""));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? "", out var index) ? index : -1;

    private void AddParameter(SqliteParameter parameter, int index)
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
    /// EN: Gets parameter.
    /// PT: Obtém parâmetro.
    /// </summary>
    protected override DbParameter GetParameter(int index) => Items[index];

    /// <summary>
    /// EN: Gets parameter.
    /// PT: Obtém parâmetro.
    /// </summary>
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException(SqlExceptionMessages.ParameterNotFoundInCollection(parameterName), nameof(parameterName));
        return Items[index];
    }

    /// <summary>
    /// EN: Sets parameter.
    /// PT: Define parâmetro.
    /// </summary>
    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        var newParameter = (SqliteParameter)value;
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
    /// EN: Sets parameter.
    /// PT: Define parâmetro.
    /// </summary>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    /// <summary>
    /// EN: Sets parameter.
    /// PT: Define parâmetro.
    /// </summary>
    public new SqliteParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    /// <summary>
    /// EN: Gets parameter.
    /// PT: Obtém parâmetro.
    /// </summary>
    public new SqliteParameter this[string name]
    {
        get => (SqliteParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    /// <summary>
    /// EN: Gets or sets count.
    /// PT: Obtém ou define count.
    /// </summary>
    public override int Count => Items.Count;

    /// <summary>
    /// EN: Gets or sets sync root.
    /// PT: Obtém ou define sync root.
    /// </summary>
    public override object SyncRoot => true;

    /// <summary>
    /// EN: Performs the add operation.
    /// PT: Executa a operação de add.
    /// </summary>
    public SqliteParameter Add(string parameterName, DbType dbType)
    {
        var parameter = new SqliteParameter
        {
            ParameterName = parameterName,
            DbType = dbType,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Performs the add operation.
    /// PT: Executa a operação de add.
    /// </summary>
    public override int Add(object value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        AddParameter((SqliteParameter)value, Items.Count);
        return Items.Count - 1;
    }

    /// <summary>
    /// EN: Performs the add operation.
    /// PT: Executa a operação de add.
    /// </summary>
    public SqliteParameter Add(SqliteParameter parameter)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(parameter, nameof(parameter));
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Performs the add operation.
    /// PT: Executa a operação de add.
    /// </summary>
    public SqliteParameter Add(string parameterName, SqliteType mySqlDbType) => Add(new(parameterName, mySqlDbType));
    /// <summary>
    /// EN: Performs the add operation.
    /// PT: Executa a operação de add.
    /// </summary>
    public SqliteParameter Add(string parameterName, SqliteType mySqlDbType, int size) => Add(new(parameterName, mySqlDbType, size));

    /// <summary>
    /// EN: Represents Add Range.
    /// PT: Representa Add Range.
    /// </summary>
    public override void AddRange(Array values)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(values, nameof(values));
        foreach (var obj in values)
            Add(obj!);
    }

    /// <summary>
    /// EN: Represents Add With Value.
    /// PT: Representa Add With Value.
    /// </summary>
    public SqliteParameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new SqliteParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Performs the contains operation.
    /// PT: Executa a operação de contains.
    /// </summary>
    public override bool Contains(object value)
        => value is SqliteParameter parameter && Items.Contains(parameter);

    /// <summary>
    /// EN: Performs the contains operation.
    /// PT: Executa a operação de contains.
    /// </summary>
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <summary>
    /// EN: Performs the copy to operation.
    /// PT: Executa a operação de copy to.
    /// </summary>
    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    /// <summary>
    /// EN: Performs the clear operation.
    /// PT: Executa a operação de clear.
    /// </summary>
    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    /// <summary>
    /// EN: Gets enumerator.
    /// PT: Obtém enumerador.
    /// </summary>
    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<SqliteParameter> IEnumerable<SqliteParameter>.GetEnumerator()
        => Items.GetEnumerator();

    /// <summary>
    /// EN: Performs the index of operation.
    /// PT: Executa a operação de index of.
    /// </summary>
    public override int IndexOf(object value)
        => value is SqliteParameter parameter ? Items.IndexOf(parameter) : -1;

    /// <summary>
    /// EN: Performs the index of operation.
    /// PT: Executa a operação de index of.
    /// </summary>
    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    /// <summary>
    /// EN: Performs the insert operation.
    /// PT: Executa a operação de insert.
    /// </summary>
    public override void Insert(int index, object? value)
        => AddParameter((SqliteParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    /// <summary>
    /// EN: Performs the insert operation.
    /// PT: Executa a operação de insert.
    /// </summary>
    public void Insert(int index, SqliteParameter item)
        => Items[index] = item;

    /// <summary>
    /// EN: Performs the remove operation.
    /// PT: Executa a operação de remove.
    /// </summary>
    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary>
    /// EN: Performs the remove at operation.
    /// PT: Executa a operação de remove at.
    /// </summary>
    public override void RemoveAt(string parameterName)
    => RemoveAt(IndexOf(parameterName));

    /// <summary>
    /// EN: Performs the remove at operation.
    /// PT: Executa a operação de remove at.
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
    /// EN: Performs the index of operation.
    /// PT: Executa a operação de index of.
    /// </summary>
    public int IndexOf(SqliteParameter item)
        => Items.IndexOf(item);
    void ICollection<SqliteParameter>.Add(SqliteParameter item)
        => AddParameter(item, Items.Count);
    /// <summary>
    /// EN: Performs the contains operation.
    /// PT: Executa a operação de contains.
    /// </summary>
    public bool Contains(SqliteParameter item)
        => Items.Contains(item);
    /// <summary>
    /// EN: Performs the copy to operation.
    /// PT: Executa a operação de copy to.
    /// </summary>
    public void CopyTo(SqliteParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    /// <summary>
    /// EN: Performs the remove operation.
    /// PT: Executa a operação de remove.
    /// </summary>
    public bool Remove(SqliteParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}
