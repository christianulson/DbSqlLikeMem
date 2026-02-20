using System.Collections;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace DbSqlLikeMem.SqlServer;
/// <summary>
/// EN: Summary for SqlServerDataParameterCollectionMock.
/// PT: Resumo para SqlServerDataParameterCollectionMock.
/// </summary>
public class SqlServerDataParameterCollectionMock
    : DbParameterCollection, IList<SqlParameter>
{
    internal readonly List<SqlParameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
    UnsafeIndexOf(NormalizeParameterName(parameterName ?? ""));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? "", out var index) ? index : -1;

    private void AddParameter(SqlParameter parameter, int index)
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
    /// EN: Summary for GetParameter.
    /// PT: Resumo para GetParameter.
    /// </summary>
    protected override DbParameter GetParameter(int index) => Items[index];

    /// <summary>
    /// EN: Summary for GetParameter.
    /// PT: Resumo para GetParameter.
    /// </summary>
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException(SqlExceptionMessages.ParameterNotFoundInCollection(parameterName), nameof(parameterName));
        return Items[index];
    }

    /// <summary>
    /// EN: Summary for SetParameter.
    /// PT: Resumo para SetParameter.
    /// </summary>
    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        var newParameter = (SqlParameter)value;
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
    /// EN: Summary for SetParameter.
    /// PT: Resumo para SetParameter.
    /// </summary>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    /// <summary>
    /// EN: Summary for indexer.
    /// PT: Resumo para indexador.
    /// </summary>
    public new SqlParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    /// <summary>
    /// EN: Summary for indexer.
    /// PT: Resumo para indexador.
    /// </summary>
    public new SqlParameter this[string name]
    {
        get => (SqlParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override int Count => Items.Count;

    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public override object SyncRoot => true;

    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public SqlParameter Add(string parameterName, DbType dbType)
    {
        var parameter = new SqlParameter
        {
            ParameterName = parameterName,
            DbType = dbType,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public override int Add(object value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        AddParameter((SqlParameter)value, Items.Count);
        return Items.Count - 1;
    }

    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public SqlParameter Add(SqlParameter parameter)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(parameter, nameof(parameter));
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public SqlParameter Add(string parameterName, SqlDbType sqlDbType) => Add(new(parameterName, sqlDbType));
    /// <summary>
    /// EN: Summary for Add.
    /// PT: Resumo para Add.
    /// </summary>
    public SqlParameter Add(string parameterName, SqlDbType sqlDbType, int size) => Add(new(parameterName, sqlDbType, size));

    /// <summary>
    /// EN: Summary for AddRange.
    /// PT: Resumo para AddRange.
    /// </summary>
    public override void AddRange(Array values)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(values, nameof(values));
        foreach (var obj in values)
            Add(obj!);
    }

    /// <summary>
    /// EN: Summary for AddWithValue.
    /// PT: Resumo para AddWithValue.
    /// </summary>
    public SqlParameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new SqlParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Summary for Contains.
    /// PT: Resumo para Contains.
    /// </summary>
    public override bool Contains(object value)
        => value is SqlParameter parameter && Items.Contains(parameter);

    /// <summary>
    /// EN: Summary for Contains.
    /// PT: Resumo para Contains.
    /// </summary>
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <summary>
    /// EN: Summary for CopyTo.
    /// PT: Resumo para CopyTo.
    /// </summary>
    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    /// <summary>
    /// EN: Summary for Clear.
    /// PT: Resumo para Clear.
    /// </summary>
    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    /// <summary>
    /// EN: Summary for GetEnumerator.
    /// PT: Resumo para GetEnumerator.
    /// </summary>
    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<SqlParameter> IEnumerable<SqlParameter>.GetEnumerator()
        => Items.GetEnumerator();

    /// <summary>
    /// EN: Summary for IndexOf.
    /// PT: Resumo para IndexOf.
    /// </summary>
    public override int IndexOf(object value)
        => value is SqlParameter parameter ? Items.IndexOf(parameter) : -1;

    /// <summary>
    /// EN: Summary for IndexOf.
    /// PT: Resumo para IndexOf.
    /// </summary>
    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    /// <summary>
    /// EN: Summary for Insert.
    /// PT: Resumo para Insert.
    /// </summary>
    public override void Insert(int index, object? value)
        => AddParameter((SqlParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    /// <summary>
    /// EN: Summary for Insert.
    /// PT: Resumo para Insert.
    /// </summary>
    public void Insert(int index, SqlParameter item)
        => Items[index] = item;

    /// <summary>
    /// EN: Summary for Remove.
    /// PT: Resumo para Remove.
    /// </summary>
    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary>
    /// EN: Summary for RemoveAt.
    /// PT: Resumo para RemoveAt.
    /// </summary>
    public override void RemoveAt(string parameterName)
    => RemoveAt(IndexOf(parameterName));

    /// <summary>
    /// EN: Summary for RemoveAt.
    /// PT: Resumo para RemoveAt.
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
    /// EN: Summary for IndexOf.
    /// PT: Resumo para IndexOf.
    /// </summary>
    public int IndexOf(SqlParameter item)
        => Items.IndexOf(item);
    void ICollection<SqlParameter>.Add(SqlParameter item)
        => AddParameter(item, Items.Count);
    /// <summary>
    /// EN: Summary for Contains.
    /// PT: Resumo para Contains.
    /// </summary>
    public bool Contains(SqlParameter item)
        => Items.Contains(item);
    /// <summary>
    /// EN: Summary for CopyTo.
    /// PT: Resumo para CopyTo.
    /// </summary>
    public void CopyTo(SqlParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    /// <summary>
    /// EN: Summary for Remove.
    /// PT: Resumo para Remove.
    /// </summary>
    public bool Remove(SqlParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}
