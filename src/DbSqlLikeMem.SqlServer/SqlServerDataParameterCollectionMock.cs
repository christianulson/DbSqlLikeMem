using System.Collections;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace DbSqlLikeMem.SqlServer;
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
            throw new ArgumentException($"Parameter '{parameter.ParameterName}' has already been defined.");
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
        ['@' or '?', '`', .. var middle, '`'] => middle.Replace("``", "`", StringComparison.Ordinal),
        ['@' or '?', '\'', .. var middle, '\''] => middle.Replace("''", "'", StringComparison.Ordinal),
        ['@' or '?', '"', .. var middle, '"'] => middle.Replace("\"\"", "\"", StringComparison.Ordinal),
        ['@' or '?', .. var rest] => rest,
        { } other => other,
    };

    protected override DbParameter GetParameter(int index) => Items[index];

    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException($"Parameter '{parameterName}' not found in the collection", nameof(parameterName));
        return Items[index];
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullException.ThrowIfNull(value);
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

    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    public new SqlParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    public new SqlParameter this[string name]
    {
        get => (SqlParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    public override int Count => Items.Count;

    public override object SyncRoot => true;

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

    public override int Add(object value)
    {
        ArgumentNullException.ThrowIfNull(value);
        AddParameter((SqlParameter)value, Items.Count);
        return Items.Count - 1;
    }

    public SqlParameter Add(SqlParameter parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    public SqlParameter Add(string parameterName, SqlDbType sqlDbType) => Add(new(parameterName, sqlDbType));
    public SqlParameter Add(string parameterName, SqlDbType sqlDbType, int size) => Add(new(parameterName, sqlDbType, size));

    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);
        foreach (var obj in values)
            Add(obj!);
    }

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

    public override bool Contains(object value)
        => value is SqlParameter parameter && Items.Contains(parameter);

    public override bool Contains(string value)
        => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<SqlParameter> IEnumerable<SqlParameter>.GetEnumerator()
        => Items.GetEnumerator();

    public override int IndexOf(object value)
        => value is SqlParameter parameter ? Items.IndexOf(parameter) : -1;

    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    public override void Insert(int index, object? value)
        => AddParameter((SqlParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    public void Insert(int index, SqlParameter item)
        => Items[index] = item;

    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    public override void RemoveAt(string parameterName)
    => RemoveAt(IndexOf(parameterName));

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

    public int IndexOf(SqlParameter item)
        => Items.IndexOf(item);
    void ICollection<SqlParameter>.Add(SqlParameter item)
        => AddParameter(item, Items.Count);
    public bool Contains(SqlParameter item)
        => Items.Contains(item);
    public void CopyTo(SqlParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    public bool Remove(SqlParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}