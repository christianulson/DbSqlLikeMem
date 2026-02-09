using System.Collections;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace DbSqlLikeMem.Oracle;
public class OracleDataParameterCollectionMock
    : DbParameterCollection, IList<OracleParameter>
{
    internal readonly List<OracleParameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
    UnsafeIndexOf(NormalizeParameterName(parameterName ?? ""));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? "", out var index) ? index : -1;

    private void AddParameter(OracleParameter parameter, int index)
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
        //parameter.ParameterCollection = this;
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
            throw new ArgumentException($"Parameter '{parameterName}' not found in the collection", nameof(parameterName));
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
        ArgumentNullException.ThrowIfNull(value);
        var newParameter = (OracleParameter)value;
        var oldParameter = Items[index];
        var oldNormalizedParameterName = NormalizeParameterName(oldParameter.ParameterName);
        if (oldNormalizedParameterName is not null)
            DicItems.Remove(oldNormalizedParameterName);
        //oldParameter.ParameterCollection = null;
        Items[index] = newParameter;
        var newNormalizedParameterName = NormalizeParameterName(newParameter.ParameterName);
        if (newNormalizedParameterName is not null)
            DicItems.Add(newNormalizedParameterName, index);
        //newParameter.ParameterCollection = this;
    }

    /// <summary>
    /// EN: Sets a parameter by name.
    /// PT: Define um parâmetro pelo nome.
    /// </summary>
    /// <param name="parameterName">EN: Parameter name. PT: Nome do parâmetro.</param>
    /// <param name="value">EN: Parameter value. PT: Valor do parâmetro.</param>
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    public new OracleParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    public new OracleParameter this[string name]
    {
        get => (OracleParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    public override int Count => Items.Count;

    public override object SyncRoot => true;

    public OracleParameter Add(string parameterName, DbType dbType)
    {
        var parameter = new OracleParameter
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
        AddParameter((OracleParameter)value, Items.Count);
        return Items.Count - 1;
    }

    public OracleParameter Add(OracleParameter parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    public OracleParameter Add(string parameterName, OracleDbType OracleDbType) => Add(new(parameterName, OracleDbType));
    public OracleParameter Add(string parameterName, OracleDbType OracleDbType, int size) => Add(new(parameterName, OracleDbType, size));

    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);
        foreach (var obj in values)
            Add(obj!);
    }

    public OracleParameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new OracleParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    public override bool Contains(object value)
        => value is OracleParameter parameter && Items.Contains(parameter);

    public override bool Contains(string value)
        => IndexOf(value) != -1;

    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    public override void Clear()
    {
        //foreach (var parameter in Items)
        //    parameter.ParameterCollection = null;
        Items.Clear();
        DicItems.Clear();
    }

    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<OracleParameter> IEnumerable<OracleParameter>.GetEnumerator()
        => Items.GetEnumerator();

    public override int IndexOf(object value)
        => value is OracleParameter parameter ? Items.IndexOf(parameter) : -1;

    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    public override void Insert(int index, object? value)
        => AddParameter((OracleParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    public void Insert(int index, OracleParameter item)
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
        //oldParameter.ParameterCollection = null;
        Items.RemoveAt(index);

        foreach (var pair in DicItems.ToList())
        {
            if (pair.Value > index)
                DicItems[pair.Key] = pair.Value - 1;
        }
    }

    public int IndexOf(OracleParameter item)
        => Items.IndexOf(item);
    void ICollection<OracleParameter>.Add(OracleParameter item)
        => AddParameter(item, Items.Count);
    public bool Contains(OracleParameter item)
        => Items.Contains(item);
    public void CopyTo(OracleParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    public bool Remove(OracleParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}
