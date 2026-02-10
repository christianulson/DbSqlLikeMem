using IBM.Data.Db2;
using System.Collections;
using System.Data.Common;

namespace DbSqlLikeMem.Db2;
/// <summary>
/// EN: Mock parameter collection for DB2 commands.
/// PT: Coleção de parâmetros mock para comandos DB2.
/// </summary>
public class Db2DataParameterCollectionMock
    : DbParameterCollection, IList<DB2Parameter>
{
    internal readonly List<DB2Parameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
    UnsafeIndexOf(NormalizeParameterName(parameterName ?? ""));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? "", out var index) ? index : -1;

    private void AddParameter(DB2Parameter parameter, int index)
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
        var newParameter = (DB2Parameter)value;
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
    /// Auto-generated summary.
    /// </summary>
    public new DB2Parameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public new DB2Parameter this[string name]
    {
        get => (DB2Parameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override int Count => Items.Count;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override object SyncRoot => true;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DB2Parameter Add(string parameterName, DbType dbType)
    {
        var parameter = new DB2Parameter
        {
            ParameterName = parameterName,
            DbType = dbType,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override int Add(object value)
    {
        ArgumentNullException.ThrowIfNull(value);
        AddParameter((DB2Parameter)value, Items.Count);
        return Items.Count - 1;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DB2Parameter Add(DB2Parameter parameter)
    {
        ArgumentNullException.ThrowIfNull(parameter);
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DB2Parameter Add(string parameterName, DB2Type mySqlDbType) => Add(new(parameterName, mySqlDbType));
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DB2Parameter Add(string parameterName, DB2Type mySqlDbType, int size) => Add(new(parameterName, mySqlDbType, size));

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void AddRange(Array values)
    {
        ArgumentNullException.ThrowIfNull(values);
        foreach (var obj in values)
            Add(obj!);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DB2Parameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new DB2Parameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool Contains(object value)
        => value is DB2Parameter parameter && Items.Contains(parameter);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();
    IEnumerator<DB2Parameter> IEnumerable<DB2Parameter>.GetEnumerator()
        => Items.GetEnumerator();

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override int IndexOf(object value)
        => value is DB2Parameter parameter ? Items.IndexOf(parameter) : -1;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void Insert(int index, object? value)
        => AddParameter((DB2Parameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Insert(int index, DB2Parameter item)
        => Items[index] = item;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public override void RemoveAt(string parameterName)
    => RemoveAt(IndexOf(parameterName));

    /// <summary>
    /// Auto-generated summary.
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
    /// Auto-generated summary.
    /// </summary>
    public int IndexOf(DB2Parameter item)
        => Items.IndexOf(item);
    void ICollection<DB2Parameter>.Add(DB2Parameter item)
        => AddParameter(item, Items.Count);
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public bool Contains(DB2Parameter item)
        => Items.Contains(item);
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void CopyTo(DB2Parameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public bool Remove(DB2Parameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;
        RemoveAt(i);
        return true;
    }
}
