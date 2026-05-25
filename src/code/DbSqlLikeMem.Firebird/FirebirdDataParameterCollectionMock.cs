namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Mock parameter collection for Firebird commands.
/// PT-br: Coleção de parâmetros simulada para comandos Firebird.
/// </summary>
public class FirebirdDataParameterCollectionMock
    : DbParameterCollection, IList<FbParameter>
{
    internal readonly List<FbParameter> Items = [];
    internal readonly Dictionary<string, int> DicItems = new(StringComparer.OrdinalIgnoreCase);

    internal int NormalizedIndexOf(string? parameterName) =>
        UnsafeIndexOf(NormalizeParameterName(parameterName ?? string.Empty));

    internal int UnsafeIndexOf(string? normalizedParameterName) =>
        DicItems.TryGetValue(normalizedParameterName ?? string.Empty, out var index) ? index : -1;

    private void AddParameter(FbParameter parameter, int index)
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

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => Items[index];

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index == -1)
            throw new ArgumentException(SqlExceptionMessages.ParameterNotFoundInCollection(parameterName), nameof(parameterName));

        return Items[index];
    }

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        var newParameter = (FbParameter)value;
        var oldParameter = Items[index];
        var oldNormalizedParameterName = NormalizeParameterName(oldParameter.ParameterName);
        if (!string.IsNullOrEmpty(oldNormalizedParameterName))
            DicItems.Remove(oldNormalizedParameterName);

        Items[index] = newParameter;
        var newNormalizedParameterName = NormalizeParameterName(newParameter.ParameterName);
        if (!string.IsNullOrEmpty(newNormalizedParameterName))
            DicItems.Add(newNormalizedParameterName, index);
    }

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value)
        => SetParameter(IndexOf(parameterName), value);

    /// <summary>
    /// EN: Gets or sets the parameter at the specified index.
    /// PT-br: Obtem ou define o parametro no indice especificado.
    /// </summary>
    public new FbParameter this[int index]
    {
        get => Items[index];
        set => SetParameter(index, value);
    }

    /// <summary>
    /// EN: Gets or sets the parameter with the specified name.
    /// PT-br: Obtem ou define o parametro com o nome especificado.
    /// </summary>
    public new FbParameter this[string name]
    {
        get => (FbParameter)GetParameter(name);
        set => SetParameter(name, value);
    }

    /// <inheritdoc />
    public override int Count => Items.Count;

    /// <inheritdoc />
    public override object SyncRoot => true;

    /// <summary>
    /// EN: Adds a new parameter with the specified name and type.
    /// PT-br: Adiciona um novo parametro com o nome e o tipo especificados.
    /// </summary>
    public FbParameter Add(string parameterName, DbType dbType)
    {
        var parameter = new FbParameter
        {
            ParameterName = parameterName,
            DbType = dbType,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <inheritdoc />
    public override int Add(object value)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
        AddParameter((FbParameter)value, Items.Count);
        return Items.Count - 1;
    }

    /// <summary>
    /// EN: Adds an existing Firebird parameter to the collection.
    /// PT-br: Adiciona um parametro Firebird ja existente a colecao.
    /// </summary>
    public FbParameter Add(FbParameter parameter)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(parameter, nameof(parameter));
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <summary>
    /// EN: Adds a new parameter with the specified Firebird type.
    /// PT-br: Adiciona um novo parametro com o tipo Firebird especificado.
    /// </summary>
    public FbParameter Add(string parameterName, FbDbType fbDbType) => Add(new(parameterName, fbDbType));

    /// <summary>
    /// EN: Adds a new parameter with the specified Firebird type and size.
    /// PT-br: Adiciona um novo parametro com o tipo Firebird e o tamanho especificados.
    /// </summary>
    public FbParameter Add(string parameterName, FbDbType fbDbType, int size) => Add(new(parameterName, fbDbType, size));

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(values, nameof(values));
        foreach (var obj in values)
            Add(obj!);
    }

    /// <summary>
    /// EN: Adds a new parameter and assigns its value immediately.
    /// PT-br: Adiciona um novo parametro e atribui seu valor imediatamente.
    /// </summary>
    public FbParameter AddWithValue(string parameterName, object? value)
    {
        var parameter = new FbParameter
        {
            ParameterName = parameterName,
            Value = value,
        };
        AddParameter(parameter, Items.Count);
        return parameter;
    }

    /// <inheritdoc />
    public override bool Contains(object value)
        => value is FbParameter parameter && Items.Contains(parameter);

    /// <inheritdoc />
    public override bool Contains(string value)
        => IndexOf(value) != -1;

    /// <inheritdoc />
    public override void CopyTo(Array array, int index)
        => ((ICollection)Items).CopyTo(array, index);

    /// <inheritdoc />
    public override void Clear()
    {
        Items.Clear();
        DicItems.Clear();
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator()
        => Items.GetEnumerator();

    /// <inheritdoc />
    IEnumerator<FbParameter> IEnumerable<FbParameter>.GetEnumerator()
        => Items.GetEnumerator();

    /// <inheritdoc />
    public override int IndexOf(object value)
        => value is FbParameter parameter ? Items.IndexOf(parameter) : -1;

    /// <inheritdoc />
    public override int IndexOf(string parameterName) => NormalizedIndexOf(parameterName);

    /// <inheritdoc />
    public override void Insert(int index, object? value)
        => AddParameter((FbParameter)(value ?? throw new ArgumentNullException(nameof(value))), index);

    /// <summary>
    /// EN: Inserts a Firebird parameter at the specified index.
    /// PT-br: Insere um parametro Firebird no indice especificado.
    /// </summary>
    public void Insert(int index, FbParameter item)
        => Items[index] = item;

    /// <inheritdoc />
    public override void Remove(object? value)
        => RemoveAt(IndexOf(value ?? throw new ArgumentNullException(nameof(value))));

    /// <inheritdoc />
    public override void RemoveAt(string parameterName)
        => RemoveAt(IndexOf(parameterName));

    /// <inheritdoc />
    public override void RemoveAt(int index)
    {
        var oldParameter = Items[index];
        var normalizedParameterName = NormalizeParameterName(oldParameter.ParameterName);
        if (!string.IsNullOrEmpty(normalizedParameterName))
            DicItems.Remove(normalizedParameterName);

        Items.RemoveAt(index);

        foreach (var pair in DicItems.ToList())
        {
            if (pair.Value > index)
                DicItems[pair.Key] = pair.Value - 1;
        }
    }

    /// <summary>
    /// EN: Gets the index of the specified Firebird parameter.
    /// PT-br: Obtem o indice do parametro Firebird especificado.
    /// </summary>
    public int IndexOf(FbParameter item)
        => Items.IndexOf(item);

    /// <inheritdoc />
    void ICollection<FbParameter>.Add(FbParameter item)
        => AddParameter(item, Items.Count);

    /// <summary>
    /// EN: Determines whether the collection contains the specified Firebird parameter.
    /// PT-br: Determina se a colecao contem o parametro Firebird especificado.
    /// </summary>
    public bool Contains(FbParameter item)
        => Items.Contains(item);

    /// <summary>
    /// EN: Copies the Firebird parameters to the specified array.
    /// PT-br: Copia os parametros Firebird para o array especificado.
    /// </summary>
    public void CopyTo(FbParameter[] array, int arrayIndex)
        => Items.CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Removes the specified Firebird parameter from the collection.
    /// PT-br: Remove o parametro Firebird especificado da colecao.
    /// </summary>
    public bool Remove(FbParameter item)
    {
        var i = IndexOf(item ?? throw new ArgumentNullException(nameof(item)));
        if (i == -1)
            return false;

        RemoveAt(i);
        return true;
    }
}
