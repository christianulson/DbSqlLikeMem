namespace DbSqlLikeMem;

/// <summary>
/// EN: Represents a composite key for fast index lookups without string allocation.
/// PT: Representa uma chave composta para buscas rápidas de índice sem alocação de string.
/// </summary>
public readonly struct IndexKey : IEquatable<IndexKey>
{
    private readonly object? _v1;
    private readonly object? _v2;
    private readonly object? _v3;
    private readonly object?[]? _more;
    private readonly int _count;
    private readonly int _hashCode;

    /// <summary>
    /// EN: Initializes a new IndexKey with a single value.
    /// PT: Inicializa uma nova IndexKey com um único valor.
    /// </summary>
    /// <param name="value">EN: Key value. PT: Valor da chave.</param>
    public IndexKey(object? value)
    {
        _v1 = value;
        _v2 = null;
        _v3 = null;
        _more = null;
        _count = 1;
        _hashCode = ComputeSingleHashCode(value);
    }

    /// <summary>
    /// EN: Initializes a new IndexKey with two values.
    /// PT: Inicializa uma nova IndexKey com dois valores.
    /// </summary>
    public IndexKey(object? v1, object? v2)
    {
        _v1 = v1;
        _v2 = v2;
        _v3 = null;
        _more = null;
        _count = 2;
        var hash = new HashCode();
        AddValueToHash(ref hash, v1);
        AddValueToHash(ref hash, v2);
        _hashCode = hash.ToHashCode();
    }

    /// <summary>
    /// EN: Initializes a new IndexKey with three values.
    /// PT: Inicializa uma nova IndexKey com três valores.
    /// </summary>
    public IndexKey(object? v1, object? v2, object? v3)
    {
        _v1 = v1;
        _v2 = v2;
        _v3 = v3;
        _more = null;
        _count = 3;
        var hash = new HashCode();
        AddValueToHash(ref hash, v1);
        AddValueToHash(ref hash, v2);
        AddValueToHash(ref hash, v3);
        _hashCode = hash.ToHashCode();
    }

    /// <summary>
    /// EN: Initializes a new IndexKey with multiple values.
    /// PT: Inicializa uma nova IndexKey com múltiplos valores.
    /// </summary>
    /// <param name="values">EN: Key values in order. PT: Valores da chave em ordem.</param>
    public IndexKey(object?[] values)
    {
        _count = values.Length;
        if (_count == 1)
        {
            _v1 = values[0];
            _v2 = null;
            _v3 = null;
            _more = null;
            _hashCode = ComputeSingleHashCode(_v1);
        }
        else if (_count == 2)
        {
            _v1 = values[0];
            _v2 = values[1];
            _v3 = null;
            _more = null;
            var hash = new HashCode();
            AddValueToHash(ref hash, _v1);
            AddValueToHash(ref hash, _v2);
            _hashCode = hash.ToHashCode();
        }
        else if (_count == 3)
        {
            _v1 = values[0];
            _v2 = values[1];
            _v3 = values[2];
            _more = null;
            var hash = new HashCode();
            AddValueToHash(ref hash, _v1);
            AddValueToHash(ref hash, _v2);
            AddValueToHash(ref hash, _v3);
            _hashCode = hash.ToHashCode();
        }
        else
        {
            _v1 = null;
            _v2 = null;
            _v3 = null;
            _more = values;
            _hashCode = ComputeCompositeHashCode(values);
        }
    }

    /// <summary>
    /// EN: Gets the key values.
    /// PT: Obtém os valores da chave.
    /// </summary>
    public IReadOnlyList<object?> Values
    {
        get
        {
            if (_more is not null) return _more;
            if (_count == 1) return [_v1];
            if (_count == 2) return [_v1, _v2];
            if (_count == 3) return [_v1, _v2, _v3];
            return [];
        }
    }

    /// <summary>
    /// EN: Formats the key for error messages.
    /// PT: Formata a chave para mensagens de erro.
    /// </summary>
    public override string ToString()
    {
        if (_more is not null)
            return string.Join(" | ", _more.Select(v => v?.ToString() ?? SqlConst.NULL));
        if (_count == 1) return _v1?.ToString() ?? SqlConst.NULL;
        if (_count == 2) return $"{_v1?.ToString() ?? SqlConst.NULL} | {_v2?.ToString() ?? SqlConst.NULL}";
        if (_count == 3) return $"{_v1?.ToString() ?? SqlConst.NULL} | {_v2?.ToString() ?? SqlConst.NULL} | {_v3?.ToString() ?? SqlConst.NULL}";
        return SqlConst.NULL;
    }

    /// <inheritdoc />
    public bool Equals(IndexKey other)
    {
        if (_count != other._count) return false;
        if (_more is not null)
        {
            if (other._more is null) return false;
            for (int i = 0; i < _more.Length; i++)
                if (!EqualsCore(_more[i], other._more[i])) return false;
            return true;
        }

        if (other._more is not null) return false;
        if (_count >= 1 && !EqualsCore(_v1, other._v1)) return false;
        if (_count >= 2 && !EqualsCore(_v2, other._v2)) return false;
        if (_count >= 3 && !EqualsCore(_v3, other._v3)) return false;

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is IndexKey other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _hashCode;

    private static int ComputeSingleHashCode(object? val)
    {
        var hash = new HashCode();
        AddValueToHash(ref hash, val);
        return hash.ToHashCode();
    }

    private static int ComputeCompositeHashCode(object?[] values)
    {
        var hash = new HashCode();
        foreach (var val in values)
            AddValueToHash(ref hash, val);
        return hash.ToHashCode();
    }

    private static void AddValueToHash(ref HashCode hash, object? val)
    {
        if (val is null || val is DBNull)
            hash.Add(0);
        else if (val is string s)
            hash.Add(StringComparer.OrdinalIgnoreCase.GetHashCode(s));
        else
            hash.Add(val);
    }

    private static bool EqualsCore(object? x, object? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || x is DBNull) return y is null || y is DBNull;
        if (y is null || y is DBNull) return false;

        if (x is string sx && y is string sy)
            return string.Equals(sx, sy, StringComparison.OrdinalIgnoreCase);

        return x.Equals(y);
    }
}
