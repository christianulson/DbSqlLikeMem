namespace DbSqlLikeMem;

internal sealed class ArrayRow : IReadOnlyDictionary<int, object?>
{
    private readonly object?[] _values;

    public ArrayRow(object?[] values)
    {
        _values = values;
    }

    public object? this[int key] => key >= 0 && key < _values.Length ? _values[key] : throw new KeyNotFoundException();

    public IEnumerable<int> Keys
    {
        get
        {
            for (var i = 0; i < _values.Length; i++)
                yield return i;
        }
    }

    public IEnumerable<object?> Values => _values;

    public int Count => _values.Length;

    public bool ContainsKey(int key) => key >= 0 && key < _values.Length;

    public bool TryGetValue(int key, out object? value)
    {
        if (key >= 0 && key < _values.Length)
        {
            value = _values[key];
            return true;
        }
        value = null;
        return false;
    }

    public IEnumerator<KeyValuePair<int, object?>> GetEnumerator()
    {
        for (var i = 0; i < _values.Length; i++)
            yield return new KeyValuePair<int, object?>(i, _values[i]);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
