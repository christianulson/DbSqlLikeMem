namespace DbSqlLikeMem.Dialect;

internal class DictionaryProcess<T> : IDictionaryProcess<T>
        where T : ProcessDef
{
    protected readonly Dictionary<string, T> _inner = new(StringComparer.OrdinalIgnoreCase);

    protected virtual string NormalizeKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("Key cannot be null or whitespace.", nameof(key));

        return key.Trim();
    }

    protected virtual void OnItemAdding(string key, T value)
    {
    }

    protected virtual void OnItemAdded(string key, T value)
    {
    }

    protected virtual void OnItemRemoving(string key, T value)
    {
    }

    protected virtual void OnItemRemoved(string key, T value)
    {
    }

    protected virtual void OnItemReplacing(string key, T oldValue, T newValue)
    {
    }

    protected virtual void OnItemReplaced(string key, T oldValue, T newValue)
    {
    }

    protected virtual void OnClearing()
    {
    }

    protected virtual void OnCleared()
    {
    }

    public T this[string key]
    {
        get => _inner[NormalizeKey(key)];
        set
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            var normalizedKey = NormalizeKey(key);

            if (_inner.TryGetValue(normalizedKey, out var oldValue))
            {
                OnItemReplacing(normalizedKey, oldValue, value);
                _inner[normalizedKey] = value;
                OnItemReplaced(normalizedKey, oldValue, value);
                return;
            }

            OnItemAdding(normalizedKey, value);
            _inner[normalizedKey] = value;
            OnItemAdded(normalizedKey, value);
        }
    }

    public ICollection<string> Keys => _inner.Keys;

    public ICollection<T> Values => _inner.Values;

    public int Count => _inner.Count;

    public bool IsReadOnly => false;

    public virtual void Add(string key, T value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        var normalizedKey = NormalizeKey(key);
        if (_inner.ContainsKey(normalizedKey))
        {
            Console.WriteLine("Duplicate key detected: " + normalizedKey);
            return;
        }

        OnItemAdding(normalizedKey, value);
            _inner.Add(normalizedKey, value);
        OnItemAdded(normalizedKey, value);
    }

    public virtual bool ContainsKey(string key)
        => _inner.ContainsKey(NormalizeKey(key));

    public virtual bool Remove(string key)
    {
        var normalizedKey = NormalizeKey(key);

        if (!_inner.TryGetValue(normalizedKey, out var existing))
            return false;

        OnItemRemoving(normalizedKey, existing);
        var removed = _inner.Remove(normalizedKey);
        if (removed)
            OnItemRemoved(normalizedKey, existing);

        return removed;
    }

    public virtual bool TryGetValue(string key, out T value)
        => _inner.TryGetValue(NormalizeKey(key), out value!);

    public virtual void Add(KeyValuePair<string, T> item)
        => Add(item.Key, item.Value);

    public virtual void Clear()
    {
        if (_inner.Count == 0)
            return;

        OnClearing();
        _inner.Clear();
        OnCleared();
    }

    public virtual bool Contains(KeyValuePair<string, T> item)
    {
        var normalizedKey = NormalizeKey(item.Key);
        return ((ICollection<KeyValuePair<string, T>>)_inner)
            .Contains(new KeyValuePair<string, T>(normalizedKey, item.Value));
    }

    public virtual void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex)
        => ((ICollection<KeyValuePair<string, T>>)_inner).CopyTo(array, arrayIndex);

    public virtual bool Remove(KeyValuePair<string, T> item)
    {
        var normalizedKey = NormalizeKey(item.Key);

        if (!_inner.TryGetValue(normalizedKey, out var existing))
            return false;

        if (!EqualityComparer<T>.Default.Equals(existing, item.Value))
            return false;

        OnItemRemoving(normalizedKey, existing);
        var removed = ((ICollection<KeyValuePair<string, T>>)_inner)
            .Remove(new KeyValuePair<string, T>(normalizedKey, item.Value));

        if (removed)
            OnItemRemoved(normalizedKey, existing);

        return removed;
    }

    public IEnumerator<KeyValuePair<string, T>> GetEnumerator()
        => _inner.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => GetEnumerator();
}