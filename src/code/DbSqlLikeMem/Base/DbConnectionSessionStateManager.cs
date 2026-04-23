namespace DbSqlLikeMem;

internal sealed class DbConnectionSessionStateManager
{
    private readonly Dictionary<string, long> _sessionSequenceValues =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, object?> _sessionContextValues =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, object?> _transactionContextValues =
        new(StringComparer.OrdinalIgnoreCase);
    private byte[]? _contextInfo;
    private string? _lastSessionSequenceKey;

    public void ClearAll()
    {
        _sessionSequenceValues.Clear();
        _sessionContextValues.Clear();
        _transactionContextValues.Clear();
        _contextInfo = null;
        _lastSessionSequenceKey = null;
    }

    public void ClearTransactionContextValues()
    {
        if (_transactionContextValues.Count > 0)
            _transactionContextValues.Clear();
    }

    public void SetSessionSequenceValue(
        string sequenceName,
        long value,
        string schemaName)
    {
        var key = BuildSessionSequenceKey(sequenceName, schemaName);
        _sessionSequenceValues[key] = value;
        _lastSessionSequenceKey = key;
    }

    public bool TryGetSessionSequenceValue(
        string sequenceName,
        out long value,
        string schemaName)
        => _sessionSequenceValues.TryGetValue(BuildSessionSequenceKey(sequenceName, schemaName), out value);

    public bool TryGetLastSessionSequenceValue(out long value)
    {
        value = default;
        return _lastSessionSequenceKey is not null
            && _sessionSequenceValues.TryGetValue(_lastSessionSequenceKey, out value);
    }

    public void SetSessionContextValue(string key, object? value)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (value is null)
        {
            _sessionContextValues.Remove(key);
            return;
        }

        _sessionContextValues[key] = value;
    }

    public bool TryGetSessionContextValue(string key, out object? value)
        => _sessionContextValues.TryGetValue(key, out value);

    public void SetTransactionContextValue(string key, object? value)
    {
        if (string.IsNullOrWhiteSpace(key))
            return;

        if (value is null)
        {
            _transactionContextValues.Remove(key);
            return;
        }

        _transactionContextValues[key] = value;
    }

    public bool TryGetTransactionContextValue(string key, out object? value)
        => _transactionContextValues.TryGetValue(key, out value);

    public void SetContextInfo(byte[]? value)
        => _contextInfo = value;

    public byte[]? GetContextInfo()
        => _contextInfo;

    public void ClearSessionSequenceValue(
        string sequenceName,
        string schemaName)
    {
        var key = BuildSessionSequenceKey(sequenceName, schemaName);
        _sessionSequenceValues.Remove(key);
        if (string.Equals(_lastSessionSequenceKey, key, StringComparison.OrdinalIgnoreCase))
            _lastSessionSequenceKey = null;
    }

    private string BuildSessionSequenceKey(
        string sequenceName,
        string schemaName)
        => $"{schemaName}:{sequenceName.NormalizeName()}";
}
