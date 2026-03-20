using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal sealed class AstSubqueryEvaluationCache
{
    private readonly ConcurrentDictionary<string, bool> _exists = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, List<object?>> _firstColumnValues = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, object> _operationData = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ScalarValueBox> _scalars = new(StringComparer.Ordinal);

    public bool GetOrAddExists(string cacheKey, Func<string, bool> valueFactory)
        => _exists.GetOrAdd(cacheKey, valueFactory);

    public List<object?> GetOrAddFirstColumnValues(string cacheKey, Func<string, List<object?>> valueFactory)
        => _firstColumnValues.GetOrAdd(cacheKey, valueFactory);

    public bool TryGetFirstColumnValues(string cacheKey, out List<object?>? value)
    {
        if (!_firstColumnValues.TryGetValue(cacheKey, out var cached))
        {
            value = null;
            return false;
        }

        value = cached;
        return true;
    }

    public bool TryGetOperationData<T>(string cacheKey, out T? value)
        where T : class
    {
        if (!_operationData.TryGetValue(cacheKey, out var cached))
        {
            value = null;
            return false;
        }

        value = cached as T;
        return value is not null;
    }

    public T GetOrAddOperationData<T>(string cacheKey, Func<string, T> valueFactory)
        where T : class
        => (T)_operationData.GetOrAdd(cacheKey, key => valueFactory(key)!);

    public object? GetOrAddScalar(string cacheKey, Func<string, object?> valueFactory)
        => _scalars.GetOrAdd(cacheKey, key => new ScalarValueBox(valueFactory(key))).Value;

    public void Clear()
    {
        _exists.Clear();
        _firstColumnValues.Clear();
        _operationData.Clear();
        _scalars.Clear();
    }

    private sealed record ScalarValueBox(object? Value);
}
