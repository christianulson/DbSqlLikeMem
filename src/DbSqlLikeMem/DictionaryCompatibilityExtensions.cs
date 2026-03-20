using System.Collections.Generic;

namespace DbSqlLikeMem;

internal static class DictionaryCompatibilityExtensions
{
    internal static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
        where TKey : notnull
    {
        if (dictionary.ContainsKey(key))
            return false;

        dictionary.Add(key, value);
        return true;
    }

    internal static int EnsureCapacity<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, int capacity)
        where TKey : notnull
        => dictionary.Count;
}
