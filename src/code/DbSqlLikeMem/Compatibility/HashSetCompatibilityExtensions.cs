namespace DbSqlLikeMem;

internal static class HashSetCompatibilityExtensions
{
    internal static HashSet<T> Create<T>()
        where T : notnull
        => new HashSet<T>();

    internal static HashSet<T> Create<T>(IEqualityComparer<T> comparer)
        where T : notnull
        => new(comparer);

    internal static HashSet<T> Create<T>(int capacity)
        where T : notnull
    {
#if NET8_0_OR_GREATER
        return new HashSet<T>(capacity);
#else
        return new HashSet<T>();
#endif
    }

    internal static HashSet<T> Create<T>(
        int capacity,
        IEqualityComparer<T> comparer)
        where T : notnull
    {
#if NET8_0_OR_GREATER
        return new HashSet<T>(capacity, comparer);
#else
        return new HashSet<T>(comparer);
#endif
    }

    internal static HashSet<T> Create<T>(
        IEnumerable<T> collection,
        IEqualityComparer<T>? comparer = null)
        where T : notnull
        => comparer is null
            ? new HashSet<T>(collection)
            : new HashSet<T>(collection, comparer);
}
