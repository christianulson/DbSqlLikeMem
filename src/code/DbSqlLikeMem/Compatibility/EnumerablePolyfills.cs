#if NET462 || NETSTANDARD2_0
namespace System.Linq;

internal static class EnumerablePolyfills
{
    public static HashSet<TSource> ToHashSet<TSource>(this IEnumerable<TSource> source)
        => source.ToHashSet(comparer: null);

    public static HashSet<TSource> ToHashSet<TSource>(
        this IEnumerable<TSource> source,
        IEqualityComparer<TSource>? comparer)
    {
        DbSqlLikeMem.ArgumentNullExceptionCompatible.ThrowIfNull(source, nameof(source));
        return new HashSet<TSource>(source, comparer);
    }
}
#endif
