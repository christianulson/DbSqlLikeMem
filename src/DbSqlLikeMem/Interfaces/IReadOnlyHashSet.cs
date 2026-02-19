using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Interfaces;

public interface IReadOnlyHashSet<T> : IReadOnlyCollection<T>, IDeserializationCallback, ISerializable
{

    IEqualityComparer<T> Comparer { get; }

    bool Contains(T item);

    void CopyTo(T[] array);

    void CopyTo(T[] array, int arrayIndex);

    void CopyTo(T[] array, int arrayIndex, int count);

    bool IsProperSubsetOf(IEnumerable<T> other);


    bool IsProperSupersetOf(IEnumerable<T> other);

    bool IsSubsetOf(IEnumerable<T> other);

    bool IsSupersetOf(IEnumerable<T> other);

    bool Overlaps(IEnumerable<T> other);

    bool SetEquals(IEnumerable<T> other);

    bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue);
}
