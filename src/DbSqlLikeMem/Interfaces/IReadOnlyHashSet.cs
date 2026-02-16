using System.Runtime.Serialization;

namespace DbSqlLikeMem.Interfaces;

public interface IReadOnlyHashSet<T> : IReadOnlyCollection<T>, IDeserializationCallback, ISerializable
{
    //
    // Summary:
    //     Gets the number of elements that are contained in a set.
    //
    // Returns:
    //     The number of elements that are contained in the set.
    int Count { get; }
    //
    // Summary:
    //     Gets the System.Collections.Generic.IEqualityComparer`1 object that is used to
    //     determine equality for the values in the set.
    //
    // Returns:
    //     The System.Collections.Generic.IEqualityComparer`1 object that is used to determine
    //     equality for the values in the set.
    IEqualityComparer<T> Comparer { get; }

    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object contains the
    //     specified element.
    //
    // Parameters:
    //   item:
    //     The element to locate in the System.Collections.Generic.HashSet`1 object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object contains the specified
    //     element; otherwise, false.
    bool Contains(T item);
    //
    // Summary:
    //     Copies the elements of a System.Collections.Generic.HashSet`1 object to an array.
    //
    //
    // Parameters:
    //   array:
    //     The one-dimensional array that is the destination of the elements copied from
    //     the System.Collections.Generic.HashSet`1 object. The array must have zero-based
    //     indexing.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     array is null.
    void CopyTo(T[] array);
    //
    // Summary:
    //     Copies the elements of a System.Collections.Generic.HashSet`1 object to an array,
    //     starting at the specified array index.
    //
    // Parameters:
    //   array:
    //     The one-dimensional array that is the destination of the elements copied from
    //     the System.Collections.Generic.HashSet`1 object. The array must have zero-based
    //     indexing.
    //
    //   arrayIndex:
    //     The zero-based index in array at which copying begins.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     array is null.
    //
    //   T:System.ArgumentOutOfRangeException:
    //     arrayIndex is less than 0.
    //
    //   T:System.ArgumentException:
    //     arrayIndex is greater than the length of the destination array.
    void CopyTo(T[] array, int arrayIndex);
    //
    // Summary:
    //     Copies the specified number of elements of a System.Collections.Generic.HashSet`1
    //     object to an array, starting at the specified array index.
    //
    // Parameters:
    //   array:
    //     The one-dimensional array that is the destination of the elements copied from
    //     the System.Collections.Generic.HashSet`1 object. The array must have zero-based
    //     indexing.
    //
    //   arrayIndex:
    //     The zero-based index in array at which copying begins.
    //
    //   count:
    //     The number of elements to copy to array.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     array is null.
    //
    //   T:System.ArgumentOutOfRangeException:
    //     arrayIndex is less than 0. -or- count is less than 0.
    //
    //   T:System.ArgumentException:
    //     arrayIndex is greater than the length of the destination array. -or- count is
    //     greater than the available space from the index to the end of the destination
    //     array.
    void CopyTo(T[] array, int arrayIndex, int count);
    //
    // Summary:
    //     Ensures that this hash set can hold the specified number of elements without
    //     growing.
    //
    // Parameters:
    //   capacity:
    //     The minimum capacity to ensure.
    //
    // Returns:
    //     The new capacity of this instance.
    //
    // Exceptions:
    //   T:System.ArgumentOutOfRangeException:
    //     capacity is less than zero.
#if NET6_0_OR_GREATER
    int EnsureCapacity(int capacity);
#endif
    //
    // Summary:
    //     Returns an enumerator that iterates through a System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     A System.Collections.Generic.HashSet`1.Enumerator object for the System.Collections.Generic.HashSet`1
    //     object.
    HashSet<T>.Enumerator GetEnumerator();

    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object is a proper
    //     subset of the specified collection.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object is a proper subset of
    //     other; otherwise, false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    bool IsProperSubsetOf(IEnumerable<T> other);
    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object is a proper
    //     superset of the specified collection.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object is a proper superset
    //     of other; otherwise, false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.

    bool IsProperSupersetOf(IEnumerable<T> other);
    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object is a subset
    //     of the specified collection.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object is a subset of other;
    //     otherwise, false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    bool IsSubsetOf(IEnumerable<T> other);
    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object is a superset
    //     of the specified collection.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object is a superset of other;
    //     otherwise, false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    bool IsSupersetOf(IEnumerable<T> other);
    //
    // Summary:
    //     Implements the System.Runtime.Serialization.ISerializable interface and raises
    //     the deserialization event when the deserialization is complete.
    //
    // Parameters:
    //   sender:
    //     The source of the deserialization event.
    //
    // Exceptions:
    //   T:System.Runtime.Serialization.SerializationException:
    //     The System.Runtime.Serialization.SerializationInfo object associated with the
    //     current System.Collections.Generic.HashSet`1 object is invalid.
    void OnDeserialization(object sender);
    //
    // Summary:
    //     Determines whether the current System.Collections.Generic.HashSet`1 object and
    //     a specified collection share common elements.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object and other share at least
    //     one common element; otherwise, false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    bool Overlaps(IEnumerable<T> other);

    //
    // Summary:
    //     Determines whether a System.Collections.Generic.HashSet`1 object and the specified
    //     collection contain the same elements.
    //
    // Parameters:
    //   other:
    //     The collection to compare to the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     true if the System.Collections.Generic.HashSet`1 object is equal to other; otherwise,
    //     false.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    bool SetEquals(IEnumerable<T> other);

    //
    // Summary:
    //     Searches the set for a given value and returns the equal value it finds, if any.
    //
    //
    // Parameters:
    //   equalValue:
    //     The value to search for.
    //
    //   actualValue:
    //     The value from the set that the search found, or the default value of T when
    //     the search yielded no match.
    //
    // Returns:
    //     A value indicating whether the search was successful.
    bool TryGetValue(T equalValue, out T actualValue);
}
