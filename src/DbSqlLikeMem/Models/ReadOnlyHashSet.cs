using DbSqlLikeMem.Interfaces;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Models;

public class ReadOnlyHashSet<T> : IReadOnlyHashSet<T>
{


    private readonly HashSet<T> _set;

    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that is empty and uses the default equality comparer for the set type.
    public ReadOnlyHashSet()
        => _set = [];
    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that uses the default equality comparer for the set type, contains elements copied
    //     from the specified collection, and has sufficient capacity to accommodate the
    //     number of elements copied.
    //
    // Parameters:
    //   collection:
    //     The collection whose elements are copied to the new set.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     collection is null.
    public ReadOnlyHashSet(IEnumerable<T> collection)
        => _set = [.. collection];
    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that is empty and uses the specified equality comparer for the set type.
    //
    // Parameters:
    //   comparer:
    //     The System.Collections.Generic.IEqualityComparer`1 implementation to use when
    //     comparing values in the set, or null to use the default System.Collections.Generic.EqualityComparer`1
    //     implementation for the set type.
    public ReadOnlyHashSet(IEqualityComparer<T> comparer)
        => _set = new HashSet<T>(comparer);
    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that is empty, but has reserved space for capacity items and uses the default
    //     equality comparer for the set type.
    //
    // Parameters:
    //   capacity:
    //     The initial size of the System.Collections.Generic.HashSet`1
    public ReadOnlyHashSet(int capacity)
        => _set = new HashSet<T>(capacity);
    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that uses the specified equality comparer for the set type, contains elements
    //     copied from the specified collection, and has sufficient capacity to accommodate
    //     the number of elements copied.
    //
    // Parameters:
    //   collection:
    //     The collection whose elements are copied to the new set.
    //
    //   comparer:
    //     The System.Collections.Generic.IEqualityComparer`1 implementation to use when
    //     comparing values in the set, or null to use the default System.Collections.Generic.EqualityComparer`1
    //     implementation for the set type.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     collection is null.
    public ReadOnlyHashSet(IEnumerable<T> collection, IEqualityComparer<T> comparer)
        => _set = new HashSet<T>(collection, comparer);
    //
    // Summary:
    //     Initializes a new instance of the System.Collections.Generic.HashSet`1 class
    //     that uses the specified equality comparer for the set type, and has sufficient
    //     capacity to accommodate capacity elements.
    //
    // Parameters:
    //   capacity:
    //     The initial size of the System.Collections.Generic.HashSet`1
    //
    //   comparer:
    //     The System.Collections.Generic.IEqualityComparer`1 implementation to use when
    //     comparing values in the set, or null (Nothing in Visual Basic) to use the default
    //     System.Collections.Generic.IEqualityComparer`1 implementation for the set type.
    public ReadOnlyHashSet(int capacity, IEqualityComparer<T> comparer)
        => _set = new HashSet<T>(capacity, comparer);

    //
    // Summary:
    //     Gets the number of elements that are contained in a set.
    //
    // Returns:
    //     The number of elements that are contained in the set.
    public int Count => _set.Count;
    //
    // Summary:
    //     Gets the System.Collections.Generic.IEqualityComparer`1 object that is used to
    //     determine equality for the values in the set.
    //
    // Returns:
    //     The System.Collections.Generic.IEqualityComparer`1 object that is used to determine
    //     equality for the values in the set.
    public IEqualityComparer<T> Comparer => _set.Comparer;

    //
    // Summary:
    //     Returns an System.Collections.IEqualityComparer object that can be used for equality
    //     testing of a System.Collections.Generic.HashSet`1 object.
    //
    // Returns:
    //     An System.Collections.IEqualityComparer object that can be used for deep equality
    //     testing of the System.Collections.Generic.HashSet`1 object.
    public static IEqualityComparer<HashSet<T>> CreateSetComparer()
        => HashSet<T>.CreateSetComparer();
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
    public bool Contains(T item)
        => _set.Contains(item);
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
    public void CopyTo(T[] array)
        => _set.CopyTo(array);
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
    public void CopyTo(T[] array, int arrayIndex)
        => _set.CopyTo(array, arrayIndex);
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
    public void CopyTo(T[] array, int arrayIndex, int count)
        => _set.CopyTo(array, arrayIndex, count);

#if NET6_0_OR_GREATER
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
    public int EnsureCapacity(int capacity)
        => _set.EnsureCapacity(capacity);
#endif

    //
    // Summary:
    //     Removes all elements in the specified collection from the current System.Collections.Generic.HashSet`1
    //     object.
    //
    // Parameters:
    //   other:
    //     The collection of items to remove from the System.Collections.Generic.HashSet`1
    //     object.
    //
    // Exceptions:
    //   T:System.ArgumentNullException:
    //     other is null.
    public void ExceptWith(IEnumerable<T> other)
        => _set.ExceptWith(other);
    //
    // Summary:
    //     Returns an enumerator that iterates through a System.Collections.Generic.HashSet`1
    //     object.
    //
    // Returns:
    //     A System.Collections.Generic.HashSet`1.Enumerator object for the System.Collections.Generic.HashSet`1
    //     object.
    public HashSet<T>.Enumerator GetEnumerator()
        => _set.GetEnumerator();

    public void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(info, nameof(info));
#pragma warning disable SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
        ((ISerializable)_set).GetObjectData(info, context);
#pragma warning restore SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
    }

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
    public bool IsProperSubsetOf(IEnumerable<T> other)
        => _set.IsProperSubsetOf(other);
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
    public bool IsProperSupersetOf(IEnumerable<T> other)
        => _set.IsProperSupersetOf(other);
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
    public bool IsSubsetOf(IEnumerable<T> other)
        => _set.IsSubsetOf(other);
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
    public bool IsSupersetOf(IEnumerable<T> other)
        => _set.IsSupersetOf(other);
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
    public virtual void OnDeserialization(object? sender)
        => _set.OnDeserialization(sender);
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
    public bool Overlaps(IEnumerable<T> other)
        => _set.Overlaps(other);

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
    public bool SetEquals(IEnumerable<T> other)
        => _set.SetEquals(other);

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
    public bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue)
        => _set.TryGetValue(equalValue, out actualValue);

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
        => _set.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _set.GetEnumerator();
}
