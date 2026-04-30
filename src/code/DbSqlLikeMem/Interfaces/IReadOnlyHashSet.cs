using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Interfaces;

/// <summary>
/// EN: Provides a read-only abstraction of a HashSet.
/// PT: Fornece uma abstracao somente leitura de um HashSet.
/// </summary>
public interface IReadOnlyHashSet<T> : IReadOnlyCollection<T>, IDeserializationCallback, ISerializable
{
    /// <summary>
    /// EN: Gets the IEqualityComparer object that is used to determine equality for the values in the set.
    /// PT: Obtem o objeto IEqualityComparer que e usado para determinar a igualdade para os valores no conjunto.
    /// </summary>
    IEqualityComparer<T> Comparer { get; }

    /// <summary>
    /// EN: Determines whether a HashSet object contains the specified element.
    /// PT: Determina se um objeto HashSet contem o elemento especificado.
    /// </summary>
    bool Contains(T item);

    /// <summary>
    /// EN: Copies the elements of a HashSet object to an array.
    /// PT: Copia os elementos de um objeto HashSet para um array.
    /// </summary>
    void CopyTo(T[] array);

    /// <summary>
    /// EN: Copies the elements of a HashSet object to an array, starting at the specified array index.
    /// PT: Copia os elementos de um objeto HashSet para um array, comecando no indice de array especificado.
    /// </summary>
    void CopyTo(T[] array, int arrayIndex);

    /// <summary>
    /// EN: Copies the specified number of elements of a HashSet object to an array, starting at the specified array index.
    /// PT: Copia o numero especificado de elementos de um objeto HashSet para um array, comecando no indice de array especificado.
    /// </summary>
    void CopyTo(T[] array, int arrayIndex, int count);

    /// <summary>
    /// EN: Determines whether a HashSet object is a proper subset of the specified collection.
    /// PT: Determina se um objeto HashSet e um subconjunto proprio da colecao especificada.
    /// </summary>
    bool IsProperSubsetOf(IEnumerable<T> other);

    /// <summary>
    /// EN: Determines whether a HashSet object is a proper superset of the specified collection.
    /// PT: Determina se um objeto HashSet e um superconjunto proprio da colecao especificada.
    /// </summary>
    bool IsProperSupersetOf(IEnumerable<T> other);

    /// <summary>
    /// EN: Determines whether a HashSet object is a subset of the specified collection.
    /// PT: Determina se um objeto HashSet e um subconjunto da colecao especificada.
    /// </summary>
    bool IsSubsetOf(IEnumerable<T> other);

    /// <summary>
    /// EN: Determines whether a HashSet object is a superset of the specified collection.
    /// PT: Determina se um objeto HashSet e um superconjunto da colecao especificada.
    /// </summary>
    bool IsSupersetOf(IEnumerable<T> other);

    /// <summary>
    /// EN: Determines whether the current HashSet object and a specified collection share common elements.
    /// PT: Determina se o objeto HashSet atual e uma colecao especificada compartilham elementos comuns.
    /// </summary>
    bool Overlaps(IEnumerable<T> other);

    /// <summary>
    /// EN: Determines whether a HashSet object and the specified collection contain the same elements.
    /// PT: Determina se um objeto HashSet e a colecao especificada contem os mesmos elementos.
    /// </summary>
    bool SetEquals(IEnumerable<T> other);

    /// <summary>
    /// EN: Searches the set for a given value and returns the equal value it finds, if any.
    /// PT: Pesquisa no conjunto um determinado valor e retorna o valor igual que ele encontra, se houver.
    /// </summary>
#if NET8_0_OR_GREATER
    bool TryGetValue(T equalValue, [MaybeNullWhen(false)] out T actualValue);
#else
    bool TryGetValue(T equalValue, out T actualValue);
#endif
}
