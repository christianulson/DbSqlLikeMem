using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;

namespace DbSqlLikeMem.Models;

/// <summary>
/// EN: Provides a read-only set wrapper over a HashSet.
/// PT: Fornece um wrapper de conjunto somente leitura sobre um HashSet.
/// </summary>
public class ReadOnlyHashSet<T> : IReadOnlyHashSet<T>
{
    private readonly HashSet<T> _set;

    /// <summary>
    /// EN: Initializes a new instance of the ReadOnlyHashSet class that is empty.
    /// PT: Inicializa uma nova instancia da classe ReadOnlyHashSet que esta vazia.
    /// </summary>
    public ReadOnlyHashSet()
        => _set = [];

    /// <summary>
    /// EN: Initializes a new instance containing elements copied from the specified collection.
    /// PT: Inicializa uma nova instancia contendo elementos copiados da colecao especificada.
    /// </summary>
    /// <param name="collection">EN: The collection whose elements are copied. PT: A colecao cujos elementos sao copiados.</param>
    public ReadOnlyHashSet(IEnumerable<T> collection)
        => _set = [.. collection];

    /// <summary>
    /// EN: Initializes a new empty instance that uses the specified equality comparer.
    /// PT: Inicializa uma nova instancia vazia que usa o comparador de igualdade especificado.
    /// </summary>
    /// <param name="comparer">EN: The equality comparer to use. PT: O comparador de igualdade a usar.</param>
    public ReadOnlyHashSet(IEqualityComparer<T> comparer)
        => _set = new HashSet<T>(comparer);

    /// <summary>
    /// EN: Initializes a new instance with the specified capacity.
    /// PT: Inicializa uma nova instancia com a capacidade especificada.
    /// </summary>
    /// <param name="capacity">EN: The initial capacity. PT: A capacidade inicial.</param>
    public ReadOnlyHashSet(int capacity)
    {
#if NET8_0_OR_GREATER
        _set = new HashSet<T>(capacity);
#else
        _set = new HashSet<T>();
#endif
    }

    /// <summary>
    /// EN: Initializes a new instance containing elements copied from the collection, using the specified equality comparer.
    /// PT: Inicializa uma nova instancia contendo elementos copiados da colecao, usando o comparador de igualdade especificado.
    /// </summary>
    /// <param name="collection">EN: The collection whose elements are copied. PT: A colecao cujos elementos sao copiados.</param>
    /// <param name="comparer">EN: The equality comparer to use. PT: O comparador de igualdade a usar.</param>
    public ReadOnlyHashSet(IEnumerable<T> collection, IEqualityComparer<T> comparer)
        => _set = new HashSet<T>(collection, comparer);

    /// <summary>
    /// EN: Initializes a new instance with the specified capacity and equality comparer.
    /// PT: Inicializa uma nova instancia com a capacidade e comparador de igualdade especificados.
    /// </summary>
    /// <param name="capacity">EN: The initial capacity. PT: A capacidade inicial.</param>
    /// <param name="comparer">EN: The equality comparer to use. PT: O comparador de igualdade a usar.</param>
    public ReadOnlyHashSet(int capacity, IEqualityComparer<T> comparer)
    {
#if NET8_0_OR_GREATER
        _set = new HashSet<T>(capacity, comparer);
#else
        _set = new HashSet<T>(comparer);
#endif
    }

    /// <summary>
    /// EN: Gets the number of elements that are contained in the set.
    /// PT: Obtem o numero de elementos que estao contidos no conjunto.
    /// </summary>
    public int Count => _set.Count;

    /// <summary>
    /// EN: Gets the equality comparer used for the values in the set.
    /// PT: Obtem o comparador de igualdade usado para os valores no conjunto.
    /// </summary>
    public IEqualityComparer<T> Comparer => _set.Comparer;

    /// <summary>
    /// EN: Returns an equality comparer object that can be used for deep equality testing.
    /// PT: Retorna um objeto comparador de igualdade que pode ser usado para testes de igualdade profunda.
    /// </summary>
    public static IEqualityComparer<HashSet<T>> CreateSetComparer()
        => HashSet<T>.CreateSetComparer();

    /// <summary>
    /// EN: Determines whether the set contains the specified element.
    /// PT: Determina se o conjunto contem o elemento especificado.
    /// </summary>
    /// <param name="item">EN: The element to locate. PT: O elemento a localizar.</param>
    public bool Contains(T item)
        => _set.Contains(item);

    /// <summary>
    /// EN: Copies the elements to an array.
    /// PT: Copia os elementos para um array.
    /// </summary>
    /// <param name="array">EN: The destination array. PT: O array de destino.</param>
    public void CopyTo(T[] array)
        => _set.CopyTo(array);

    /// <summary>
    /// EN: Copies the elements to an array, starting at the specified index.
    /// PT: Copia os elementos para um array, comecando no indice especificado.
    /// </summary>
    /// <param name="array">EN: The destination array. PT: O array de destino.</param>
    /// <param name="arrayIndex">EN: The start index. PT: O indice inicial.</param>
    public void CopyTo(T[] array, int arrayIndex)
        => _set.CopyTo(array, arrayIndex);

    /// <summary>
    /// EN: Copies the specified number of elements to an array, starting at the specified index.
    /// PT: Copia o numero especificado de elementos para um array, comecando no indice especificado.
    /// </summary>
    /// <param name="array">EN: The destination array. PT: O array de destino.</param>
    /// <param name="arrayIndex">EN: The start index. PT: O indice inicial.</param>
    /// <param name="count">EN: The number of elements to copy. PT: O numero de elementos a copiar.</param>
    public void CopyTo(T[] array, int arrayIndex, int count)
        => _set.CopyTo(array, arrayIndex, count);

#if NET6_0_OR_GREATER
    /// <summary>
    /// EN: Ensures that this set can hold the specified number of elements without growing.
    /// PT: Garante que este conjunto possa conter o numero especificado de elementos sem crescer.
    /// </summary>
    /// <param name="capacity">EN: The minimum capacity to ensure. PT: A capacidade minima a garantir.</param>
    public int EnsureCapacity(int capacity)
        => _set.EnsureCapacity(capacity);
#endif

    /// <summary>
    /// EN: Removes all elements in the specified collection from the current set.
    /// PT: Remove todos os elementos da colecao especificada do conjunto atual.
    /// </summary>
    /// <param name="other">EN: The collection of items to remove. PT: A colecao de itens a remover.</param>
    public void ExceptWith(IEnumerable<T> other)
        => _set.ExceptWith(other);

    /// <summary>
    /// EN: Returns an enumerator that iterates through the set.
    /// PT: Retorna um enumerador que itera atraves do conjunto.
    /// </summary>
    public HashSet<T>.Enumerator GetEnumerator()
        => _set.GetEnumerator();

    /// <summary>
    /// EN: Implements the ISerializable interface.
    /// PT: Implementa a interface ISerializable.
    /// </summary>
    /// <param name="info">EN: The SerializationInfo. PT: O SerializationInfo.</param>
    /// <param name="context">EN: The StreamingContext. PT: O StreamingContext.</param>
    public void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(info, nameof(info));
#pragma warning disable SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
        ((ISerializable)_set).GetObjectData(info, context);
#pragma warning restore SYSLIB0050 // Formatter-based serialization is obsolete and should not be used.
    }

    /// <summary>
    /// EN: Determines whether the set is a proper subset of the specified collection.
    /// PT: Determina se o conjunto e um subconjunto proprio da colecao especificada.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool IsProperSubsetOf(IEnumerable<T> other)
        => _set.IsProperSubsetOf(other);

    /// <summary>
    /// EN: Determines whether the set is a proper superset of the specified collection.
    /// PT: Determina se o conjunto e um superconjunto proprio da colecao especificada.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool IsProperSupersetOf(IEnumerable<T> other)
        => _set.IsProperSupersetOf(other);

    /// <summary>
    /// EN: Determines whether the set is a subset of the specified collection.
    /// PT: Determina se o conjunto e um subconjunto da colecao especificada.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool IsSubsetOf(IEnumerable<T> other)
        => _set.IsSubsetOf(other);

    /// <summary>
    /// EN: Determines whether the set is a superset of the specified collection.
    /// PT: Determina se o conjunto e um superconjunto da colecao especificada.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool IsSupersetOf(IEnumerable<T> other)
        => _set.IsSupersetOf(other);

    /// <summary>
    /// EN: Raises the deserialization event.
    /// PT: Aciona o evento de desserializacao.
    /// </summary>
    /// <param name="sender">EN: The source. PT: A fonte.</param>
    public virtual void OnDeserialization(object? sender)
        => _set.OnDeserialization(sender);

    /// <summary>
    /// EN: Determines whether the set and a specified collection share common elements.
    /// PT: Determina se o conjunto e uma colecao especificada compartilham elementos comuns.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool Overlaps(IEnumerable<T> other)
        => _set.Overlaps(other);

    /// <summary>
    /// EN: Determines whether the set and the specified collection contain the same elements.
    /// PT: Determina se o conjunto e a colecao especificada contem os mesmos elementos.
    /// </summary>
    /// <param name="other">EN: The collection to compare. PT: A colecao a comparar.</param>
    public bool SetEquals(IEnumerable<T> other)
        => _set.SetEquals(other);

    /// <summary>
    /// EN: Searches the set for a given value and returns the equal value it finds.
    /// PT: Pesquisa no conjunto por um valor e retorna o valor igual encontrado.
    /// </summary>
    /// <param name="equalValue">EN: The value to search. PT: O valor a procurar.</param>
    /// <param name="actualValue">EN: The found value. PT: O valor encontrado.</param>
    public bool TryGetValue(
        T equalValue,
#if NET8_0_OR_GREATER
        [MaybeNullWhen(false)]
#endif
        out T actualValue)
    {
#if NET8_0_OR_GREATER
        return _set.TryGetValue(equalValue, out actualValue);
#else
        foreach (var item in _set)
        {
            if (_set.Comparer.Equals(item, equalValue))
            {
                actualValue = item;
                return true;
            }
        }

        actualValue = default!;
        return false;
#endif
    }

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
        => _set.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _set.GetEnumerator();
}
