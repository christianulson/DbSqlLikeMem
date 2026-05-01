#if NET462 || NETSTANDARD2_0
namespace System;

/// <summary>
/// EN: Provides a minimal polyfill for System.Index on target frameworks that do not include it.
/// PT-br: Fornece um polyfill minimo de System.Index para frameworks alvo que nao o incluem.
/// </summary>
public readonly struct Index : IEquatable<Index>
{
    private readonly int _value;

    /// <summary>
    /// EN: Initializes a new index value.
    /// PT-br: Inicializa um novo valor de indice.
    /// </summary>
    /// <param name="value">EN: Zero-based index value. PT-br: Valor do indice baseado em zero.</param>
    /// <param name="fromEnd">EN: True to count from the end, false to count from the start. PT-br: True para contar a partir do fim, false para contar a partir do inicio.</param>
    public Index(int value, bool fromEnd = false)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value));

        _value = fromEnd ? ~value : value;
    }

    /// <summary>
    /// EN: Gets the first position in a sequence.
    /// PT-br: Obtém a primeira posicao em uma sequencia.
    /// </summary>
    public static Index Start => new(0);

    /// <summary>
    /// EN: Gets the position after the last element in a sequence.
    /// PT-br: Obtém a posicao apos o ultimo elemento em uma sequencia.
    /// </summary>
    public static Index End => new(0, fromEnd: true);

    /// <summary>
    /// EN: Gets the underlying non-negative index value.
    /// PT-br: Obtém o valor subjacente nao negativo do indice.
    /// </summary>
    public int Value => _value < 0 ? ~_value : _value;

    /// <summary>
    /// EN: Gets whether this index is measured from the end.
    /// PT-br: Obtém se este indice é medido a partir do fim.
    /// </summary>
    public bool IsFromEnd => _value < 0;

    /// <summary>
    /// EN: Resolves this index to an absolute offset for a collection length.
    /// PT-br: Resolve este indice para um deslocamento absoluto para um comprimento de colecao.
    /// </summary>
    /// <param name="length">EN: The collection length. PT-br: O comprimento da colecao.</param>
    /// <returns>EN: The resolved absolute offset. PT-br: O deslocamento absoluto resolvido.</returns>
    public int GetOffset(int length)
    {
        var offset = IsFromEnd ? length - Value : Value;
        if ((uint)offset > (uint)length)
            throw new ArgumentOutOfRangeException(nameof(length));

        return offset;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is Index other && Equals(other);

    /// <summary>
    /// EN: Determines whether this index equals another index.
    /// PT-br: Determina se este indice e igual a outro indice.
    /// </summary>
    /// <param name="other">EN: Other index to compare. PT-br: Outro indice para comparar.</param>
    /// <returns>EN: True when both indexes represent the same value. PT-br: True quando ambos os indices representam o mesmo valor.</returns>
    public bool Equals(Index other)
        => _value == other._value;

    /// <inheritdoc />
    public override int GetHashCode()
        => _value;

    /// <summary>
    /// EN: Converts an integer into an index from the start.
    /// PT-br: Converte um inteiro em um indice contado a partir do inicio.
    /// </summary>
    /// <param name="value">EN: Zero-based index value. PT-br: Valor do indice baseado em zero.</param>
    public static implicit operator Index(int value)
        => new(value);

    /// <inheritdoc />
    public override string ToString()
        => IsFromEnd ? $"^{Value}" : Value.ToString();
}

/// <summary>
/// EN: Provides a minimal polyfill for System.Range on target frameworks that do not include it.
/// PT-br: Fornece um polyfill minimo de System.Range para frameworks alvo que nao o incluem.
/// </summary>
/// <remarks>
/// EN: Initializes a new range with start and end indexes.
/// PT-br: Inicializa um novo range com indices inicial e final.
/// </remarks>
/// <param name="start">EN: Start index, inclusive. PT-br: Indice inicial, inclusivo.</param>
/// <param name="end">EN: End index, exclusive. PT-br: Indice final, exclusivo.</param>
public readonly struct Range(Index start, Index end) : IEquatable<Range>
{
    /// <summary>
    /// EN: Gets the start index of the range.
    /// PT-br: Obtém o indice inicial do range.
    /// </summary>
    public Index Start { get; } = start;

    /// <summary>
    /// EN: Gets the end index of the range.
    /// PT-br: Obtém o indice final do range.
    /// </summary>
    public Index End { get; } = end;

    /// <summary>
    /// EN: Gets a range that covers all elements.
    /// PT-br: Obtém um range que cobre todos os elementos.
    /// </summary>
    public static Range All => new(Index.Start, Index.End);

    /// <summary>
    /// EN: Creates a range from the provided start index to the end.
    /// PT-br: Cria um range do indice inicial informado ate o fim.
    /// </summary>
    /// <param name="start">EN: Start index, inclusive. PT-br: Indice inicial, inclusivo.</param>
    /// <returns>EN: A range that starts at the provided index and goes to the end. PT-br: Um range que começa no indice informado e vai ate o fim.</returns>
    public static Range StartAt(Index start)
        => new(start, Index.End);

    /// <summary>
    /// EN: Creates a range from the beginning to the provided end index.
    /// PT-br: Cria um range do inicio ate o indice final informado.
    /// </summary>
    /// <param name="end">EN: End index, exclusive. PT-br: Indice final, exclusivo.</param>
    /// <returns>EN: A range that starts at the beginning and ends at the provided index. PT-br: Um range que começa no inicio e termina no indice informado.</returns>
    public static Range EndAt(Index end)
        => new(Index.Start, end);

    /// <summary>
    /// EN: Resolves this range into absolute offset and length for a collection size.
    /// PT-br: Resolve este range em deslocamento absoluto e comprimento para um tamanho de colecao.
    /// </summary>
    /// <param name="length">EN: The collection length. PT-br: O comprimento da colecao.</param>
    /// <returns>EN: Tuple with resolved offset and length. PT-br: Tupla com deslocamento e comprimento resolvidos.</returns>
    public (int Offset, int Length) GetOffsetAndLength(int length)
    {
        var start = Start.GetOffset(length);
        var end = End.GetOffset(length);

        if ((uint)end > (uint)length || end < start)
            throw new ArgumentOutOfRangeException(nameof(length));

        return (start, end - start);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is Range other && Equals(other);

    /// <summary>
    /// EN: Determines whether this range equals another range.
    /// PT-br: Determina se este range e igual a outro range.
    /// </summary>
    /// <param name="other">EN: Other range to compare. PT-br: Outro range para comparar.</param>
    /// <returns>EN: True when both ranges have equal start and end indexes. PT-br: True quando ambos os ranges tem inicio e fim iguais.</returns>
    public bool Equals(Range other)
        => Start.Equals(other.Start) && End.Equals(other.End);

    /// <inheritdoc />
    public override int GetHashCode()
        => (Start, End).GetHashCode();

    /// <inheritdoc />
    public override string ToString()
        => $"{Start}..{End}";
}
#endif
