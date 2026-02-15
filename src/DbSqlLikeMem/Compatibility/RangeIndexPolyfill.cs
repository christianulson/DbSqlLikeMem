#if NET48
namespace System;

/// <summary>
/// Minimal polyfill for frameworks where System.Index is not available.
/// </summary>
public readonly struct Index : IEquatable<Index>
{
    private readonly int _value;

    /// <summary>
    /// Initializes a new index value.
    /// </summary>
    /// <param name="value">Zero-based index value.</param>
    /// <param name="fromEnd">True to count from the end, false to count from the start.</param>
    public Index(int value, bool fromEnd = false)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value));

        _value = fromEnd ? ~value : value;
    }

    /// <summary>
    /// Gets the first position in a sequence.
    /// </summary>
    public static Index Start => new(0);

    /// <summary>
    /// Gets the position after the last element in a sequence.
    /// </summary>
    public static Index End => new(0, fromEnd: true);

    /// <summary>
    /// Gets the underlying non-negative index value.
    /// </summary>
    public int Value => _value < 0 ? ~_value : _value;

    /// <summary>
    /// Gets whether this index is measured from the end.
    /// </summary>
    public bool IsFromEnd => _value < 0;

    /// <summary>
    /// Resolves this index to an absolute offset for a collection length.
    /// </summary>
    /// <param name="length">The collection length.</param>
    /// <returns>The resolved absolute offset.</returns>
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
    /// Determines whether this index equals another index.
    /// </summary>
    /// <param name="other">Other index to compare.</param>
    /// <returns>True when both _indexes represent the same value.</returns>
    public bool Equals(Index other)
        => _value == other._value;

    /// <inheritdoc />
    public override int GetHashCode()
        => _value;

    /// <summary>
    /// Converts an integer into an index from the start.
    /// </summary>
    /// <param name="value">Zero-based index value.</param>
    public static implicit operator Index(int value)
        => new(value);

    /// <inheritdoc />
    public override string ToString()
        => IsFromEnd ? $"^{Value}" : Value.ToString();
}

/// <summary>
/// Minimal polyfill for frameworks where System.Range is not available.
/// </summary>
public readonly struct Range : IEquatable<Range>
{
    /// <summary>
    /// Initializes a new range with start and end _indexes.
    /// </summary>
    /// <param name="start">Start index, inclusive.</param>
    /// <param name="end">End index, exclusive.</param>
    public Range(Index start, Index end)
    {
        Start = start;
        End = end;
    }

    /// <summary>
    /// Gets the start index of the range.
    /// </summary>
    public Index Start { get; }

    /// <summary>
    /// Gets the end index of the range.
    /// </summary>
    public Index End { get; }

    /// <summary>
    /// Gets a range that covers all elements.
    /// </summary>
    public static Range All => new(Index.Start, Index.End);

    /// <summary>
    /// Creates a range from the provided start index to the end.
    /// </summary>
    /// <param name="start">Start index, inclusive.</param>
    /// <returns>A range that starts at <paramref name="start"/> and goes to the end.</returns>
    public static Range StartAt(Index start)
        => new(start, Index.End);

    /// <summary>
    /// Creates a range from the beginning to the provided end index.
    /// </summary>
    /// <param name="end">End index, exclusive.</param>
    /// <returns>A range that starts at the beginning and ends at <paramref name="end"/>.</returns>
    public static Range EndAt(Index end)
        => new(Index.Start, end);

    /// <summary>
    /// Resolves this range into absolute offset and length for a collection size.
    /// </summary>
    /// <param name="length">The collection length.</param>
    /// <returns>Tuple with resolved offset and length.</returns>
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
    /// Determines whether this range equals another range.
    /// </summary>
    /// <param name="other">Other range to compare.</param>
    /// <returns>True when both ranges have equal start and end _indexes.</returns>
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
