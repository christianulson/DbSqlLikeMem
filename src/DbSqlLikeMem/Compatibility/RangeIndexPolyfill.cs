#if NET48
namespace System;

/// <summary>
/// Minimal polyfill for frameworks where System.Index is not available.
/// </summary>
public readonly struct Index : IEquatable<Index>
{
    private readonly int _value;

    public Index(int value, bool fromEnd = false)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value));

        _value = fromEnd ? ~value : value;
    }

    public static Index Start => new(0);

    public static Index End => new(0, fromEnd: true);

    public int Value => _value < 0 ? ~_value : _value;

    public bool IsFromEnd => _value < 0;

    public int GetOffset(int length)
    {
        var offset = IsFromEnd ? length - Value : Value;
        if ((uint)offset > (uint)length)
            throw new ArgumentOutOfRangeException(nameof(length));

        return offset;
    }

    public override bool Equals(object? obj)
        => obj is Index other && Equals(other);

    public bool Equals(Index other)
        => _value == other._value;

    public override int GetHashCode()
        => _value;

    public static implicit operator Index(int value)
        => new(value);

    public override string ToString()
        => IsFromEnd ? $"^{Value}" : Value.ToString();
}

/// <summary>
/// Minimal polyfill for frameworks where System.Range is not available.
/// </summary>
public readonly struct Range : IEquatable<Range>
{
    public Range(Index start, Index end)
    {
        Start = start;
        End = end;
    }

    public Index Start { get; }

    public Index End { get; }

    public static Range All => new(Index.Start, Index.End);

    public static Range StartAt(Index start)
        => new(start, Index.End);

    public static Range EndAt(Index end)
        => new(Index.Start, end);

    public (int Offset, int Length) GetOffsetAndLength(int length)
    {
        var start = Start.GetOffset(length);
        var end = End.GetOffset(length);

        if ((uint)end > (uint)length || end < start)
            throw new ArgumentOutOfRangeException(nameof(length));

        return (start, end - start);
    }

    public override bool Equals(object? obj)
        => obj is Range other && Equals(other);

    public bool Equals(Range other)
        => Start.Equals(other.Start) && End.Equals(other.End);

    public override int GetHashCode()
        => (Start, End).GetHashCode();

    public override string ToString()
        => $"{Start}..{End}";
}
#endif
