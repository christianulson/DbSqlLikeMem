namespace DbSqlLikeMem;

internal sealed record GuidDefaultValue(bool Sequential = false);

internal static class GuidDefaultValueHelper
{
    private const long GregorianEpochTicks = 0x01B21DD213814000L;

    private static readonly object Sync = new();
    private static long _lastTimestamp = long.MinValue;
    private static ushort _clockSequence = CreateInitialClockSequence();
    private static readonly byte[] Node = CreateNode();

    internal static bool IsGeneratedGuidDefaultValue(object? value)
        => value is GuidDefaultValue
            || value is string text && TryParseGeneratedGuidDefaultValueText(text, out _);

    internal static bool IsGeneratedGuidDefaultValueText(string text)
        => TryParseGeneratedGuidDefaultValueText(text, out _);

    internal static bool TryGetGeneratedGuidDefaultValue(object? value, out GuidDefaultValue generatedValue)
    {
        switch (value)
        {
            case GuidDefaultValue existing:
                generatedValue = existing;
                return true;
            case string text:
                return TryParseGeneratedGuidDefaultValueText(text, out generatedValue);
            default:
                generatedValue = default!;
                return false;
        }
    }

    internal static bool TryParseGeneratedGuidDefaultValueText(string text, out GuidDefaultValue generatedValue)
    {
        var normalized = RemoveWhitespace(text.Trim());
        while (normalized.Length >= 2
            && normalized[0] == '('
            && normalized[^1] == ')')
        {
            normalized = RemoveWhitespace(normalized[1..^1].Trim());
        }

        if (normalized.Equals("NEWID", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("NEWID()", StringComparison.OrdinalIgnoreCase))
        {
            generatedValue = new GuidDefaultValue();
            return true;
        }

        if (normalized.Equals("NEWSEQUENTIALID", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("NEWSEQUENTIALID()", StringComparison.OrdinalIgnoreCase))
        {
            generatedValue = new GuidDefaultValue(true);
            return true;
        }

        generatedValue = default!;
        return false;
    }

    internal static Guid CreateGuidValue(GuidDefaultValue generatedValue)
        => generatedValue.Sequential
            ? CreateSequentialGuid()
            : Guid.NewGuid();

    internal static Guid CreateSequentialGuid()
    {
        long timestamp;
        ushort clockSequence;

        lock (Sync)
        {
            timestamp = DateTime.UtcNow.Ticks + GregorianEpochTicks;
            if (timestamp <= _lastTimestamp)
            {
                _clockSequence = (ushort)((_clockSequence + 1) & 0x3FFF);
                timestamp = _lastTimestamp + 1;
            }

            _lastTimestamp = timestamp;
            clockSequence = _clockSequence;
        }

        var timeLow = (int)(timestamp & 0xFFFFFFFF);
        var timeMid = (short)((timestamp >> 32) & 0xFFFF);
        var timeHiAndVersion = (short)(((timestamp >> 48) & 0x0FFF) | 0x1000);
        var clockSeqHiAndReserved = (byte)(((clockSequence >> 8) & 0x3F) | 0x80);
        var clockSeqLow = (byte)(clockSequence & 0xFF);

        return new Guid(
            timeLow,
            timeMid,
            timeHiAndVersion,
            clockSeqHiAndReserved,
            clockSeqLow,
            Node[0],
            Node[1],
            Node[2],
            Node[3],
            Node[4],
            Node[5]);
    }

    private static byte[] CreateNode()
    {
        var node = new byte[6];
        using var randomNumberGenerator = RandomNumberGenerator.Create();
        if (randomNumberGenerator is null)
            throw new InvalidOperationException("Unable to create a random number generator.");

        randomNumberGenerator.GetBytes(node);
        node[0] |= 0x01;
        return node;
    }

    private static ushort CreateInitialClockSequence()
    {
        var buffer = new byte[2];
        using var randomNumberGenerator = RandomNumberGenerator.Create();
        if (randomNumberGenerator is null)
            throw new InvalidOperationException("Unable to create a random number generator.");

        randomNumberGenerator.GetBytes(buffer);
        return (ushort)(((buffer[0] << 8) | buffer[1]) & 0x3FFF);
    }

    private static string RemoveWhitespace(string text)
    {
        if (text.Length == 0)
            return text;

        var buffer = new char[text.Length];
        var length = 0;

        foreach (var ch in text)
        {
            if (!char.IsWhiteSpace(ch))
                buffer[length++] = ch;
        }

        return length == buffer.Length ? text : new string(buffer, 0, length);
    }
}
