namespace DbSqlLikeMem;

internal static class StringCompatibility
{
    internal static int IndexOfAny(
        string? value,
        params char[] anyOf)
    {
        if (string.IsNullOrEmpty(value) || anyOf is null || anyOf.Length == 0)
            return -1;

        for (var i = 0; i < value!.Length; i++)
        {
            var current = value[i];
            for (var j = 0; j < anyOf.Length; j++)
            {
                if (current == anyOf[j])
                    return i;
            }
        }

        return -1;
    }

    internal static ReadOnlySpan<char> Trim(
        ReadOnlySpan<char> value,
        params char[] trimChars)
    {
        if (trimChars is null || trimChars.Length == 0)
            return value;

        var start = 0;
        var end = value.Length - 1;

        while (start <= end && Contains(trimChars, value[start]))
            start++;

        while (end >= start && Contains(trimChars, value[end]))
            end--;

        return value.Slice(start, end - start + 1);
    }

    internal static IEnumerable<string> SplitAndTrim(
        string value,
        char separator)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        foreach (var part in value.Split(separator))
        {
            var trimmed = part.Trim();
            if (!string.IsNullOrWhiteSpace(trimmed))
                yield return trimmed;
        }
    }

    private static bool Contains(
        char[] values,
        char candidate)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] == candidate)
                return true;
        }

        return false;
    }
}
