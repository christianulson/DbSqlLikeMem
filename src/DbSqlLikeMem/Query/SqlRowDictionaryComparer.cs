using System.Globalization;

namespace DbSqlLikeMem;

internal sealed class SqlRowDictionaryComparer(ISqlDialect dialect)
    : IEqualityComparer<Dictionary<int, object?>>
{
    public bool Equals(Dictionary<int, object?>? x, Dictionary<int, object?>? y)
    {
        if (ReferenceEquals(x, y))
            return true;
        if (x is null || y is null || x.Count != y.Count)
            return false;

        foreach (var item in x)
        {
            if (!y.TryGetValue(item.Key, out var rightValue))
                return false;

            if (!item.Value.EqualsSql(rightValue, dialect))
                return false;
        }

        return true;
    }

    public int GetHashCode(Dictionary<int, object?> row)
    {
        var hash = new HashCode();
        foreach (var key in row.Keys.OrderBy(k => k))
        {
            hash.Add(key);
            hash.Add(NormalizeHash(row[key]));
        }

        return hash.ToHashCode();
    }

    private object? NormalizeHash(object? value)
    {
        if (value is null or DBNull)
            return null;

        if (TryNormalizeNumericHash(value, out var numericHash))
            return numericHash;

        if (value is string text)
        {
            return dialect.TextComparison == StringComparison.OrdinalIgnoreCase
                ? text.ToUpperInvariant()
                : text;
        }

        return value;
    }

    private bool TryNormalizeNumericHash(object value, out string normalized)
    {
        normalized = string.Empty;

        if (TryGetNumericValue(value, out var numeric))
        {
            normalized = numeric.ToString("G29", CultureInfo.InvariantCulture);
            return true;
        }

        if (!dialect.SupportsImplicitNumericStringComparison)
            return false;

        if (value is string text
            && decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
        {
            normalized = parsed.ToString("G29", CultureInfo.InvariantCulture);
            return true;
        }

        return false;
    }

    private static bool TryGetNumericValue(object value, out decimal numeric)
    {
        switch (value)
        {
            case byte b:
                numeric = b;
                return true;
            case short s:
                numeric = s;
                return true;
            case int i:
                numeric = i;
                return true;
            case long l:
                numeric = l;
                return true;
            case float f:
                numeric = (decimal)f;
                return true;
            case double d:
                numeric = (decimal)d;
                return true;
            case decimal m:
                numeric = m;
                return true;
            default:
                numeric = default;
                return false;
        }
    }
}
