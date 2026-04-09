namespace DbSqlLikeMem;

internal static class AstQueryAggregateKeyHelper
{
    internal static bool TryGetStringAggregateText(object? value, out string text)
    {
        text = string.Empty;

        if (AstQueryExecutorBase.IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                return true;
        }
    }

    internal static bool TryGetStringAggregateKeyAndText(
        object? value,
        bool useOrdinalTextComparison,
        out string text,
        out string distinctKey)
    {
        _ = useOrdinalTextComparison;

        text = string.Empty;
        distinctKey = string.Empty;

        if (AstQueryExecutorBase.IsNullish(value))
            return false;

        switch (value)
        {
            case string textValue:
                text = textValue;
                distinctKey = textValue;
                return true;
            case decimal decimalValue:
                text = decimalValue.ToString(CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case double doubleValue:
                text = doubleValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case float floatValue:
                text = floatValue.ToString("R", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case DateTime dateTime:
                text = dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
                distinctKey = text;
                return true;
            case bool boolValue:
                text = boolValue ? "1" : "0";
                distinctKey = text;
                return true;
            default:
                text = value?.ToString() ?? string.Empty;
                distinctKey = text;
                return true;
        }
    }
}
