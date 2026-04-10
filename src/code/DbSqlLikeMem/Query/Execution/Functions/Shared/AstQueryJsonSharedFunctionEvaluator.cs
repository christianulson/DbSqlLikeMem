using System.Text.Json.Nodes;

namespace DbSqlLikeMem;

internal static class AstQueryJsonSharedFunctionEvaluator
{
    internal static bool TryParseJsonElement(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
            return true;
        }
        catch
        {
            element = default;
            return false;
        }
    }

    internal static string BuildJsonArray(IEnumerable<object?> values)
    {
        var estimatedCapacity = values is ICollection<object?> collection
            ? Math.Max(2, (collection.Count * 8) + 2)
            : values is IReadOnlyCollection<object?> readOnlyCollection
                ? Math.Max(2, (readOnlyCollection.Count * 8) + 2)
                : 16;
        var builder = new StringBuilder(estimatedCapacity);
        builder.Append('[');
        var isFirst = true;
        foreach (var value in values)
        {
            if (!isFirst)
                builder.Append(',');

            isFirst = false;

            if (value is null or DBNull)
            {
                builder.Append("null");
                continue;
            }

            if (value is JsonElement element)
            {
                builder.Append(element.GetRawText());
                continue;
            }

            builder.Append(JsonSerializer.Serialize(value));
        }

        builder.Append(']');
        return builder.ToString();
    }

    internal static JsonNode CloneJsonNode(JsonNode node)
        => JsonNode.Parse(node.ToJsonString())!;

    internal static void StripJsonNullProperties(JsonNode node)
    {
        if (node is JsonObject obj)
        {
            var toRemove = obj
                .Where(pair => pair.Value is null)
                .Select(pair => pair.Key)
                .ToArray();

            foreach (var key in toRemove)
                obj.Remove(key);

            foreach (var pair in obj)
            {
                if (pair.Value is not null)
                    StripJsonNullProperties(pair.Value);
            }

            return;
        }

        if (node is JsonArray array)
        {
            foreach (var item in array)
            {
                if (item is not null)
                    StripJsonNullProperties(item);
            }
        }
    }
}
