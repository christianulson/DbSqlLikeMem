using System.Text.Json;
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
        var parts = values.Select(static value =>
        {
            if (value is null or DBNull)
                return "null";

            if (value is JsonElement element)
                return element.GetRawText();

            return JsonSerializer.Serialize(value);
        });

        return "[" + string.Join(",", parts) + "]";
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
