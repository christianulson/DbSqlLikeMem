using FluentAssertions;
using System.Text.Json;

namespace DbSqlLikeMem.TestTools.Json;

internal static class JsonTextAssertions
{
    internal static void ShouldMatchJsonText(string? actual, string? expected)
    {
        NormalizeJsonText(actual).Should().Be(NormalizeJsonText(expected));
    }

    private static string? NormalizeJsonText(string? json)
    {
        if (json is null)
            return null;

        using var document = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(document.RootElement);
    }
}
