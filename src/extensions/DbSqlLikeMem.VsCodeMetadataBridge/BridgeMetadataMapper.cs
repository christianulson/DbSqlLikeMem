using System.Globalization;
using System.Text;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VsCodeMetadataBridge;

internal static class BridgeMetadataMapper
{
    public static BridgeDatabaseObjectReference ToBridgeObject(DatabaseObjectReference reference)
    {
        var properties = reference.Properties ?? new Dictionary<string, string>(StringComparer.Ordinal);

        var columns = ParseColumns(Get(properties, "Columns"));
        var foreignKeys = ParseForeignKeys(Get(properties, "ForeignKeys"), reference.Schema);
        var sequence = CreateSequenceMetadata(properties);

        return new BridgeDatabaseObjectReference(
            reference.Schema,
            reference.Name,
            reference.Type.ToString(),
            columns.Count > 0 ? columns : null,
            foreignKeys.Count > 0 ? foreignKeys : null,
            sequence,
            Get(properties, "RequiredIn"),
            Get(properties, "OptionalIn"),
            Get(properties, "OutParams"),
            Get(properties, "ReturnParam"),
            Get(properties, "Parameters"),
            Get(properties, "ReturnTypeSql"),
            Get(properties, "BodySql"));
    }

    private static BridgeSequenceMetadataReference? CreateSequenceMetadata(IReadOnlyDictionary<string, string> properties)
    {
        var startValue = Get(properties, "StartValue");
        var incrementBy = Get(properties, "IncrementBy");
        var currentValue = Get(properties, "CurrentValue");
        if (string.IsNullOrWhiteSpace(startValue)
            && string.IsNullOrWhiteSpace(incrementBy)
            && string.IsNullOrWhiteSpace(currentValue))
        {
            return null;
        }

        return new BridgeSequenceMetadataReference(
            string.IsNullOrWhiteSpace(startValue) ? null : startValue,
            string.IsNullOrWhiteSpace(incrementBy) ? null : incrementBy,
            string.IsNullOrWhiteSpace(currentValue) ? null : currentValue);
    }

    private static List<BridgeColumnReference> ParseColumns(string text)
    {
        var result = new List<BridgeColumnReference>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 4)
            {
                continue;
            }

            result.Add(new BridgeColumnReference(
                Unescape(parts[0]),
                Unescape(parts[1]),
                parts[3] == "1",
                ParseInt(parts[2])));
        }

        return result
            .OrderBy(x => x.OrdinalPosition)
            .ThenBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static List<BridgeForeignKeyReference> ParseForeignKeys(string text, string schema)
    {
        var result = new List<BridgeForeignKeyReference>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 3)
            {
                continue;
            }

            var column = Unescape(parts[0]);
            var refTable = Unescape(parts[1]);
            var refColumn = Unescape(parts[2]);
            var name = string.IsNullOrWhiteSpace(column)
                ? $"FK_{refTable}_{refColumn}"
                : column;

            result.Add(new BridgeForeignKeyReference(name, string.IsNullOrWhiteSpace(schema) ? string.Empty : schema, refTable));
        }

        return result
            .OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string Get(IReadOnlyDictionary<string, string> properties, string key)
        => properties.TryGetValue(key, out var value) ? value : string.Empty;

    private static int ParseInt(string value)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result) ? result : 0;

    private static string Unescape(string value)
    {
        var sb = new StringBuilder(value.Length);
        var escape = false;
        foreach (var ch in value)
        {
            if (escape)
            {
                sb.Append(ch);
                escape = false;
                continue;
            }

            if (ch == '\\')
            {
                escape = true;
                continue;
            }

            sb.Append(ch);
        }

        return sb.ToString();
    }

    private static List<string> SplitEscaped(string value, char separator)
    {
        var parts = new List<string>();
        if (string.IsNullOrEmpty(value))
        {
            return parts;
        }

        var sb = new StringBuilder();
        var escape = false;
        foreach (var ch in value)
        {
            if (escape)
            {
                sb.Append(ch);
                escape = false;
                continue;
            }

            if (ch == '\\')
            {
                escape = true;
                continue;
            }

            if (ch == separator)
            {
                parts.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            sb.Append(ch);
        }

        parts.Add(sb.ToString());
        return parts;
    }
}
