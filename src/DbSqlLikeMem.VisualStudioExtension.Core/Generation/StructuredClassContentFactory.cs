using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using System.Globalization;
using System.Text;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public static class StructuredClassContentFactory
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string Build(DatabaseObjectReference dbObject, string? @namespace = null, string? databaseType = null)
    {
        var effectiveDatabaseType = string.IsNullOrWhiteSpace(databaseType) ? "MySql" : databaseType;
        var className = $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}{dbObject.Type}Factory";
        var methodName = $"Create{dbObject.Type}{GenerationRuleSet.ToPascalCase(dbObject.Name)}";

        var columns = ParseColumns(Get(dbObject, "Columns"));
        var primaryKey = ParseCommaSeparated(Get(dbObject, "PrimaryKey"));
        var indexes = ParseIndexes(Get(dbObject, "Indexes"));
        var foreignKeys = ParseForeignKeys(Get(dbObject, "ForeignKeys"));

        var sb = new StringBuilder();
        if (!string.IsNullOrWhiteSpace(@namespace))
        {
            sb.AppendLine($"namespace {@namespace};");
            sb.AppendLine();
        }

        sb.AppendLine($"// DBSqlLikeMem:Schema={dbObject.Schema}");
        sb.AppendLine($"// DBSqlLikeMem:Object={dbObject.Name}");
        sb.AppendLine($"// DBSqlLikeMem:Type={dbObject.Type}");
        sb.AppendLine($"// DBSqlLikeMem:Columns={Get(dbObject, "Columns")}");
        sb.AppendLine($"// DBSqlLikeMem:PrimaryKey={Get(dbObject, "PrimaryKey")}");
        sb.AppendLine($"// DBSqlLikeMem:Indexes={Get(dbObject, "Indexes")}");
        sb.AppendLine($"// DBSqlLikeMem:ForeignKeys={Get(dbObject, "ForeignKeys")}");

        sb.AppendLine($"public static class {className}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static ITableMock {methodName}(this DbMock db)");
        sb.AppendLine("    {");
        sb.AppendLine($"        var table = db.AddTable({Literal(dbObject.Name)});");

        foreach (var c in columns.OrderBy(c => c.Ordinal))
        {
            var mappedDbType = GenerationRuleSet.MapDbType(c.DataType, c.CharMaxLen, c.NumPrecision, c.Name, effectiveDatabaseType);
            var ctor = $"DbType.{mappedDbType}, {Bool(c.IsNullable)}";
            if (c.IsIdentity) ctor += ", true";
            if (!string.IsNullOrWhiteSpace(c.DefaultValue) && GenerationRuleSet.IsSimpleLiteralDefault(c.DefaultValue))
                ctor += $", defaultValue: {GenerationRuleSet.FormatDefaultLiteral(c.DefaultValue, mappedDbType)}";
            if (c.CharMaxLen is > 0 and <= int.MaxValue)
                ctor += $", size: {(int)c.CharMaxLen.Value}";
            if (c.NumScale is >= 0)
                ctor += $", decimalPlaces: {c.NumScale.Value}";

            var enums = GenerationRuleSet.TryParseEnumValues(c.ColumnType);
            if (enums.Length > 0)
                ctor += $", enumValues: [{string.Join(", ", enums.Select(Literal))}]";

            sb.AppendLine();
            sb.Append($"        table.AddColumn({Literal(c.Name)}, {ctor})");
            if (!string.IsNullOrWhiteSpace(c.Generated) && GenerationRuleSet.TryConvertIfIsNull(c.Generated, out var genCode))
                sb.Append($"\n            .GetGenValue = {genCode}");

            sb.Append(';');
        }
        sb.AppendLine();

        if (primaryKey.Count > 0)
        {
            sb.AppendLine($"        table.AddPrimaryKeyIndexes({string.Join(",", primaryKey.Select(_ => $"\"{_}\""))});");
            sb.AppendLine($"        table.CreateIndex(\"PRIMARY\", [{string.Join(", ", primaryKey.Select(Literal))}], unique: true);");
        }

        foreach (var idx in indexes.OrderBy(i => i.Name, StringComparer.OrdinalIgnoreCase))
        {
            sb.AppendLine($"        table.CreateIndex({Literal(idx.Name)}, [{string.Join(", ", idx.Columns.Select(Literal))}], unique: {Bool(idx.Unique)});");
        }

        foreach (var fk in foreignKeys)
        {
            sb.AppendLine($"        table.CreateForeignKey({Literal(fk.Column)}, {Literal(fk.RefTable)}, {Literal(fk.RefColumn)});");
        }

        sb.AppendLine("        return table;");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string Get(DatabaseObjectReference obj, string key)
        => obj.Properties?.TryGetValue(key, out var v) == true ? v : string.Empty;

    private static List<ColumnMeta> ParseColumns(string text)
    {
        var result = new List<ColumnMeta>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 10) continue;
            result.Add(new ColumnMeta(
                Name: Unescape(parts[0]),
                DataType: Unescape(parts[1]),
                Ordinal: ParseInt(parts[2]),
                IsNullable: parts[3] == "1",
                IsIdentity: parts[4] == "1",
                DefaultValue: Unescape(parts[5]),
                CharMaxLen: ParseNullableLong(parts[6]),
                NumScale: ParseNullableInt(parts[7]),
                ColumnType: Unescape(parts[8]),
                Generated: Unescape(parts[9]),
                NumPrecision: ParseNullableInt(parts.Count > 10 ? parts[10] : string.Empty)));
        }
        return result;
    }

    private static List<string> ParseCommaSeparated(string text)
        => [.. SplitEscaped(text, ',').Select(Unescape).Where(s => !string.IsNullOrWhiteSpace(s))];

    private static List<IndexMeta> ParseIndexes(string text)
    {
        var result = new List<IndexMeta>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 3) continue;
            result.Add(new IndexMeta(Unescape(parts[0]), parts[1] == "1", [.. SplitEscaped(parts[2], ',').Select(Unescape).Where(x => !string.IsNullOrWhiteSpace(x))]));
        }
        return result;
    }

    private static List<ForeignKeyMeta> ParseForeignKeys(string text)
    {
        var result = new List<ForeignKeyMeta>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 3) continue;
            result.Add(new ForeignKeyMeta(Unescape(parts[0]), Unescape(parts[1]), Unescape(parts[2])));
        }
        return result;
    }

    private static string Bool(bool value) => value ? "true" : "false";
    private static string Literal(string value) => GenerationRuleSet.Literal(value);

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
        if (string.IsNullOrEmpty(value)) return parts;
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

    private static int ParseInt(string value)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : 0;

    private static int? ParseNullableInt(string value)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;

    private static long? ParseNullableLong(string value)
        => long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;

    private sealed record ColumnMeta(string Name, string DataType, int Ordinal, bool IsNullable, bool IsIdentity, string DefaultValue, long? CharMaxLen, int? NumScale, string ColumnType, string Generated, int? NumPrecision);
    private sealed record IndexMeta(string Name, bool Unique, List<string> Columns);
    private sealed record ForeignKeyMeta(string Column, string RefTable, string RefColumn);

}
