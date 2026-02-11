using DbSqlLikeMem.VisualStudioExtension.Core.Models;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public static partial class StructuredClassContentFactory
{
    public static string Build(DatabaseObjectReference dbObject, string? @namespace = null)
    {
        var className = $"{ToPascalCase(dbObject.Name)}{dbObject.Type}Factory";
        var methodName = $"Create{dbObject.Type}{ToPascalCase(dbObject.Name)}";

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
            var ctor = $"new({c.Ordinal}, DbType.{MapDbType(c)}, {Bool(c.IsNullable)}";
            if (c.IsIdentity) ctor += ", true";
            ctor += ")";
            sb.AppendLine($"        table.Columns[{Literal(c.Name)}] = {ctor};");

            if (!string.IsNullOrWhiteSpace(c.DefaultValue) && IsSimpleLiteralDefault(c.DefaultValue))
            {
                sb.AppendLine($"        table.Columns[{Literal(c.Name)}].DefaultValue = {FormatDefaultLiteral(c.DefaultValue, MapDbType(c))};");
            }

            if (c.CharMaxLen is > 0 and <= int.MaxValue)
            {
                sb.AppendLine($"        table.Columns[{Literal(c.Name)}].Size = {(int)c.CharMaxLen.Value};");
            }

            if (c.NumScale is >= 0)
            {
                sb.AppendLine($"        table.Columns[{Literal(c.Name)}].DecimalPlaces = {c.NumScale.Value};");
            }

            var enums = TryParseEnumValues(c.ColumnType);
            if (enums.Length > 0)
            {
                sb.AppendLine($"        table.Columns[{Literal(c.Name)}].EnumValues = new[] {{ {string.Join(", ", enums.Select(Literal))} }};");
            }

            if (!string.IsNullOrWhiteSpace(c.Generated) && TryConvertIfIsNull(c.Generated, out var genCode))
            {
                sb.AppendLine($"        table.Columns[{Literal(c.Name)}].GetGenValue = {genCode};");
            }
        }

        if (primaryKey.Count > 0)
        {
            foreach (var pk in primaryKey)
            {
                sb.AppendLine($"        table.PrimaryKeyIndexes.Add(table.Columns[{Literal(pk)}]?.Index);");
            }
            sb.AppendLine($"        table.CreateIndex(new IndexDef(\"PRIMARY\", [{string.Join(", ", primaryKey.Select(Literal))}], unique: true));");
        }

        foreach (var idx in indexes.OrderBy(i => i.Name, StringComparer.OrdinalIgnoreCase))
        {
            sb.AppendLine($"        table.CreateIndex(new IndexDef({Literal(idx.Name)}, [{string.Join(", ", idx.Columns.Select(Literal))}], unique: {Bool(idx.Unique)}));");
        }

        foreach (var fk in foreignKeys)
        {
            sb.AppendLine($"        table.ForeignKeys.Add(({Literal(fk.Column)}, {Literal(fk.RefTable)}, {Literal(fk.RefColumn)}));");
        }

        sb.AppendLine("        return table;");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string Get(DatabaseObjectReference obj, string key)
        => obj.Properties?.TryGetValue(key, out var v) == true ? v : string.Empty;

    private static string MapDbType(ColumnMeta c)
    {
        var t = c.DataType.ToLowerInvariant();
        var looksGuid = (t is "binary" or "varbinary") && c.CharMaxLen == 16
            || (t is "char" && c.CharMaxLen == 36 && (c.Name.EndsWith("guid", StringComparison.OrdinalIgnoreCase) || c.Name.EndsWith("uuid", StringComparison.OrdinalIgnoreCase)));
        if (looksGuid) return "Guid";

        return t switch
        {
            "tinyint" => "Byte",
            "smallint" => "Int16",
            "mediumint" => "Int32",
            "int" or "integer" => "Int32",
            "bigint" => "Int64",
            "bit" => "Boolean",
            "decimal" or "numeric" => "Decimal",
            "double" => "Double",
            "float" or "real" => "Single",
            "date" => "Date",
            "datetime" or "timestamp" => "DateTime",
            "time" => "Time",
            "char" or "nchar" or "varchar" or "nvarchar" or "text" or "tinytext" or "mediumtext" or "longtext" or "json" or "enum" or "set" => "String",
            "binary" or "varbinary" or "blob" or "tinyblob" or "mediumblob" or "longblob" or "bytea" => "Binary",
            "uniqueidentifier" or "uuid" => "Guid",
            "bool" or "boolean" => "Boolean",
            _ => "Object"
        };
    }

    private static string FormatDefaultLiteral(string value, string dbType)
    {
        if (dbType == "Boolean")
        {
            var cleaned = value.Trim('(', ')', '\'', ' ');
            return cleaned is "1" or "true" or "TRUE" ? "true" : "false";
        }

        if (dbType is "Byte" or "Int16" or "Int32" or "Int64" or "Decimal" or "Double" or "Single")
        {
            return value.Trim('(', ')', ' ');
        }

        return Literal(value.Trim('(', ')', '\''));
    }

    private static bool IsSimpleLiteralDefault(string value)
    {
        var v = value.Trim();
        if (Regex.IsMatch(v, @"\(\s*\)$")) return false;
        if (v.Equals("current_timestamp", StringComparison.OrdinalIgnoreCase)) return false;
        if (v.Equals("null", StringComparison.OrdinalIgnoreCase)) return false;
        return true;
    }

    private static bool TryConvertIfIsNull(string sqlExpr, out string code)
    {
        var m = GeneratedRegexPattern().Match(sqlExpr);
        if (!m.Success)
        {
            code = string.Empty;
            return false;
        }

        var col = m.Groups["col"].Value;
        var val = m.Groups["val"].Value;
        code = $"(row, tb) => !row.TryGetValue(tb.Columns[{Literal(col)}].Index, out var dtDel) || dtDel is null ? (byte?){val} : null";
        return true;
    }

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
                Generated: Unescape(parts[9])));
        }
        return result;
    }

    private static List<string> ParseCommaSeparated(string text)
        => SplitEscaped(text, ',').Select(Unescape).Where(s => !string.IsNullOrWhiteSpace(s)).ToList();

    private static List<IndexMeta> ParseIndexes(string text)
    {
        var result = new List<IndexMeta>();
        foreach (var item in SplitEscaped(text, ';'))
        {
            var parts = SplitEscaped(item, '|');
            if (parts.Count < 3) continue;
            result.Add(new IndexMeta(Unescape(parts[0]), parts[1] == "1", SplitEscaped(parts[2], ',').Select(Unescape).Where(x => !string.IsNullOrWhiteSpace(x)).ToList()));
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

    private static string ToPascalCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) return "Object";
        var parts = value.Split(['_', '-', '.', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(Capitalize)
            .Where(p => !string.IsNullOrWhiteSpace(p));
        var joined = string.Concat(parts);
        return string.IsNullOrWhiteSpace(joined) ? "Object" : joined;
    }

    private static string Capitalize(string value)
    {
        var cleaned = new string(value.Where(char.IsLetterOrDigit).ToArray());
        if (string.IsNullOrWhiteSpace(cleaned)) return string.Empty;
        return cleaned.Length == 1 ? cleaned.ToUpperInvariant() : string.Concat(char.ToUpper(cleaned[0], CultureInfo.InvariantCulture), cleaned[1..]);
    }

    private static string Bool(bool value) => value ? "true" : "false";
    private static string Literal(string value) => $"\"{value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal)}\"";

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

    private sealed record ColumnMeta(string Name, string DataType, int Ordinal, bool IsNullable, bool IsIdentity, string DefaultValue, long? CharMaxLen, int? NumScale, string ColumnType, string Generated);
    private sealed record IndexMeta(string Name, bool Unique, List<string> Columns);
    private sealed record ForeignKeyMeta(string Column, string RefTable, string RefColumn);

    [GeneratedRegex(@"if\s*\(\s*\(\s*`(?<col>\w+)`\s+is\s+null\s*\)\s*,\s*(?<val>[^,]+)\s*,\s*null\s*\)", RegexOptions.IgnoreCase)]
    private static partial Regex GeneratedRegexPattern();
}
