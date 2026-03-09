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
        if (dbObject.Type == DatabaseObjectType.Sequence)
            return BuildSequence(dbObject, @namespace);

        var effectiveDatabaseType = string.IsNullOrWhiteSpace(databaseType) ? "MySql" : databaseType;
        var className = $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}{dbObject.Type}Factory";
        var methodName = $"Create{dbObject.Type}{GenerationRuleSet.ToPascalCase(dbObject.Name)}";

        var columns = ReadColumns(dbObject);
        var primaryKey = ReadPrimaryKey(dbObject);
        var indexes = ReadIndexes(dbObject);
        var foreignKeys = ReadForeignKeys(dbObject);

        var sb = new StringBuilder();
        AppendFileHeader(sb, dbObject, @namespace);
        sb.AppendLine($"public static class {className}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static ITableMock {methodName}(this DbMock db)");
        sb.AppendLine("    {");
        sb.AppendLine($"        var table = db.AddTable({Literal(dbObject.Name)});");
        AppendColumns(sb, columns, effectiveDatabaseType);
        sb.AppendLine();

        AppendPrimaryKey(sb, primaryKey);
        AppendIndexes(sb, indexes);
        AppendForeignKeys(sb, dbObject, foreignKeys);
        sb.AppendLine("        return table;");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static void AppendFileHeader(StringBuilder sb, DatabaseObjectReference dbObject, string? @namespace)
    {
        AppendNamespace(sb, @namespace);
        AppendObjectMetadata(sb, dbObject);
    }

    private static void AppendNamespace(StringBuilder sb, string? @namespace)
    {
        if (string.IsNullOrWhiteSpace(@namespace))
            return;

        sb.AppendLine($"namespace {@namespace};");
        sb.AppendLine();
    }

    private static void AppendObjectMetadata(StringBuilder sb, DatabaseObjectReference dbObject)
    {
        sb.AppendLine($"// DBSqlLikeMem:Schema={dbObject.Schema}");
        sb.AppendLine($"// DBSqlLikeMem:Object={dbObject.Name}");
        sb.AppendLine($"// DBSqlLikeMem:Type={dbObject.Type}");
        sb.AppendLine($"// DBSqlLikeMem:Columns={Get(dbObject, "Columns")}");
        sb.AppendLine($"// DBSqlLikeMem:PrimaryKey={Get(dbObject, "PrimaryKey")}");
        sb.AppendLine($"// DBSqlLikeMem:Indexes={Get(dbObject, "Indexes")}");
        sb.AppendLine($"// DBSqlLikeMem:ForeignKeys={Get(dbObject, "ForeignKeys")}");
    }

    private static void AppendColumns(StringBuilder sb, IReadOnlyList<ColumnMeta> columns, string effectiveDatabaseType)
    {
        foreach (var column in columns.OrderBy(c => c.Ordinal))
        {
            var ctor = BuildColumnConstructor(column, effectiveDatabaseType);
            sb.AppendLine();
            sb.Append($"        table.AddColumn({Literal(column.Name)}, {ctor})");
            if (!string.IsNullOrWhiteSpace(column.Generated)
                && GenerationRuleSet.TryConvertIfIsNull(column.Generated, out var genCode))
            {
                sb.Append($"\n            .GetGenValue = {genCode}");
            }

            sb.Append(';');
        }
    }

    private static string BuildColumnConstructor(ColumnMeta column, string effectiveDatabaseType)
    {
        var mappedDbType = GenerationRuleSet.MapDbType(
            column.DataType,
            column.CharMaxLen,
            column.NumPrecision,
            column.Name,
            effectiveDatabaseType);
        var ctor = $"DbType.{mappedDbType}, {Bool(column.IsNullable)}";
        if (column.IsIdentity)
            ctor += ", true";
        if (!string.IsNullOrWhiteSpace(column.DefaultValue) && GenerationRuleSet.IsSimpleLiteralDefault(column.DefaultValue))
            ctor += $", defaultValue: {GenerationRuleSet.FormatDefaultLiteral(column.DefaultValue, mappedDbType)}";
        if (column.CharMaxLen is > 0 and <= int.MaxValue)
            ctor += $", size: {(int)column.CharMaxLen.Value}";
        if (column.NumScale is >= 0)
            ctor += $", decimalPlaces: {column.NumScale.Value}";

        var enums = GenerationRuleSet.TryParseEnumValues(column.ColumnType);
        if (enums.Length > 0)
            ctor += $", enumValues: [{string.Join(", ", enums.Select(Literal))}]";

        return ctor;
    }

    private static void AppendPrimaryKey(StringBuilder sb, IReadOnlyList<string> primaryKey)
    {
        if (primaryKey.Count == 0)
            return;

        sb.AppendLine($"        table.AddPrimaryKeyIndexes({string.Join(",", primaryKey.Select(_ => $"\"{_}\""))});");
        sb.AppendLine($"        table.CreateIndex(\"PRIMARY\", [{string.Join(", ", primaryKey.Select(Literal))}], unique: true);");
    }

    private static void AppendIndexes(StringBuilder sb, IReadOnlyList<IndexMeta> indexes)
    {
        foreach (var idx in indexes.OrderBy(i => i.Name, StringComparer.OrdinalIgnoreCase))
            sb.AppendLine($"        table.CreateIndex({Literal(idx.Name)}, [{string.Join(", ", idx.Columns.Select(Literal))}], unique: {Bool(idx.Unique)});");
    }

    private static void AppendForeignKeys(
        StringBuilder sb,
        DatabaseObjectReference dbObject,
        IReadOnlyList<ForeignKeyMeta> foreignKeys)
    {
        foreach (var fk in foreignKeys)
        {
            sb.AppendLine($"        table.CreateForeignKey({Literal($"FK_{dbObject.Name}_{fk.Column}_{fk.RefTable}_{fk.RefColumn}")}, {Literal(fk.RefTable)}, [({Literal(fk.Column)}, {Literal(fk.RefColumn)})]);");
        }
    }

    private static string BuildSequence(DatabaseObjectReference dbObject, string? @namespace)
    {
        var className = $"{GenerationRuleSet.ToPascalCase(dbObject.Name)}{dbObject.Type}Factory";
        var methodName = $"Create{dbObject.Type}{GenerationRuleSet.ToPascalCase(dbObject.Name)}";
        var startValue = ParseNullableLong(Get(dbObject, "StartValue")) ?? 1L;
        var incrementBy = ParseNullableLong(Get(dbObject, "IncrementBy")) ?? 1L;
        var currentValue = ParseNullableLong(Get(dbObject, "CurrentValue"));

        var sb = new StringBuilder();
        AppendSequenceFileHeader(sb, dbObject, @namespace);
        sb.AppendLine($"public static class {className}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static SequenceDef {methodName}(this DbMock db)");
        sb.AppendLine("    {");
        sb.AppendLine($"        return db.AddSequence({Literal(dbObject.Name)}, startValue: {startValue}L, incrementBy: {incrementBy}L, currentValue: {(currentValue.HasValue ? $"{currentValue.Value}L" : "null")}, schemaName: {Literal(dbObject.Schema)});");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static void AppendSequenceFileHeader(StringBuilder sb, DatabaseObjectReference dbObject, string? @namespace)
    {
        AppendNamespace(sb, @namespace);
        sb.AppendLine($"// DBSqlLikeMem:Schema={dbObject.Schema}");
        sb.AppendLine($"// DBSqlLikeMem:Object={dbObject.Name}");
        sb.AppendLine($"// DBSqlLikeMem:Type={dbObject.Type}");
        sb.AppendLine($"// DBSqlLikeMem:StartValue={Get(dbObject, "StartValue")}");
        sb.AppendLine($"// DBSqlLikeMem:IncrementBy={Get(dbObject, "IncrementBy")}");
        sb.AppendLine($"// DBSqlLikeMem:CurrentValue={Get(dbObject, "CurrentValue")}");
    }

    private static string Get(DatabaseObjectReference obj, string key)
        => obj.Properties?.TryGetValue(key, out var v) == true ? v : string.Empty;

    private static List<ColumnMeta> ReadColumns(DatabaseObjectReference dbObject)
        => ParseColumns(Get(dbObject, "Columns"));

    private static List<string> ReadPrimaryKey(DatabaseObjectReference dbObject)
        => ParseCommaSeparated(Get(dbObject, "PrimaryKey"));

    private static List<IndexMeta> ReadIndexes(DatabaseObjectReference dbObject)
        => ParseIndexes(Get(dbObject, "Indexes"));

    private static List<ForeignKeyMeta> ReadForeignKeys(DatabaseObjectReference dbObject)
        => ParseForeignKeys(Get(dbObject, "ForeignKeys"));

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
