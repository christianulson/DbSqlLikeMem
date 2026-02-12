using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public static class GenerationRuleSet
{
    private static readonly Regex IsNullExpression = new(
        @"if\s*\(\s*\(\s*`(?<col>\w+)`\s+is\s+null\s*\)\s*,\s*(?<val>[^,]+)\s*,\s*null\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(500));

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string ToPascalCase(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return "Object";
        }

        var parts = value
            .Split(['_', '-', '.', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(Capitalize)
            .Where(static part => !string.IsNullOrWhiteSpace(part));

        var joined = string.Concat(parts);
        if (!string.IsNullOrWhiteSpace(joined))
        {
            return joined;
        }

        var filtered = new string([.. value.Where(char.IsLetterOrDigit)]);
        return string.IsNullOrWhiteSpace(filtered) ? "Object" : Capitalize(filtered);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string MapDbType(
        string dataType,
        long? charMaxLen,
        int? numPrecision,
        string columnName,
        string? databaseType = null)
    {
        var strategy = GenerationRuleStrategyResolver.Resolve(databaseType);
        return strategy.MapDbType(new GenerationTypeContext(dataType, charMaxLen, numPrecision, columnName));
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static bool IsSimpleLiteralDefault(string value)
    {
        var normalized = value.Trim();
        if (Regex.IsMatch(normalized, @"\b\w+\s*\([^)]*\)")) return false;
        if (normalized.Equals("current_timestamp", StringComparison.OrdinalIgnoreCase)) return false;
        if (normalized.Equals("null", StringComparison.OrdinalIgnoreCase)) return false;
        return true;
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string FormatDefaultLiteral(string value, string dbType)
    {
        if (dbType == "Boolean")
        {
            return value.Contains("'0'", StringComparison.InvariantCultureIgnoreCase) ? "false" : "true";
        }

        if (!IsNumericDbType(dbType))
        {
            return Literal(value.Trim('(', ')', '\''));
        }

        return value.Trim('(', ')', ' ');
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string[] TryParseEnumValues(string columnType)
    {
        var match = Regex.Match(columnType, @"^(enum|set)\((.*)\)$", RegexOptions.IgnoreCase);
        if (!match.Success)
        {
            return [];
        }

        return [.. Regex.Matches(match.Groups[2].Value, @"'((?:\\'|[^'])*)'")
            .Cast<Match>()
            .Select(m => m.Groups[1].Value.Replace("\\'", "'"))];
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static bool TryConvertIfIsNull(string sqlExpr, out string code)
    {
        var match = IsNullExpression.Match(sqlExpr);
        if (!match.Success)
        {
            code = string.Empty;
            return false;
        }

        var column = match.Groups["col"].Value;
        var value = match.Groups["val"].Value;
        code = $"(row, tb) => !row.TryGetValue(tb.Columns[{Literal(column)}].Index, out var dtDel) || dtDel is null ? (byte?){value} : null";
        return true;
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static string Literal(string value)
        => "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";

    private static bool IsNumericDbType(string dbType)
        => dbType is "Byte" or "Int16" or "Int32" or "Int64" or "Decimal" or "Double" or "Single" or "UInt64";

    private static string Capitalize(string part)
    {
        if (string.IsNullOrWhiteSpace(part))
        {
            return string.Empty;
        }

        var normalized = new StringBuilder(part.Length);
        foreach (var ch in part)
        {
            if (char.IsLetterOrDigit(ch))
            {
                normalized.Append(ch);
            }
        }

        if (normalized.Length == 0)
        {
            return string.Empty;
        }

        var cleaned = normalized.ToString();
        if (cleaned.Length == 1)
        {
            return cleaned.ToUpperInvariant();
        }

        return string.Concat(char.ToUpper(cleaned[0], CultureInfo.InvariantCulture), cleaned.Substring(1));
    }
}
