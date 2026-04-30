using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Normalizes database names and expressions into CLR-friendly generation tokens.
/// PT: Normaliza nomes de banco e expressoes em tokens de geracao amigaveis ao CLR.
/// </summary>
public static class GenerationRuleSet
{
    private static readonly Regex IsNullExpression = new(
        @"if\s*\(\s*\(\s*`(?<col>\w+)`\s+is\s+null\s*\)\s*,\s*(?<val>[^,]+)\s*,\s*null\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(500));

    private static readonly Regex OracleSequenceDefaultExpression = new(
        @"^\s*(?:(?<schema>""(?:[^""]|"""")*""|[A-Za-z_][A-Za-z0-9_$#]*)\s*\.\s*)?(?<sequence>""(?:[^""]|"""")*""|[A-Za-z_][A-Za-z0-9_$#]*)\s*\.\s*nextval(?:\s*\(\s*\))?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled,
        TimeSpan.FromMilliseconds(500));

    /// <summary>
    /// EN: Converts a database identifier into PascalCase for generated names.
    /// PT: Converte um identificador de banco para PascalCase nos nomes gerados.
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
    /// EN: Maps a database column type to a CLR-friendly generated type name.
    /// PT: Mapeia um tipo de coluna de banco para um nome de tipo amigavel ao CLR.
    /// </summary>
    public static string MapDbType(
        string dataType,
        long? charMaxLen,
        int? numPrecision,
        string columnName,
        string? databaseType = null)
        => MapDbType(dataType, charMaxLen, numPrecision, null, columnName, databaseType);

    /// <summary>
    /// EN: Maps a database column type to a CLR-friendly generated type name, including numeric scale when available.
    /// PT: Mapeia um tipo de coluna de banco para um nome de tipo amigavel ao CLR, incluindo a escala numerica quando disponivel.
    /// </summary>
    public static string MapDbType(
        string dataType,
        long? charMaxLen,
        int? numPrecision,
        int? numScale,
        string columnName,
        string? databaseType = null)
    {
        var strategy = GenerationRuleStrategyResolver.Resolve(databaseType);
        return strategy.MapDbType(new GenerationTypeContext(dataType, charMaxLen, numPrecision, numScale, columnName));
    }

    /// <summary>
    /// EN: Checks whether a default expression is a simple literal.
    /// PT: Verifica se uma expressao padrao e um literal simples.
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
    /// EN: Formats a default value expression for generated C# code.
    /// PT: Formata uma expressao de valor padrao para o codigo C# gerado.
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
    /// EN: Formats an Oracle sequence default as a DbMock sequence reference.
    /// PT: Formata um default de sequence do Oracle como uma referencia de sequence do DbMock.
    /// </summary>
    /// <param name="value">EN: Oracle default expression. PT: Expressao de default do Oracle.</param>
    /// <param name="databaseType">EN: Source database type. PT: Tipo de banco de origem.</param>
    /// <param name="schemaName">EN: Fallback schema name when the expression omits it. PT: Nome do schema de fallback quando a expressao o omite.</param>
    /// <param name="code">EN: Generated C# code when the expression is a sequence. PT: Codigo C# gerado quando a expressao for uma sequence.</param>
    /// <returns>EN: True when the default is a sequence reference. PT: True quando o default for uma referencia de sequence.</returns>
    public static bool TryFormatSequenceDefaultValue(
        string value,
        string? databaseType,
        string? schemaName,
        out string code)
    {
        code = string.Empty;
        if (!string.Equals(databaseType, "Oracle", StringComparison.OrdinalIgnoreCase))
            return false;

        var match = OracleSequenceDefaultExpression.Match(TrimOuterParentheses(value));
        if (!match.Success)
            return false;

        var sequenceName = UnquoteIdentifier(match.Groups["sequence"].Value);
        var sequenceSchema = match.Groups["schema"].Success
            ? UnquoteIdentifier(match.Groups["schema"].Value)
            : string.IsNullOrWhiteSpace(schemaName) ? null : schemaName;

        var ex = $"throw new ArgumentException(\"Sequence \" + {Literal(sequenceName)} + \" not found\")";

        code = string.IsNullOrWhiteSpace(sequenceSchema)
            ? $"db.TryGetSequence({Literal(sequenceName)}, out var seq) ? seq : {ex}"
            : $"db.TryGetSequence({Literal(sequenceName)}, out var seq, schemaName: {Literal(sequenceSchema!)}) ? seq : {ex}";
        return true;
    }

    /// <summary>
    /// EN: Parses enum and set literals into discrete values.
    /// PT: Analisa literais enum e set em valores discretos.
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
    /// EN: Converts an SQL IS NULL expression into generated C# code when supported.
    /// PT: Converte uma expressao SQL IS NULL em codigo C# gerado quando suportado.
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
    /// EN: Escapes a string so it can be emitted as a C# string literal.
    /// PT: Escapa uma string para que ela possa ser emitida como literal de string em C#.
    /// </summary>
    public static string Literal(string value)
        => "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";

    private static bool IsNumericDbType(string dbType)
        => NumericDbTypes.Contains(dbType);

    private static readonly HashSet<string> NumericDbTypes = new(StringComparer.Ordinal)
    {
        "Byte",
        "Int16",
        "Int32",
        "Int64",
        "Decimal",
        "Double",
        "Single",
        "UInt64"
    };

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

    private static string UnquoteIdentifier(string value)
        => value.Length >= 2 && value[0] == '"' && value[value.Length - 1] == '"'
            ? value.Substring(1, value.Length - 2).Replace("\"\"", "\"")
            : value;

    private static string TrimOuterParentheses(string value)
    {
        var result = value.Trim();
        while (result.Length >= 2 && result[0] == '(' && result[result.Length - 1] == ')')
        {
            var inner = result.Substring(1, result.Length - 2).Trim();
            if (inner.Length + 2 != result.Length)
                break;

            result = inner;
        }

        return result;
    }
}
