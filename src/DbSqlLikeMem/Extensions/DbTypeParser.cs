namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the class DbTypeParser.
/// PT: Define a classe DbTypeParser.
/// </summary>
public static class DbTypeParser
{
    /// <summary>
    /// EN: Implements Parse.
    /// PT: Implementa Parse.
    /// </summary>
    public static object? Parse(this DbType dbType, string? value)
    {
        // 1️⃣ null, vazio ou "null" textual => null
        if (string.IsNullOrWhiteSpace(value) ||
            value!.Equals("null", StringComparison.OrdinalIgnoreCase))
            return null;

        // remove aspas simples comuns em SQL literals
        value = Unquote(value);

        return dbType switch
        {
            DbType.String
            or DbType.AnsiString
            or DbType.StringFixedLength
            or DbType.AnsiStringFixedLength => value,

            DbType.Byte => byte.Parse(value, CultureInfo.InvariantCulture),
            DbType.SByte => sbyte.Parse(value, CultureInfo.InvariantCulture),

            DbType.Int16 => short.Parse(value, CultureInfo.InvariantCulture),
            DbType.Int32 => int.Parse(value, CultureInfo.InvariantCulture),
            DbType.Int64 => long.Parse(value, CultureInfo.InvariantCulture),

            DbType.UInt16 => ushort.Parse(value, CultureInfo.InvariantCulture),
            DbType.UInt32 => uint.Parse(value, CultureInfo.InvariantCulture),
            DbType.UInt64 => ulong.Parse(value, CultureInfo.InvariantCulture),

            DbType.Decimal => decimal.Parse(value, CultureInfo.InvariantCulture),
            DbType.VarNumeric => decimal.Parse(value, CultureInfo.InvariantCulture),
            DbType.Double => double.Parse(value, CultureInfo.InvariantCulture),
            DbType.Single => float.Parse(value, CultureInfo.InvariantCulture),

            DbType.Boolean => ParseBool(value),

            DbType.Date => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal).Date,
            DbType.DateTime => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal),
            DbType.DateTime2 => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            DbType.DateTimeOffset
                                => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture),
            DbType.Time => TimeSpan.Parse(value, CultureInfo.InvariantCulture),

            DbType.Guid => Guid.Parse(value),

            DbType.Binary => ParseBinary(value),

            DbType.Currency => decimal.Parse(value, CultureInfo.InvariantCulture),
            DbType.Object => ParseObject(value),

            _ => throw new NotSupportedException($"DbType não suportado: {dbType}")
        };
    }

    /// <summary>
    /// EN: Parses binary literals from SQL-style hexadecimal or base64 payloads.
    /// PT: Faz o parsing de literais binários a partir de payloads hexadecimais estilo SQL ou base64.
    /// </summary>
    private static byte[] ParseBinary(string value)
    {
        if (TryParseHexBinary(value, out var hexBytes))
            return hexBytes;

        return Convert.FromBase64String(value);
    }

    /// <summary>
    /// EN: Parses DbType.Object with light inference for JSON, booleans and numeric literals.
    /// PT: Faz o parsing de DbType.Object com inferência leve para literais JSON, booleanos e numéricos.
    /// </summary>
    private static object ParseObject(string value)
    {
        if (LooksLikeJson(value))
            return System.Text.Json.JsonDocument.Parse(value);

        if (TryParseBool(value, out var boolValue))
            return boolValue;

        if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var numericValue))
            return numericValue;

        if (Guid.TryParse(value, out var guidValue))
            return guidValue;

        if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
            return dateTimeOffsetValue;

        return value;
    }

    private static bool ParseBool(string value)
        => TryParseBool(value, out var parsed)
            ? parsed
            : bool.Parse(value);

    /// <summary>
    /// EN: Attempts to parse textual SQL-like boolean aliases.
    /// PT: Tenta fazer o parsing de aliases booleanos textuais estilo SQL.
    /// </summary>
    private static bool TryParseBool(string value, out bool parsed)
    {
        value = value.Trim();
        switch (value)
        {
            case "1":
            case "true":
            case "TRUE":
            case "yes":
            case "YES":
            case "y":
            case "Y":
            case "on":
            case "ON":
                parsed = true;
                return true;
            case "0":
            case "false":
            case "FALSE":
            case "no":
            case "NO":
            case "n":
            case "N":
            case "off":
            case "OFF":
                parsed = false;
                return true;
            default:
                parsed = false;
                return false;
        }
    }

    private static string Unquote(string value)
    {
        if (value.Length >= 2 &&
            ((value.StartsWith("'") && value.EndsWith("'")) ||
             (value.StartsWith("\"") && value.EndsWith("\""))))
        {
            return value[1..^1];
        }
        return value;
    }

    private static bool LooksLikeJson(string value)
    {
        var trimmed = value.Trim();
        return (trimmed.StartsWith("{", StringComparison.Ordinal) && trimmed.EndsWith("}", StringComparison.Ordinal))
            || (trimmed.StartsWith("[", StringComparison.Ordinal) && trimmed.EndsWith("]", StringComparison.Ordinal));
    }

    private static bool TryParseHexBinary(string value, out byte[] bytes)
    {
        bytes = [];
        var trimmed = value.Trim();
        if (TryNormalizeHexPayload(trimmed, out var hex) == false)
            return false;
        if (hex.Length == 0 || hex.Length % 2 != 0)
            return false;

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!byte.TryParse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
                return false;

            buffer[i / 2] = part;
        }

        bytes = buffer;
        return true;
    }

    private static bool TryNormalizeHexPayload(string trimmed, out string hex)
    {
        hex = string.Empty;

        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = trimmed[2..];
            return true;
        }

        // SQL quoted hex forms: X'ABCD' / x'ABCD'
        if (trimmed.Length >= 3
            && (trimmed[0] == 'x' || trimmed[0] == 'X')
            && trimmed[1] == '\''
            && trimmed[^1] == '\'')
        {
            hex = trimmed[2..^1];
            return true;
        }

        return false;
    }
}

