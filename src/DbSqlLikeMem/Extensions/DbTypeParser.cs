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

            DbType.Binary => Convert.FromBase64String(value),

            DbType.Currency => decimal.Parse(value, CultureInfo.InvariantCulture),

            _ => throw new NotSupportedException($"DbType não suportado: {dbType}")
        };
    }

    private static bool ParseBool(string value)
    {
        value = value.Trim();
        return value switch
        {
            "1" or "true" or "TRUE" => true,
            "0" or "false" or "FALSE" => false,
            "yes" or "YES" or "y" or "Y" or "on" or "ON" => true,
            "no" or "NO" or "n" or "N" or "off" or "OFF" => false,
            _ => bool.Parse(value)
        };
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
}

