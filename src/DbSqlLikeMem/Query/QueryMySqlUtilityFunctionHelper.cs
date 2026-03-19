namespace DbSqlLikeMem;

internal delegate bool TryConvertNumericToInt64Delegate(object value, out long numeric);

internal static class QueryMySqlUtilityFunctionHelper
{
    public static bool TryEvalUtilityFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        return TryEvalMySqlBase64Functions(fn, dialect, evalArg, out result)
            || TryEvalMySqlStringCompareFunction(fn, dialect, evalArg, out result)
            || TryEvalMySqlChecksumFunction(fn, dialect, evalArg, out result)
            || TryEvalMySqlNetworkFunctions(fn, dialect, evalArg, out result)
            || TryEvalMySqlUuidFunctions(fn, dialect, evalArg, tryConvertNumericToInt64, out result);
    }

    private static bool TryEvalMySqlBase64Functions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("FROM_BASE64", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TO_BASE64", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (dialect.Version < 56)
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (fn.Name.Equals("TO_BASE64", StringComparison.OrdinalIgnoreCase))
        {
            var bytes = value as byte[]
                ?? Encoding.UTF8.GetBytes(value!.ToString() ?? string.Empty);
            result = Convert.ToBase64String(bytes);
            return true;
        }

        var payload = value!.ToString();
        if (string.IsNullOrWhiteSpace(payload))
        {
            result = null;
            return true;
        }

        try
        {
            result = Convert.FromBase64String(payload);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMySqlStringCompareFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRCMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STRCMP() espera dois argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        var comparison = string.Compare(
            Convert.ToString(left, CultureInfo.InvariantCulture),
            Convert.ToString(right, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);

        result = comparison < 0 ? -1 : comparison > 0 ? 1 : 0;
        return true;
    }

    private static bool TryEvalMySqlChecksumFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("CRC32", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("CRC32() espera um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var bytes = value as byte[]
            ?? Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
        result = (long)ComputeCrc32(bytes);
        return true;
    }

    private static bool TryEvalMySqlNetworkFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("INET_ATON" or "INET_NTOA" or "INET6_ATON" or "INET6_NTOA"))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        if (name is "INET_ATON")
        {
            var textValue = evalArg(0);
            if (IsNullish(textValue))
            {
                result = null;
                return true;
            }

            var text = Convert.ToString(textValue, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!IPAddress.TryParse(text, out var address) || address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork)
            {
                result = null;
                return true;
            }

            var bytes = address.GetAddressBytes();
            var numeric = ((uint)bytes[0] << 24) | ((uint)bytes[1] << 16) | ((uint)bytes[2] << 8) | bytes[3];
            result = (long)numeric;
            return true;
        }

        if (name is "INET_NTOA")
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryConvertNumericToUInt64(value!, out var numeric) || numeric > uint.MaxValue)
            {
                result = null;
                return true;
            }

            var bytes = new[]
            {
                (byte)((numeric >> 24) & 0xFF),
                (byte)((numeric >> 16) & 0xFF),
                (byte)((numeric >> 8) & 0xFF),
                (byte)(numeric & 0xFF)
            };
            result = new IPAddress(bytes).ToString();
            return true;
        }

        if (name is "INET6_ATON")
        {
            if (dialect.Version < 56 || dialect.Version >= 84)
                throw SqlUnsupported.ForDialect(dialect, "INET6_ATON");

            var textValue = evalArg(0);
            if (IsNullish(textValue))
            {
                result = null;
                return true;
            }

            var text = Convert.ToString(textValue, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!IPAddress.TryParse(text, out var address))
            {
                result = null;
                return true;
            }

            result = address.GetAddressBytes();
            return true;
        }

        if (dialect.Version < 56 || dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(dialect, "INET6_NTOA");

        var value6 = evalArg(0);
        if (IsNullish(value6))
        {
            result = null;
            return true;
        }

        if (value6 is not byte[] bytes6 || (bytes6.Length != 4 && bytes6.Length != 16))
        {
            result = null;
            return true;
        }

        result = new IPAddress(bytes6).ToString();
        return true;
    }

    private static bool TryEvalMySqlUuidFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("UUID_TO_BIN" or "BIN_TO_UUID"))
        {
            result = null;
            return false;
        }

        if (!MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{name}() espera ao menos um argumento.");

        var swapFlag = false;
        if (fn.Args.Count > 1)
        {
            var flagValue = evalArg(1);
            if (!IsNullish(flagValue) && tryConvertNumericToInt64(flagValue!, out var numericFlag))
                swapFlag = numericFlag != 0;
        }

        if (name == "UUID_TO_BIN")
        {
            if (dialect.Version < 80)
                throw SqlUnsupported.ForDialect(dialect, "UUID_TO_BIN");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is byte[] byteValue)
            {
                if (byteValue.Length != 16)
                {
                    result = null;
                    return true;
                }

                result = swapFlag ? ApplyMySqlUuidSwap(byteValue) : [.. byteValue];
                return true;
            }

            var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
            if (!TryParseUuidHex(text, out var bytes))
            {
                result = null;
                return true;
            }

            result = swapFlag ? ApplyMySqlUuidSwap(bytes) : bytes;
            return true;
        }

        if (dialect.Version < 80)
            throw SqlUnsupported.ForDialect(dialect, "BIN_TO_UUID");

        var binValue = evalArg(0);
        if (IsNullish(binValue))
        {
            result = null;
            return true;
        }

        if (binValue is not byte[] binBytes || binBytes.Length != 16)
        {
            result = null;
            return true;
        }

        var normalized = swapFlag ? ApplyMySqlUuidUnswap(binBytes) : [.. binBytes];
        result = FormatUuid(normalized);
        return true;
    }

    private static bool TryConvertNumericToUInt64(object value, out ulong numeric)
    {
        numeric = 0;
        switch (value)
        {
            case byte b:
                numeric = b;
                return true;
            case sbyte sb:
                if (sb < 0) return false;
                numeric = (ulong)sb;
                return true;
            case short s:
                if (s < 0) return false;
                numeric = (ulong)s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i:
                if (i < 0) return false;
                numeric = (ulong)i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l:
                if (l < 0) return false;
                numeric = (ulong)l;
                return true;
            case ulong ul:
                numeric = ul;
                return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture);
        return ulong.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    private static uint ComputeCrc32(byte[] bytes)
    {
        var table = Crc32Table.Value;
        var crc = uint.MaxValue;
        foreach (var b in bytes)
        {
            var index = (crc ^ b) & 0xFF;
            crc = (crc >> 8) ^ table[index];
        }

        return crc ^ uint.MaxValue;
    }

    private static readonly Lazy<uint[]> Crc32Table = new(static () =>
    {
        var table = new uint[256];
        for (var i = 0; i < table.Length; i++)
        {
            var crc = (uint)i;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0
                    ? (crc >> 1) ^ 0xEDB88320u
                    : crc >> 1;
            }

            table[i] = crc;
        }

        return table;
    });

    private static bool TryParseUuidHex(string text, out byte[] bytes)
    {
        bytes = [];
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var normalized = text.Trim().Trim('{', '}').Replace("-", string.Empty);
        if (normalized.Length != 32)
            return false;

        bytes = new byte[16];
        for (var i = 0; i < bytes.Length; i++)
        {
            if (!TryParseHexByte(normalized, i * 2, out bytes[i]))
                return false;
        }

        return true;
    }

    private static bool TryParseHexByte(string text, int index, out byte value)
    {
        value = 0;
        if (!TryGetHexNibble(text[index], out var high)
            || !TryGetHexNibble(text[index + 1], out var low))
        {
            return false;
        }

        value = (byte)((high << 4) | low);
        return true;
    }

    private static bool TryGetHexNibble(char ch, out int nibble)
    {
        if (ch is >= '0' and <= '9')
        {
            nibble = ch - '0';
            return true;
        }

        if (ch is >= 'a' and <= 'f')
        {
            nibble = ch - 'a' + 10;
            return true;
        }

        if (ch is >= 'A' and <= 'F')
        {
            nibble = ch - 'A' + 10;
            return true;
        }

        nibble = 0;
        return false;
    }

    private static byte[] ApplyMySqlUuidSwap(byte[] bytes)
    {
        var swapped = new byte[16];
        var map = new[] { 6, 7, 4, 5, 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15 };
        for (var i = 0; i < swapped.Length; i++)
            swapped[i] = bytes[map[i]];
        return swapped;
    }

    private static byte[] ApplyMySqlUuidUnswap(byte[] bytes)
    {
        var swapped = new byte[16];
        var map = new[] { 4, 5, 6, 7, 2, 3, 0, 1, 8, 9, 10, 11, 12, 13, 14, 15 };
        for (var i = 0; i < swapped.Length; i++)
            swapped[i] = bytes[map[i]];
        return swapped;
    }

    private static string FormatUuid(byte[] bytes)
        => $"{bytes[0]:x2}{bytes[1]:x2}{bytes[2]:x2}{bytes[3]:x2}-{bytes[4]:x2}{bytes[5]:x2}-{bytes[6]:x2}{bytes[7]:x2}-{bytes[8]:x2}{bytes[9]:x2}-{bytes[10]:x2}{bytes[11]:x2}{bytes[12]:x2}{bytes[13]:x2}{bytes[14]:x2}{bytes[15]:x2}";

    private static bool IsNullish(object? value) => value is null or DBNull;
}
