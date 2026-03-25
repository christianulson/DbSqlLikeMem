namespace DbSqlLikeMem;

internal delegate bool TryConvertNumericToInt64Delegate(object value, out long numeric);

internal static class QueryMySqlUtilityFunctionHelper
{
    private static readonly HashSet<string> _networkFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "INET_ATON",
        "INET_NTOA",
        "INET6_ATON",
        "INET6_NTOA"
    };

    private static readonly HashSet<string> _ipValidationFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "IS_IPV4",
        "IS_IPV4_COMPAT",
        "IS_IPV4_MAPPED",
        "IS_IPV6"
    };

    private static readonly HashSet<string> _uuidFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "UUID_TO_BIN",
        "BIN_TO_UUID"
    };

    private delegate bool MySqlUtilityFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result);

    private static readonly IReadOnlyDictionary<string, MySqlUtilityFunctionHandler> _handlers =
        CreateHandlers();

    public static bool TryEvalUtilityFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, evalArg, tryConvertNumericToInt64, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, MySqlUtilityFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, MySqlUtilityFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalMySqlBase64Functions, "FROM_BASE64", "TO_BASE64");
        Register(handlers, TryEvalMySqlStringCompareFunction, "STRCMP");
        Register(handlers, TryEvalMySqlChecksumFunction, "CRC32");
        Register(handlers, TryEvalInetAtonFunction, "INET_ATON");
        Register(handlers, TryEvalInetNtoAFunction, "INET_NTOA");
        Register(handlers, TryEvalInet6AtonFunction, "INET6_ATON");
        Register(handlers, TryEvalInet6NtoAFunction, "INET6_NTOA");
        Register(handlers, TryEvalIsIpv4Function, "IS_IPV4");
        Register(handlers, TryEvalIsIpv4CompatFunction, "IS_IPV4_COMPAT");
        Register(handlers, TryEvalIsIpv4MappedFunction, "IS_IPV4_MAPPED");
        Register(handlers, TryEvalIsIpv6Function, "IS_IPV6");
        Register(handlers, TryEvalUuidToBinFunction, "UUID_TO_BIN");
        Register(handlers, TryEvalBinToUuidFunction, "BIN_TO_UUID");
        return handlers;
    }

    private static void Register(
        IDictionary<string, MySqlUtilityFunctionHandler> handlers,
        MySqlUtilityFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static bool TryEvalMySqlBase64Functions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = tryConvertNumericToInt64;
        if (!(fn.Name.Equals("FROM_BASE64", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("TO_BASE64", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (context.Dialect.Version < 56)
            throw SqlUnsupported.ForDialect(context.Dialect, fn.Name.ToUpperInvariant());

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
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = tryConvertNumericToInt64;
        if (!fn.Name.Equals("STRCMP", StringComparison.OrdinalIgnoreCase))
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
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = tryConvertNumericToInt64;
        if (!fn.Name.Equals("CRC32", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalInetAtonFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = tryConvertNumericToInt64;
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

    private static bool TryEvalInetNtoAFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
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

    private static bool TryEvalInet6AtonFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        if (context.Dialect.Version < 56 || context.Dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(context.Dialect, "INET6_ATON");

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

    private static bool TryEvalInet6NtoAFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        if (context.Dialect.Version < 56 || context.Dialect.Version >= 84)
            throw SqlUnsupported.ForDialect(context.Dialect, "INET6_NTOA");

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

    private static bool TryEvalIsIpv4Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = tryConvertNumericToInt64;
        return TryEvalIpVersionFunction(evalArg, expectedV4: true, expectedV6: false, out result);
    }

    private static bool TryEvalIsIpv4CompatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = tryConvertNumericToInt64;
        return TryEvalIpv4CompatOrMapped(evalArg, compat: true, out result);
    }

    private static bool TryEvalIsIpv4MappedFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = tryConvertNumericToInt64;
        return TryEvalIpv4CompatOrMapped(evalArg, compat: false, out result);
    }

    private static bool TryEvalIsIpv6Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = tryConvertNumericToInt64;
        return TryEvalIpVersionFunction(evalArg, expectedV4: false, expectedV6: true, out result);
    }

    private static bool TryEvalUuidToBinFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = context;
        if (fn.Args.Count == 0)
            throw new InvalidOperationException("UUID_TO_BIN() espera ao menos um argumento.");

        var swapFlag = false;
        if (fn.Args.Count > 1)
        {
            var flagValue = evalArg(1);
            if (!IsNullish(flagValue) && tryConvertNumericToInt64(flagValue!, out var numericFlag))
                swapFlag = numericFlag != 0;
        }

        if (context.Dialect.Version < 80)
            throw SqlUnsupported.ForDialect(context.Dialect, "UUID_TO_BIN");

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

    private static bool TryEvalBinToUuidFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        out object? result)
    {
        _ = fn;
        _ = tryConvertNumericToInt64;
        if (context.Dialect.Version < 80)
            throw SqlUnsupported.ForDialect(context.Dialect, "BIN_TO_UUID");

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

        var normalized = fn.Args.Count > 1 && !IsNullish(evalArg(1)) && tryConvertNumericToInt64(evalArg(1)!, out var numericFlag) && numericFlag != 0
            ? ApplyMySqlUuidUnswap(binBytes)
            : [.. binBytes];
        result = FormatUuid(normalized);
        return true;
    }

    private static bool TryEvalIpVersionFunction(
        Func<int, object?> evalArg,
        bool expectedV4,
        bool expectedV6,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        result = expectedV4
            ? (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 1 : 0)
            : (expectedV6 && ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? 1 : 0);
        return true;
    }

    private static bool TryEvalIpv4CompatOrMapped(
        Func<int, object?> evalArg,
        bool compat,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        if (ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            result = 0;
            return true;
        }

        var bytes = ip.GetAddressBytes();
        if (bytes.Length != 16)
        {
            result = 0;
            return true;
        }

        var isV4Mapped = bytes.Take(10).All(static b => b == 0) && bytes[10] == 0xff && bytes[11] == 0xff;
        result = compat
            ? (!isV4Mapped && bytes.Take(12).All(static b => b == 0) ? 1 : 0)
            : (isV4Mapped ? 1 : 0);
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
