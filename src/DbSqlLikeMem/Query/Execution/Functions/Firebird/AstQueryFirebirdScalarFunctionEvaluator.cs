namespace DbSqlLikeMem;

internal static class AstQueryFirebirdScalarFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(context.Dialect.Name, "firebird", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (string.Equals(fn.Name, "ASCII_CHAR", StringComparison.OrdinalIgnoreCase))
            return TryEvalAsciiCharFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ASCII_VAL", StringComparison.OrdinalIgnoreCase))
            return TryEvalAsciiValFunction(evalArg, out result);

        if (string.Equals(fn.Name, "UNICODE_CHAR", StringComparison.OrdinalIgnoreCase))
            return TryEvalUnicodeCharFunction(evalArg, out result);

        if (string.Equals(fn.Name, "UNICODE_VAL", StringComparison.OrdinalIgnoreCase))
            return TryEvalUnicodeValFunction(evalArg, out result);

        if (string.Equals(fn.Name, "CHAR_TO_UUID", StringComparison.OrdinalIgnoreCase))
            return TryEvalCharToUuidFunction(evalArg, out result);

        if (string.Equals(fn.Name, "UUID_TO_CHAR", StringComparison.OrdinalIgnoreCase))
            return TryEvalUuidToCharFunction(evalArg, out result);

        if (string.Equals(fn.Name, "GEN_UUID", StringComparison.OrdinalIgnoreCase))
            return TryEvalGenUuidFunction(fn, out result);

        if (string.Equals(fn.Name, "HASH", StringComparison.OrdinalIgnoreCase))
            return TryEvalHashFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "TRUNC", StringComparison.OrdinalIgnoreCase))
            return TryEvalTruncFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "BIN_AND", StringComparison.OrdinalIgnoreCase))
            return TryEvalBinaryBitwiseFunction(fn, evalArg, BinaryBitwiseOperation.And, out result);

        if (string.Equals(fn.Name, "BIN_OR", StringComparison.OrdinalIgnoreCase))
            return TryEvalBinaryBitwiseFunction(fn, evalArg, BinaryBitwiseOperation.Or, out result);

        if (string.Equals(fn.Name, "BIN_XOR", StringComparison.OrdinalIgnoreCase))
            return TryEvalBinaryBitwiseFunction(fn, evalArg, BinaryBitwiseOperation.Xor, out result);

        if (string.Equals(fn.Name, "BIN_NOT", StringComparison.OrdinalIgnoreCase))
            return TryEvalBinaryNotFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "BIN_SHL", StringComparison.OrdinalIgnoreCase))
            return TryEvalShiftFunction(fn, evalArg, isLeftShift: true, out result);

        if (string.Equals(fn.Name, "BIN_SHR", StringComparison.OrdinalIgnoreCase))
            return TryEvalShiftFunction(fn, evalArg, isLeftShift: false, out result);

        if (string.Equals(fn.Name, "MAXVALUE", StringComparison.OrdinalIgnoreCase))
            return TryEvalMinMaxFunction(context, fn, evalArg, isGreatest: true, out result);

        if (string.Equals(fn.Name, "MINVALUE", StringComparison.OrdinalIgnoreCase))
            return TryEvalMinMaxFunction(context, fn, evalArg, isGreatest: false, out result);

        result = null;
        return false;
    }

    private static bool TryEvalMinMaxFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool isGreatest,
        out object? result)
    {
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        object? current = null;
        foreach (var index in Enumerable.Range(0, fn.Args.Count))
        {
            var value = evalArg(index);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (current is null)
            {
                current = value;
                continue;
            }

            var comparison = context.Compare(current, value!);
            if (isGreatest && comparison < 0)
                current = value;
            else if (!isGreatest && comparison > 0)
                current = value;
        }

        result = current;
        return true;
    }

    private static bool TryEvalHashFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var bytes = value is byte[] buffer
            ? buffer
            : Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);

        var algorithmName = fn.Args.Count > 1
            ? Convert.ToString(evalArg(1), CultureInfo.InvariantCulture) ?? string.Empty
            : string.Empty;
        if (!string.IsNullOrWhiteSpace(algorithmName))
        {
            if (!algorithmName.Equals("CRC32", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return true;
            }

            result = unchecked((int)ComputeCrc32(bytes));
            return true;
        }

        result = unchecked((long)ComputeHash64(bytes));
        return true;
    }

    private static bool TryEvalTruncFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!decimal.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Number, CultureInfo.InvariantCulture, out var number))
        {
            result = null;
            return true;
        }

        var scale = fn.Args.Count > 1 && !AstQueryExecutorBase.IsNullish(evalArg(1))
            ? Convert.ToInt32(evalArg(1), CultureInfo.InvariantCulture)
            : 0;

        var factor = Pow10(Math.Abs(scale));
        result = scale == 0
            ? Math.Truncate(number)
            : scale > 0
                ? Math.Truncate(number * factor) / factor
                : Math.Truncate(number / factor) * factor;
        return true;
    }

    private static decimal Pow10(int digits)
    {
        var factor = 1m;
        for (var i = 0; i < digits; i++)
            factor *= 10m;

        return factor;
    }

    private static bool TryEvalAsciiCharFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var codePoint = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            if (codePoint < 0 || codePoint > 255)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(codePoint);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static ulong ComputeHash64(byte[] bytes)
    {
        const ulong highBitsMask = 0xF000000000000000UL;

        var hash = 0UL;
        foreach (var b in bytes)
        {
            hash = (hash << 4) + b;
            var high = hash & highBitsMask;
            if (high != 0)
            {
                hash ^= high >> 56;
                hash &= ~high;
            }
        }

        return hash;
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

    private static bool TryEvalAsciiValFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        result = text.Length == 0 ? 0 : (int)text[0];
        return true;
    }

    private static bool TryEvalUnicodeCharFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var codePoint = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            if (codePoint < 0 || codePoint > char.MaxValue)
            {
                result = null;
                return true;
            }

            result = char.ConvertFromUtf32(codePoint);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalUnicodeValFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (text.Length == 0)
        {
            result = null;
            return true;
        }

        result = char.IsSurrogatePair(text, 0)
            ? char.ConvertToUtf32(text, 0)
            : (int)text[0];
        return true;
    }

    private static bool TryEvalCharToUuidFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        var hex = new string(text.Where(char.IsLetterOrDigit).ToArray());
        if (hex.Length != 32)
        {
            result = null;
            return true;
        }

        try
        {
            result = FromHexString(hex);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalUuidToCharFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var bytes = value switch
        {
            byte[] buffer => buffer,
            string text => TryParseUuidBytes(text, out var parsed) ? parsed : null,
            _ => TryParseUuidBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty, out var fallbackParsed) ? fallbackParsed : null
        };

        if (bytes is null || bytes.Length == 0)
        {
            result = null;
            return true;
        }

        var hex = ToHexString(bytes);
        if (hex.Length < 32)
        {
            result = null;
            return true;
        }

        result = $"{hex[..8]}-{hex.Substring(8, 4)}-{hex.Substring(12, 4)}-{hex.Substring(16, 4)}-{hex[20..32]}";
        return true;
    }

    private static bool TryEvalGenUuidFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (fn.Args.Count != 0)
        {
            result = null;
            return true;
        }

        result = Guid.NewGuid().ToByteArray();
        return true;
    }

    private static bool TryParseUuidBytes(string text, out byte[] bytes)
    {
        bytes = [];

        var hex = new string(text.Where(char.IsLetterOrDigit).ToArray());
        if (hex.Length == 32)
        {
            try
            {
                bytes = FromHexString(hex);
                return true;
            }
            catch
            {
                bytes = [];
                return false;
            }
        }

        return false;
    }

    private static string ToHexString(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));

        return sb.ToString();
    }

    private static byte[] FromHexString(string hex)
    {
        if (hex.Length % 2 == 1)
            hex = "0" + hex;

        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            var slice = hex.Substring(i * 2, 2);
            bytes[i] = byte.Parse(slice, NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return bytes;
    }

    private enum BinaryBitwiseOperation
    {
        And,
        Or,
        Xor
    }

    private static bool TryEvalBinaryBitwiseFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        BinaryBitwiseOperation operation,
        out object? result)
    {
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var firstValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(firstValue))
        {
            result = null;
            return true;
        }

        var current = Convert.ToInt64(firstValue, CultureInfo.InvariantCulture);
        for (var i = 1; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var operand = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            current = operation switch
            {
                BinaryBitwiseOperation.And => current & operand,
                BinaryBitwiseOperation.Or => current | operand,
                BinaryBitwiseOperation.Xor => current ^ operand,
                _ => current
            };
        }

        result = current;
        return true;
    }

    private static bool TryEvalShiftFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool isLeftShift,
        out object? result)
    {
        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        var shiftValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(shiftValue))
        {
            result = null;
            return true;
        }

        var current = Convert.ToInt64(value, CultureInfo.InvariantCulture);
        var shift = Convert.ToInt32(shiftValue, CultureInfo.InvariantCulture);
        result = isLeftShift ? current << shift : current >> shift;
        return true;
    }

    private static bool TryEvalBinaryNotFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var current = Convert.ToInt64(value, CultureInfo.InvariantCulture);
        result = ~current;
        return true;
    }
}
