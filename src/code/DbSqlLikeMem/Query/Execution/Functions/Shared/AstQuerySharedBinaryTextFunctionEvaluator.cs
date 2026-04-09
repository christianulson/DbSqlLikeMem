using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySharedBinaryTextFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (string.Equals(fn.Name, "MD5", StringComparison.OrdinalIgnoreCase))
            return TryEvalMd5Function(fn, evalArg, out result);

        if (string.Equals(fn.Name, "CRYPT_HASH", StringComparison.OrdinalIgnoreCase))
            return TryEvalCryptHashFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "BASE64_ENCODE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "BASE64_DECODE", StringComparison.OrdinalIgnoreCase))
            return TryEvalBase64Functions(fn, evalArg, out result);

        if (string.Equals(fn.Name, "HEX", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "HEX_ENCODE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "UNHEX", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "HEX_DECODE", StringComparison.OrdinalIgnoreCase))
            return TryEvalHexFunctions(fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalMd5Function(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MD5", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var bytes = Encoding.UTF8.GetBytes(text);
        using var md5 = MD5.Create();
        var hash = ComputeHash(md5, bytes);
        result = ToHexString(hash);
        return true;
    }

    private static bool TryEvalCryptHashFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var algorithmName = fn.Args[1] is RawSqlExpr rawAlgorithm
            ? rawAlgorithm.Sql
            : Convert.ToString(evalArg(1), CultureInfo.InvariantCulture) ?? string.Empty;
        if (IsNullish(value) || string.IsNullOrWhiteSpace(algorithmName))
        {
            result = null;
            return true;
        }

        var hashAlgorithm = CreateHashAlgorithm(algorithmName);
        if (hashAlgorithm is null)
        {
            result = null;
            return true;
        }

        var bytes = value is byte[] buffer
            ? buffer
            : Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);

        using (hashAlgorithm)
        {
            result = ComputeHash(hashAlgorithm, bytes);
        }

        return true;
    }

    private static bool TryEvalHexFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var isEncode = string.Equals(fn.Name, "HEX", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "HEX_ENCODE", StringComparison.OrdinalIgnoreCase);
        if (isEncode)
        {
            if (value is byte[] bytes)
            {
                result = ToHexString(bytes);
                return true;
            }

            if (value is string text)
            {
                result = ToHexString(Encoding.UTF8.GetBytes(text));
                return true;
            }

            try
            {
                var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                result = number.ToString("X", CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        var payload = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (payload.Length == 0)
        {
            result = Array.Empty<byte>();
            return true;
        }

        if (string.Equals(fn.Name, "HEX_DECODE", StringComparison.OrdinalIgnoreCase)
            && payload.Length % 2 == 1)
        {
            result = null;
            return true;
        }

        if (payload.Length % 2 == 1)
            payload = "0" + payload;

        var output = new byte[payload.Length / 2];
        for (var i = 0; i < output.Length; i++)
        {
            var slice = payload.Substring(i * 2, 2);
            if (!byte.TryParse(slice, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var parsed))
            {
                result = null;
                return true;
            }

            output[i] = parsed;
        }

        result = output;
        return true;
    }

    private static bool TryEvalBase64Functions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "BASE64_ENCODE", StringComparison.OrdinalIgnoreCase))
        {
            var bytes = value is byte[] buffer
                ? buffer
                : Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
            result = Convert.ToBase64String(bytes);
            return true;
        }

        var payload = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (payload.Length == 0)
        {
            result = Array.Empty<byte>();
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

    private static string ToHexString(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));

        return sb.ToString();
    }

    private static HashAlgorithm? CreateHashAlgorithm(string algorithmName)
        => algorithmName.Trim().ToUpperInvariant() switch
        {
            "MD5" => MD5.Create(),
            "SHA1" => SHA1.Create(),
            "SHA256" => SHA256.Create(),
            "SHA512" => SHA512.Create(),
            _ => null
        };
}
