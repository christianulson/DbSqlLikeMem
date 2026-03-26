using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryMySqlUtilityFunctionEvaluator
{
    private delegate bool MySqlUtilityFunctionHandler(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result);

    private static readonly Dictionary<string, MySqlUtilityFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        result = null;

        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, row, evalArg, tryConvertNumericToInt64, tryConvertNumericToDouble, out result);

        return false;
    }

    private static Dictionary<string, MySqlUtilityFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, MySqlUtilityFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalSetFunctions, "ELT", "MAKE_SET", "EXPORT_SET");
        Register(handlers, TryEvalHexFunctions, "HEX", "UNHEX");
        Register(handlers, TryEvalFormatFunction, "FORMAT");
        Register(handlers, TryEvalRandomBytesFunction, "RANDOM_BYTES");
        Register(handlers, TryEvalSleepFunction, "SLEEP");
        Register(handlers, TryEvalLastInsertIdFunction, "LAST_INSERT_ID");
        Register(handlers, TryEvalCompressFunctions, "COMPRESS", "UNCOMPRESS", "UNCOMPRESSED_LENGTH");
        Register(handlers, TryEvalFormatBytesFunction, "FORMAT_BYTES");
        Register(handlers, TryEvalFormatPicoTimeFunction, "FORMAT_PICO_TIME");
        Register(handlers, TryEvalXmlFunctions, "EXTRACTVALUE", "UPDATEXML");
        Register(handlers, TryEvalCryptoFunctions, "AES_ENCRYPT", "AES_DECRYPT", "DES_ENCRYPT", "DES_DECRYPT", "ENCODE", "DECODE", "ENCRYPT");
        Register(handlers, TryEvalDefaultFunction, SqlConst.DEFAULT);
        Register(handlers, TryEvalMemberOfFunction, "MEMBER_OF");
        Register(handlers, TryEvalIsIpv4Function, "IS_IPV4");
        Register(handlers, TryEvalIsIpv4CompatFunction, "IS_IPV4_COMPAT");
        Register(handlers, TryEvalIsIpv4MappedFunction, "IS_IPV4_MAPPED");
        Register(handlers, TryEvalIsIpv6Function, "IS_IPV6");
        Register(handlers, TryEvalRegexFunctions, "REGEXP_INSTR", "REGEXP_REPLACE", "REGEXP_SUBSTR", "REGEXP_LIKE");

        return handlers;
    }

    private static void Register(
        Dictionary<string, MySqlUtilityFunctionHandler> handlers,
        MySqlUtilityFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers.Add(name, handler);
    }

    private static bool TryEvalSetFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (string.Equals(fn.Name, "ELT", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ELT() espera indice e valores.");

            var indexValue = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(indexValue))
            {
                result = null;
                return true;
            }

            if (!tryConvertNumericToInt64(indexValue!, out var index) || index <= 0)
            {
                result = null;
                return true;
            }

            var position = (int)index;
            if (position >= fn.Args.Count)
            {
                result = null;
                return true;
            }

            var value = evalArg(position);
            result = AstQueryExecutorBase.IsNullish(value) ? null : value;
            return true;
        }

        if (string.Equals(fn.Name, "MAKE_SET", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("MAKE_SET() espera bits e valores.");

            var bitsValue = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(bitsValue) || !tryConvertNumericToInt64(bitsValue!, out var bits))
            {
                result = null;
                return true;
            }

            var selected = new List<string>();
            for (var i = 1; i < fn.Args.Count; i++)
            {
                if ((bits & (1L << (i - 1))) == 0)
                    continue;

                var value = evalArg(i);
                if (AstQueryExecutorBase.IsNullish(value))
                    continue;

                selected.Add(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
            }

            result = selected.Count == 0 ? null : string.Join(",", selected);
            return true;
        }

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("EXPORT_SET() espera bits, on, off.");

        var bitsExportValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(bitsExportValue) || !tryConvertNumericToInt64(bitsExportValue!, out var bitsExport))
        {
            result = null;
            return true;
        }

        var onValue = evalArg(1);
        var offValue = evalArg(2);
        var separatorValue = fn.Args.Count > 3 ? evalArg(3) : ",";
        var limitValue = fn.Args.Count > 4 ? evalArg(4) : null;

        var onText = Convert.ToString(onValue, CultureInfo.InvariantCulture) ?? string.Empty;
        var offText = Convert.ToString(offValue, CultureInfo.InvariantCulture) ?? string.Empty;
        var separator = Convert.ToString(separatorValue, CultureInfo.InvariantCulture) ?? ",";
        var limit = 64;
        if (!AstQueryExecutorBase.IsNullish(limitValue) && tryConvertNumericToInt64(limitValue!, out var limitParsed) && limitParsed > 0)
            limit = (int)limitParsed;

        var pieces = new string[limit];
        for (var i = 0; i < limit; i++)
            pieces[i] = (bitsExport & (1L << i)) != 0 ? onText : offText;

        result = string.Join(separator, pieces);
        return true;
    }

    private static bool TryEvalHexFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "HEX", StringComparison.OrdinalIgnoreCase))
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

    private static bool TryEvalFormatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FORMAT() espera valor e casas decimais.");

        var value = evalArg(0);
        var decimalsValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(decimalsValue))
        {
            result = null;
            return true;
        }

        var locale = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var cultureName = string.IsNullOrWhiteSpace(locale) ? string.Empty : locale!.Replace('_', '-');
        var culture = string.IsNullOrWhiteSpace(cultureName)
            ? CultureInfo.InvariantCulture
            : CultureInfo.GetCultureInfo(cultureName);

        if (!tryConvertNumericToInt64(decimalsValue!, out var decimalsParsed))
        {
            result = null;
            return true;
        }

        var decimals = (int)Math.Max(0, decimalsParsed);
        try
        {
            var numeric = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            result = numeric.ToString("N" + decimals, culture);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalRandomBytesFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("RANDOM_BYTES() espera o tamanho em bytes.");

        var lengthValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        if (!tryConvertNumericToInt64(lengthValue!, out var length) || length < 0 || length > int.MaxValue)
        {
            result = null;
            return true;
        }

        if (length == 0)
        {
            result = Array.Empty<byte>();
            return true;
        }

        var buffer = new byte[length];
        using (var rng = RandomNumberGenerator.Create())
            rng.GetBytes(buffer);
        result = buffer;
        return true;
    }

    private static bool TryEvalSleepFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException("SLEEP() espera o tempo em segundos.");

        var secondsValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(secondsValue))
        {
            result = null;
            return true;
        }

        if (!tryConvertNumericToDouble(secondsValue!, out var seconds) || seconds < 0d)
        {
            result = null;
            return true;
        }

        // Avoid real delays; return the same success code as a completed sleep.
        result = 0;
        return true;
    }

    private static bool TryEvalCompressFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera ao menos um argumento.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "COMPRESS", StringComparison.OrdinalIgnoreCase))
        {
            var text = value?.ToString() ?? string.Empty;
            var input = value is byte[] bytes
                ? bytes
                : Encoding.UTF8.GetBytes(text);

            using var output = new MemoryStream();
            using (var deflate = new DeflateStream(output, CompressionLevel.Optimal, leaveOpen: true))
                deflate.Write(input, 0, input.Length);

            result = output.ToArray();
            return true;
        }

        if (value is not byte[] compressed)
        {
            result = null;
            return true;
        }

        try
        {
            using var input = new MemoryStream(compressed);
            using var deflate = new DeflateStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            deflate.CopyTo(output);
            var decompressed = output.ToArray();

            result = string.Equals(fn.Name, "UNCOMPRESSED_LENGTH", StringComparison.OrdinalIgnoreCase)
                ? decompressed.LongLength
                : decompressed;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalFormatBytesFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT_BYTES() espera um valor numerico.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!tryConvertNumericToDouble(value!, out var bytes) || bytes < 0d)
        {
            result = null;
            return true;
        }

        if (bytes < 1024d)
        {
            result = $"{Math.Truncate(bytes)} bytes";
            return true;
        }

        var units = new[] { "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
        var unitIndex = 0;
        var scaled = bytes / 1024d;
        while (scaled >= 1024d && unitIndex < units.Length - 1)
        {
            scaled /= 1024d;
            unitIndex++;
        }

        result = $"{scaled.ToString("0.00", CultureInfo.InvariantCulture)} {units[unitIndex]}";
        return true;
    }

    private static bool TryEvalFormatPicoTimeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMAT_PICO_TIME() espera um valor numerico.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (!tryConvertNumericToDouble(value!, out var pico) || pico < 0d)
        {
            result = null;
            return true;
        }

        var units = new[]
        {
            ("ps", 1000d),
            ("ns", 1000d),
            ("us", 1000d),
            ("ms", 1000d),
            ("s", 60d),
            ("min", 60d),
            ("h", 24d),
            ("d", double.PositiveInfinity)
        };

        var scaled = pico;
        var unit = "ps";
        foreach (var (candidate, factor) in units)
        {
            unit = candidate;
            if (scaled < factor)
                break;
            if (double.IsInfinity(factor))
                break;
            scaled /= factor;
        }

        result = string.Format(CultureInfo.InvariantCulture, "{0:0.00} {1}", scaled, unit);
        return true;
    }

    private static bool TryEvalXmlFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (string.Equals(fn.Name, "EXTRACTVALUE", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("EXTRACTVALUE() espera xml e xpath.");

            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "UPDATEXML", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("UPDATEXML() espera xml, xpath e novo xml.");

            result = null;
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalLastInsertIdFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        _ = row;
        _ = tryConvertNumericToInt64;
        _ = tryConvertNumericToDouble;

        if (!string.Equals(fn.Name, "LAST_INSERT_ID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count > 0)
        {
            var value = evalArg(0);
            context.Connection.SetLastInsertId(value);
            result = value;
            return true;
        }

        result = context.Connection.GetLastInsertId() ?? 0;
        return true;
    }

    private static bool TryEvalCryptoFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!string.Equals(fn.Name, "AES_ENCRYPT", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "AES_DECRYPT", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "DES_ENCRYPT", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "DES_DECRYPT", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "ENCODE", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "DECODE", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "ENCRYPT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!string.Equals(fn.Name, "AES_ENCRYPT", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(fn.Name, "AES_DECRYPT", StringComparison.OrdinalIgnoreCase)
            && !context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (string.Equals(fn.Name, "ENCRYPT", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("ENCRYPT() espera texto.");

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var saltValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() ?? string.Empty : string.Empty;
            var text = value?.ToString() ?? string.Empty;
            var payload = Encoding.UTF8.GetBytes(saltValue + text);
            using var sha = SHA256.Create();
            var hash = sha.ComputeHash(payload);
            var sb = new StringBuilder(hash.Length * 2);
            foreach (var b in hash)
                sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
            result = sb.ToString();
            return true;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera payload e chave.");

        var payloadValue = evalArg(0);
        var keyValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(payloadValue) || AstQueryExecutorBase.IsNullish(keyValue))
        {
            result = null;
            return true;
        }

        var keyText = keyValue?.ToString() ?? string.Empty;

        if (string.Equals(fn.Name, "AES_ENCRYPT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "AES_DECRYPT", StringComparison.OrdinalIgnoreCase))
        {
            var payload = TryGetBinaryPayload(payloadValue, out var payloadBytes)
                ? payloadBytes
                : Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);
            var key = BuildXorKeyBytes(Encoding.UTF8.GetBytes(keyText), 16);
            var output1 = new byte[payload.Length];
            if (key.Length > 0)
            {
                for (var i = 0; i < payload.Length; i++)
                    output1[i] = (byte)(payload[i] ^ key[i % key.Length]);
            }

            if (string.Equals(fn.Name, "AES_ENCRYPT", StringComparison.OrdinalIgnoreCase))
            {
                result = output1;
                return true;
            }

            result = Encoding.UTF8.GetString(output1);
            return true;
        }

        if (string.Equals(fn.Name, "DES_ENCRYPT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "DES_DECRYPT", StringComparison.OrdinalIgnoreCase))
        {
            var payload = TryGetBinaryPayload(payloadValue, out var payloadBytes)
                ? payloadBytes
                : Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);
            var key = BuildXorKeyBytes(Encoding.UTF8.GetBytes(keyText), 8);
            var des = DES.Create();
            if (des is null)
            {
                result = null;
                return true;
            }

            using (des)
            {
                des.Mode = CipherMode.ECB;
                des.Padding = PaddingMode.PKCS7;
                des.Key = key;
                des.IV = new byte[des.BlockSize / 8];

                if (string.Equals(fn.Name, "DES_ENCRYPT", StringComparison.OrdinalIgnoreCase))
                {
                    using var encryptor = des.CreateEncryptor();
                    result = encryptor.TransformFinalBlock(payload, 0, payload.Length);
                    return true;
                }

                try
                {
                    using var decryptor = des.CreateDecryptor();
                    var decrypted = decryptor.TransformFinalBlock(payload, 0, payload.Length);
                    result = Encoding.UTF8.GetString(decrypted);
                    return true;
                }
                catch
                {
                    result = null;
                    return true;
                }
            }
        }

        var keyBytes = Encoding.UTF8.GetBytes(keyText);
        if (keyBytes.Length == 0)
        {
            result = null;
            return true;
        }

        if (payloadValue is not byte[] inputBytes)
            inputBytes = Encoding.UTF8.GetBytes(payloadValue?.ToString() ?? string.Empty);

        var output = new byte[inputBytes.Length];
        for (var i = 0; i < inputBytes.Length; i++)
            output[i] = (byte)(inputBytes[i] ^ keyBytes[i % keyBytes.Length]);

        if (string.Equals(fn.Name, "DECODE", StringComparison.OrdinalIgnoreCase))
        {
            result = Encoding.UTF8.GetString(output);
            return true;
        }

        result = output;
        return true;
    }

    private static bool TryEvalDefaultFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (fn.Args.Count != 1)
            throw new InvalidOperationException("DEFAULT() espera um argumento.");

        var arg = fn.Args[0];
        string? qualifier = null;
        string? columnName = null;

        if (arg is ColumnExpr columnExpr)
        {
            qualifier = columnExpr.Qualifier;
            columnName = columnExpr.Name;
        }
        else if (arg is IdentifierExpr identifierExpr)
        {
            var name = identifierExpr.Name;
            var dot = name.IndexOf('.');
            if (dot > 0)
            {
                qualifier = name[..dot];
                columnName = name[(dot + 1)..];
            }
            else
            {
                columnName = name;
            }
        }
        else
        {
            var value = evalArg(0);
            result = AstQueryExecutorBase.IsNullish(value) ? null : value;
            return true;
        }

        if (string.IsNullOrWhiteSpace(columnName))
        {
            result = null;
            return true;
        }

        if (TryResolveDefaultValue(row, qualifier, columnName!, out var defaultValue))
        {
            result = defaultValue;
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryResolveDefaultValue(
        EvalRow row,
        string? qualifier,
        string columnName,
        out object? defaultValue)
    {
        defaultValue = null;
        if (row.Sources.Count == 0)
            return false;

        if (!string.IsNullOrWhiteSpace(qualifier))
        {
            if (!row.Sources.TryGetValue(qualifier!, out var source))
                return false;

            if (source.Physical is null)
                return false;

            if (!source.Physical.Columns.TryGetValue(columnName, out var column))
                return false;

            defaultValue = column.DefaultValue;
            return true;
        }

        foreach (var source in row.Sources.Values)
        {
            if (source.Physical is null)
                continue;

            if (source.Physical.Columns.TryGetValue(columnName, out var column))
            {
                defaultValue = column.DefaultValue;
                return true;
            }
        }

        return false;
    }

    private static bool TryEvalRegexFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var pattern = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(pattern))
        {
            result = null;
            return true;
        }

        var position = 1;
        if (fn.Args.Count >= 3 && !AstQueryExecutorBase.IsNullish(evalArg(2)))
            position = Math.Max(1, Convert.ToInt32(evalArg(2)!.ToDec()));

        var occurrence = 1;
        if (fn.Args.Count >= 4 && !AstQueryExecutorBase.IsNullish(evalArg(3)))
            occurrence = Math.Max(1, Convert.ToInt32(evalArg(3)!.ToDec()));

        var returnOption = 0;
        if (fn.Args.Count >= 5 && !AstQueryExecutorBase.IsNullish(evalArg(4)))
            returnOption = Convert.ToInt32(evalArg(4)!.ToDec());

        var matchType = fn.Args.Count >= 6 ? evalArg(5)?.ToString() ?? string.Empty : string.Empty;
        var options = RegexOptions.CultureInvariant;
        if (context.Dialect.RegexIsCaseInsensitive)
            options |= RegexOptions.IgnoreCase;

        if (!string.IsNullOrWhiteSpace(matchType))
        {
            if (matchType.IndexOf("c", StringComparison.OrdinalIgnoreCase) >= 0)
                options &= ~RegexOptions.IgnoreCase;
            if (matchType.IndexOf("i", StringComparison.OrdinalIgnoreCase) >= 0)
                options |= RegexOptions.IgnoreCase;
            if (matchType.IndexOf("m", StringComparison.OrdinalIgnoreCase) >= 0)
                options |= RegexOptions.Multiline;
            if (matchType.IndexOf("n", StringComparison.OrdinalIgnoreCase) >= 0)
                options |= RegexOptions.Singleline;
        }

        var startIndex = Math.Min(source.Length, Math.Max(0, position - 1));
        var scoped = source[startIndex..];

        try
        {
            if (string.Equals(fn.Name, "REGEXP_LIKE", StringComparison.OrdinalIgnoreCase))
            {
                result = Regex.IsMatch(scoped, pattern, options) ? 1 : 0;
                return true;
            }

            if (string.Equals(fn.Name, "REGEXP_REPLACE", StringComparison.OrdinalIgnoreCase))
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                if (fn.Args.Count >= 4 && !AstQueryExecutorBase.IsNullish(evalArg(3)) && occurrence > 0)
                {
                    var matches = Regex.Matches(scoped, pattern, options);
                    if (matches.Count == 0)
                    {
                        result = scoped;
                        return true;
                    }

                    var idx = Math.Min(occurrence - 1, matches.Count - 1);
                    var replacementMatch = matches[idx];
                    result = scoped.Substring(0, replacementMatch.Index)
                        + replacement
                        + scoped.Substring(replacementMatch.Index + replacementMatch.Length);
                    return true;
                }

                result = Regex.Replace(scoped, pattern, replacement, options);
                return true;
            }

            var matchesForInstr = Regex.Matches(scoped, pattern, options);
            if (matchesForInstr.Count == 0)
            {
                result = string.Equals(fn.Name, "REGEXP_SUBSTR", StringComparison.OrdinalIgnoreCase) ? null : 0;
                return true;
            }

            var index = Math.Min(occurrence - 1, matchesForInstr.Count - 1);
            var instrMatch = matchesForInstr[index];

            if (string.Equals(fn.Name, "REGEXP_INSTR", StringComparison.OrdinalIgnoreCase))
            {
                var positionValue = returnOption == 1
                    ? startIndex + instrMatch.Index + instrMatch.Length
                    : startIndex + instrMatch.Index + 1;
                result = positionValue;
                return true;
            }

            // REGEXP_SUBSTR
            result = instrMatch.Value;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalMemberOfFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MEMBER OF espera dois argumentos.");

        var candidateValue = evalArg(0);
        var jsonValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(candidateValue) || AstQueryExecutorBase.IsNullish(jsonValue))
        {
            result = null;
            return true;
        }

        if (!TryParseJsonCandidate(candidateValue!, out var candidateElement)
            || !TryParseJsonElement(jsonValue!, out var jsonElement))
        {
            result = null;
            return true;
        }

        if (jsonElement.ValueKind != JsonValueKind.Array)
        {
            result = null;
            return true;
        }

        foreach (var item in jsonElement.EnumerateArray())
        {
            if (JsonElementEquals(item, candidateElement))
            {
                result = 1;
                return true;
            }
        }

        result = 0;
        return true;
    }

    private static bool TryEvalIsIpv4Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = row;
        _ = tryConvertNumericToInt64;
        _ = tryConvertNumericToDouble;

        result = TryEvalIpVersionFunction(evalArg, static ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork, out var value)
            ? value
            : null;
        return true;
    }

    private static bool TryEvalIsIpv6Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = row;
        _ = tryConvertNumericToInt64;
        _ = tryConvertNumericToDouble;

        result = TryEvalIpVersionFunction(evalArg, static ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6, out var value)
            ? value
            : null;
        return true;
    }

    private static bool TryEvalIsIpv4CompatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = row;
        _ = tryConvertNumericToInt64;
        _ = tryConvertNumericToDouble;

        result = TryEvalIpv4CompatOrMapped(evalArg, false);
        return true;
    }

    private static bool TryEvalIsIpv4MappedFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        EvalRow row,
        Func<int, object?> evalArg,
        TryConvertNumericToInt64Delegate tryConvertNumericToInt64,
        TryConvertNumericToDoubleDelegate tryConvertNumericToDouble,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = row;
        _ = tryConvertNumericToInt64;
        _ = tryConvertNumericToDouble;

        result = TryEvalIpv4CompatOrMapped(evalArg, true);
        return true;
    }

    private static object? TryEvalIpv4CompatOrMapped(Func<int, object?> evalArg, bool mapped)
    {
        var value = evalArg(0);
        if (IsNullish(value))
            return null;

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
            return 0;

        if (ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
            return 0;

        var bytes = ip.GetAddressBytes();
        if (bytes.Length != 16)
            return 0;

        var isV4Mapped = bytes.Take(10).All(static b => b == 0) && bytes[10] == 0xff && bytes[11] == 0xff;
        return mapped
            ? (isV4Mapped ? 1 : 0)
            : (!isV4Mapped && bytes.Take(12).All(static b => b == 0) ? 1 : 0);
    }

    private static bool TryEvalIpVersionFunction(
        Func<int, object?> evalArg,
        Func<IPAddress, bool> predicate,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return false;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        result = predicate(ip) ? 1 : 0;
        return true;
    }

    private static bool TryParseJsonElement(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
            return true;
        }
        catch
        {
            element = default;
            return false;
        }
    }

    private static bool TryParseJsonCandidate(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        var trimmed = text.TrimStart();
        if (trimmed.StartsWith("{", StringComparison.Ordinal)
            || trimmed.StartsWith("[", StringComparison.Ordinal)
            || trimmed.StartsWith("\"", StringComparison.Ordinal)
            || trimmed.StartsWith("true", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("false", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("null", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("-", StringComparison.Ordinal)
            || char.IsDigit(trimmed[0]))
        {
            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
                return true;
            }
            catch
            {
                // fall through and treat as string
            }
        }

        element = JsonSerializer.SerializeToElement(text);
        return true;
    }

    private static bool JsonElementEquals(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == JsonValueKind.Number
                && right.ValueKind == JsonValueKind.Number)
            {
                if (left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec))
                    return ldec == rdec;

                return left.GetDouble().Equals(right.GetDouble());
            }

            return false;
        }

        return left.ValueKind switch
        {
            JsonValueKind.Object => JsonObjectEquals(left, right),
            JsonValueKind.Array => JsonArrayEquals(left, right),
            JsonValueKind.String => string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal),
            JsonValueKind.Number => left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec)
                ? ldec == rdec
                : left.GetDouble().Equals(right.GetDouble()),
            JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null => true,
            _ => string.Equals(left.GetRawText(), right.GetRawText(), StringComparison.Ordinal)
        };
    }

    private static bool JsonObjectEquals(JsonElement left, JsonElement right)
    {
        var leftProps = left.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
        var rightProps = right.EnumerateObject().ToDictionary(static prop => prop.Name, static prop => prop.Value, StringComparer.Ordinal);
        if (leftProps.Count != rightProps.Count)
            return false;

        foreach (var kvp in leftProps)
        {
            if (!rightProps.TryGetValue(kvp.Key, out var rightValue))
                return false;

            if (!JsonElementEquals(kvp.Value, rightValue))
                return false;
        }

        return true;
    }

    private static bool JsonArrayEquals(JsonElement left, JsonElement right)
    {
        var leftItems = left.EnumerateArray().ToArray();
        var rightItems = right.EnumerateArray().ToArray();
        if (leftItems.Length != rightItems.Length)
            return false;

        for (var i = 0; i < leftItems.Length; i++)
        {
            if (!JsonElementEquals(leftItems[i], rightItems[i]))
                return false;
        }

        return true;
    }

    private static byte[] BuildXorKeyBytes(byte[] key, int length)
    {
        var output = new byte[length];
        if (key.Length == 0)
            return output;

        for (var i = 0; i < key.Length; i++)
            output[i % length] ^= key[i];

        return output;
    }

    private static bool TryGetBinaryPayload(object? value, out byte[] bytes)
    {
        switch (value)
        {
            case byte[] buffer:
                bytes = buffer;
                return true;
            case ArraySegment<byte> segment:
                bytes = [.. segment];
                return true;
            case ReadOnlyMemory<byte> readOnlyMemory:
                bytes = readOnlyMemory.ToArray();
                return true;
            case Memory<byte> memory:
                bytes = memory.ToArray();
                return true;
            case IEnumerable<byte> sequence:
                bytes = [.. sequence];
                return true;
            default:
                bytes = Array.Empty<byte>();
                return false;
        }
    }

    private static string ToHexString(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("X2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }
}
