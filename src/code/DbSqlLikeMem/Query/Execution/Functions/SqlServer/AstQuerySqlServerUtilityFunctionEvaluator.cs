namespace DbSqlLikeMem;

internal delegate bool AstQueryTryCoerceDateTime(object? value, out DateTime result);

internal delegate bool AstQueryTryParseOffset(string value, out TimeSpan offset);

internal delegate bool AstQueryTryParseCachedDateTimeOffset(string text, DateTimeStyles styles, out DateTimeOffset dto);

internal delegate bool AstQueryTryConvertNumericToDecimal(object? value, out decimal result);

internal delegate bool AstQueryTryEvalSqlServerUtilityFunction(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQuerySqlServerUtilityFunctionEvaluator
{
    private readonly Func<ISqlDialect?> _getDialect;
    private readonly AstQueryTryConvertNumericToDecimal _tryConvertNumericToDecimal;
    private readonly AstQueryTryCoerceDateTime _tryCoerceDateTime;
    private readonly AstQueryTryParseOffset _tryParseOffset;
    private readonly AstQueryTryParseCachedDateTimeOffset _tryParseCachedDateTimeOffset;
    private readonly IReadOnlyDictionary<string, AstQueryTryEvalSqlServerUtilityFunction> _handlers;

    internal AstQuerySqlServerUtilityFunctionEvaluator(
        Func<ISqlDialect?> getDialect,
        AstQueryTryConvertNumericToDecimal tryConvertNumericToDecimal,
        AstQueryTryCoerceDateTime tryCoerceDateTime,
        AstQueryTryParseOffset tryParseOffset,
        AstQueryTryParseCachedDateTimeOffset tryParseCachedDateTimeOffset)
    {
        _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
        _tryConvertNumericToDecimal = tryConvertNumericToDecimal ?? throw new ArgumentNullException(nameof(tryConvertNumericToDecimal));
        _tryCoerceDateTime = tryCoerceDateTime ?? throw new ArgumentNullException(nameof(tryCoerceDateTime));
        _tryParseOffset = tryParseOffset ?? throw new ArgumentNullException(nameof(tryParseOffset));
        _tryParseCachedDateTimeOffset = tryParseCachedDateTimeOffset ?? throw new ArgumentNullException(nameof(tryParseCachedDateTimeOffset));
        _handlers = CreateHandlers();
    }

    private Dictionary<string, AstQueryTryEvalSqlServerUtilityFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalSqlServerUtilityFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalAppNameFunction, "APP_NAME");
        Register(handlers, TryEvalGetAnsiNullFunction, "GETANSINULL");
        Register(handlers, TryEvalCharIndexFunction, "CHARINDEX");
        Register(handlers, TryEvalDataLengthFunction, "DATALENGTH");
        Register(handlers, TryEvalLenFunction, "LEN");
        Register(handlers, TryEvalErrorFunctions, "ERROR_LINE", "ERROR_MESSAGE", "ERROR_NUMBER", "ERROR_PROCEDURE", "ERROR_SEVERITY", "ERROR_STATE");
        Register(handlers, TryEvalHostIdFunction, "HOST_ID");
        Register(handlers, TryEvalHostNameFunction, "HOST_NAME");
        Register(handlers, TryEvalSqlServerGuidFunction, "NEWID", "NEWSEQUENTIALID");
        Register(handlers, TryEvalSqlServerLocalDateTimeFunction, "CURRENT_TIMESTAMP", "GETDATE", "SYSTEMDATE", "SYSDATETIME");
        Register(handlers, TryEvalSqlServerUtcDateTimeFunction, "SYSUTCDATETIME", "GETUTCDATE");
        Register(handlers, TryEvalSqlServerDateTimeOffsetFunction, "SYSDATETIMEOFFSET");
        Register(handlers, TryEvalSessionUserFunction, "SESSION_USER");
        Register(handlers, TryEvalSystemUserFunction, "SYSTEM_USER");
        Register(handlers, TryEvalIsDateFunction, "ISDATE");
        Register(handlers, TryEvalIsJsonFunction, "ISJSON");
        Register(handlers, TryEvalIsNumericFunction, "ISNUMERIC");
        Register(handlers, TryEvalSqlServerStringEscapeFunction, "STRING_ESCAPE");
        Register(handlers, TryEvalSqlServerStrFunction, "STR");
        Register(handlers, TryEvalSoundexFunction, "SOUNDEX");
        Register(handlers, TryEvalDifferenceFunction, "DIFFERENCE");
        Register(handlers, TryEvalSqlServerToDateTimeOffsetFunction, "TODATETIMEOFFSET");
        Register(handlers, TryEvalSqlServerSwitchOffsetFunction, "SWITCHOFFSET");
        Register(handlers, TryEvalSqlServerFormatFunction, "FORMAT");
        Register(handlers, TryEvalSqlServerFormatMessageFunction, "FORMATMESSAGE");
        Register(handlers, TryEvalSqlServerCompressFunction, "COMPRESS");
        Register(handlers, TryEvalSqlServerDecompressFunction, "DECOMPRESS");
        Register(handlers, TryEvalSqlServerChecksumFunction, "CHECKSUM", "BINARY_CHECKSUM");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalSqlServerUtilityFunction> handlers,
        AstQueryTryEvalSqlServerUtilityFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    internal static bool TryEvalAppNameFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = "DbSqlLikeMem";
        return true;
    }

    internal static bool TryEvalCharIndexFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var start = fn.Args.Count > 2 ? evalArg(2) : null;
        var startIndex = 0;

        if (!AstQueryExecutorBase.IsNullish(start))
        {
            startIndex = Convert.ToInt32(start.ToDec()) - 1;
            if (startIndex < 0)
            {
                result = 0;
                return true;
            }
        }

        if (needle.Length == 0)
        {
            result = startIndex + 1;
            return true;
        }

        var index = haystack.IndexOf(needle, startIndex, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    internal static bool TryEvalDataLengthFunction(
        QueryExecutionContext context,
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

        if (value is byte[] bytes)
        {
            result = bytes.Length;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = Encoding.Unicode.GetByteCount(text);
        return true;
    }

    internal static bool TryEvalLenFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = text.TrimEnd().Length;
        return true;
    }

    internal static bool TryEvalGetAnsiNullFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;

        result = 1;
        return true;
    }

    internal static bool TryEvalHostIdFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = 1;
        return true;
    }

    internal static bool TryEvalHostNameFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = "localhost";
        return true;
    }

    internal static bool TryEvalDifferenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var first = evalArg(0)?.ToString() ?? string.Empty;
        var second = evalArg(1)?.ToString() ?? string.Empty;
        var soundex1 = AstQuerySqlServerResolutionHelper.ComputeSoundex(first);
        var soundex2 = AstQuerySqlServerResolutionHelper.ComputeSoundex(second);
        var score = 0;
        for (var i = 0; i < Math.Min(soundex1.Length, soundex2.Length); i++)
        {
            if (soundex1[i] == soundex2[i])
                score++;
        }

        result = score;
        return true;
    }

    internal static bool TryEvalSoundexFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SOUNDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = AstQuerySqlServerResolutionHelper.ComputeSoundex(value?.ToString() ?? string.Empty);
        return true;
    }

    internal static bool TryEvalSessionUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;

        result = "dbo";
        return true;
    }

    internal static bool TryEvalSystemUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;
        _ = evalArg;

        result = "sa";
        return true;
    }

    internal static bool TryEvalIsDateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = AstQueryExecutorBase.TryCoerceDateTime(value, out _)
            ? 1
            : 0;
        return true;
    }

    internal static bool TryEvalIsJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out _);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    internal static bool TryEvalIsNumericFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = double.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out _)
            ? 1
            : 0;
        return true;
    }

    internal bool TryEvalCurrentUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        out object? result)
    {
        if (!string.Equals(fn.Name, "CURRENT_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        _ = context;
        _ = fn;

        result = "dbo";
        return true;
    }

    internal static bool TryEvalCurrentUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "CURRENT_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        _ = context;
        _ = fn;
        _ = evalArg;

        result = "dbo";
        return true;
    }

    internal bool TryEvalSessionContextFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        if (fn.Args.Count == 0)
            throw new InvalidOperationException("SESSION_CONTEXT() expects a key.");

        var key = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(key))
        {
            result = null;
            return true;
        }

        context.Connection.TryGetSessionContextValue(key!, out result);
        return true;
    }

    internal static bool TryEvalSqlServerJsonModifyFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "JSON_MODIFY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && !definition.AllowsCall)
        {
            throw context.NotSupported("JSON_MODIFY");
        }

        if (definition is null)
            throw context.NotSupported("JSON_MODIFY");

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("JSON_MODIFY() espera JSON, path e novo valor.");

        var json = evalArg(0);
        var pathValue = evalArg(1)?.ToString();
        var newValue = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(json) || string.IsNullOrWhiteSpace(pathValue) || !AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        if (!AstQueryJsonPathFunctionEvaluator.TryParseSqlServerJsonModifyPath(pathValue!, out var tokens, out var append, out var strict))
        {
            result = null;
            return true;
        }

        var exists = AstQueryJsonPathFunctionEvaluator.TryGetJsonNodeAtPath(root, tokens, out var existingNode);
        if (append)
        {
            var array = existingNode as System.Text.Json.Nodes.JsonArray;
            if (!exists || array is null)
            {
                if (strict)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                result = root.ToJsonString();
                return true;
            }

            array.Add(AstQueryJsonPathFunctionEvaluator.CreateJsonNodeFromValue(newValue));
            result = root.ToJsonString();
            return true;
        }

        if (AstQueryExecutorBase.IsNullish(newValue))
        {
            if (strict)
            {
                if (!exists)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                if (!AstQueryJsonPathFunctionEvaluator.TrySetJsonPathValue(ref root, tokens, null))
                {
                    result = null;
                    return true;
                }
            }
            else if (exists)
            {
                AstQueryJsonPathFunctionEvaluator.TryRemoveJsonPathValue(root, tokens);
            }

            result = root.ToJsonString();
            return true;
        }

        if (strict && !exists)
            throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

        if (!AstQueryJsonPathFunctionEvaluator.TrySetJsonPathValue(ref root, tokens, newValue))
        {
            if (strict)
                throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

            result = root.ToJsonString();
            return true;
        }

        result = root.ToJsonString();
        return true;
    }

    internal static bool TryEvalOpenJsonFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!string.Equals(fn.Name, SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
            || openJsonDefinition is null)
            throw context.NotSupported(SqlConst.OPENJSON);

        var json = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(json) ? null : json?.ToString();
        return true;
    }

    internal static bool TryEvalSqlServerGuidFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = Guid.NewGuid().ToString("D");
        return true;
    }

    private static bool TryEvalSqlServerLocalDateTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        _ = fn;
        result = context.EvaluationUtcNow;
        return true;
    }

    private static bool TryEvalSqlServerUtcDateTimeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = context.EvaluationUtcNow;
        return true;
    }

    private static bool TryEvalSqlServerDateTimeOffsetFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = evalArg;
        result = new DateTimeOffset(context.EvaluationUtcNow, TimeSpan.Zero);
        return true;
    }

    private bool TryEvalSqlServerStringEscapeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para STRING_ESCAPE.");
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("STRING_ESCAPE() espera texto e tipo.");

        var textValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(textValue))
        {
            result = null;
            return true;
        }

        var typeValue = evalArg(1)?.ToString() ?? string.Empty;
        if (!typeValue.Equals("json", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("STRING_ESCAPE() currently supports only 'json' in the mock.");

        result = EscapeSqlServerJsonString(textValue?.ToString() ?? string.Empty);
        return true;
    }

    private static string EscapeSqlServerJsonString(string text)
    {
        var builder = new StringBuilder(text.Length);
        foreach (var ch in text)
        {
            builder.Append(ch switch
            {
                '"' => "\\\"",
                '\\' => "\\\\",
                '\b' => "\\b",
                '\f' => "\\f",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                _ when ch < 0x20 => $"\\u{(int)ch:x4}",
                _ => ch.ToString()
            });
        }

        return builder.ToString();
    }

    internal static bool TryEvalSqlServerFormatFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("FORMAT() espera valor e máscara.");

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var format = evalArg(1)?.ToString();
        var cultureName = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var culture = string.IsNullOrWhiteSpace(cultureName)
            ? CultureInfo.InvariantCulture
            : CultureInfo.GetCultureInfo(cultureName!);

        result = value is IFormattable formattable
            ? formattable.ToString(format, culture)
            : value!.ToString();
        return true;
    }

    internal static bool TryEvalSqlServerFormatMessageFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count == 0)
            throw new InvalidOperationException("FORMATMESSAGE() espera ao menos a mensagem.");

        result = AstQueryFormatFunctionHelper.FormatPrintf(
            evalArg(0)?.ToString() ?? string.Empty,
            [.. Enumerable.Range(1, Math.Max(0, fn.Args.Count - 1)).Select(evalArg)]);
        return true;
    }

    internal static bool TryEvalSqlServerCompressFunction(
        QueryExecutionContext context,
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

        var input = value switch
        {
            byte[] bytes => bytes,
            _ => Encoding.UTF8.GetBytes(value!.ToString() ?? string.Empty)
        };

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(input, 0, input.Length);

        var compressed = output.ToArray();
        if (compressed.Length >= 10)
        {
            // SQL Server uses a stable gzip header that differs from the runtime default.
            compressed[8] = 0x04;
            compressed[9] = 0x00;
        }

        result = compressed;
        return true;
    }

    internal static bool TryEvalSqlServerDecompressFunction(
        QueryExecutionContext context,
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

        if (value is not byte[] bytes)
        {
            result = null;
            return true;
        }

        using var input = new MemoryStream(bytes);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        result = output.ToArray();
        return true;
    }

    internal static bool TryEvalSqlServerChecksumFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isChecksum = fn.ResolvedScalarFunction?.Name.Equals("CHECKSUM", StringComparison.OrdinalIgnoreCase) == true
            || string.Equals(fn.Name, "CHECKSUM", StringComparison.OrdinalIgnoreCase);
        var hash = 17;
        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            hash = unchecked((hash * 31) + ComputeSqlServerChecksumComponent(value, binary: !isChecksum));
        }

        result = fn.Args.Count == 0 ? 0 : hash;
        return true;
    }

    internal static int ComputeSqlServerChecksumComponent(object? value, bool binary)
    {
        if (value is null or DBNull)
            return 0;

        return value switch
        {
            byte[] bytes => ComputeSqlServerBytesChecksum(bytes),
            string text => ComputeSqlServerTextChecksum(text, binary),
            char ch => ComputeSqlServerTextChecksum(ch.ToString(), binary),
            bool b => b ? 1 : 0,
            sbyte sb => sb,
            byte b => b,
            short s => s,
            ushort us => us,
            int i => i,
            uint ui => unchecked((int)ui),
            long l => unchecked((int)l),
            ulong ul => unchecked((int)ul),
            float f => unchecked((int)BitConverterCompatibility.SingleToInt32Bits(f)),
            double d => unchecked((int)BitConverterCompatibility.DoubleToInt64Bits(d)),
            decimal dec => ComputeSqlServerDecimalChecksum(dec),
            DateTime dt => unchecked((int)dt.Ticks),
            DateTimeOffset dto => unchecked((int)dto.UtcTicks),
            TimeSpan ts => unchecked((int)ts.Ticks),
            _ => ComputeSqlServerTextChecksum(value.ToString() ?? string.Empty, binary)
        };
    }

    private static int ComputeSqlServerDecimalChecksum(decimal value)
    {
        var bits = decimal.GetBits(value);
        unchecked
        {
            var hash = 17;
            for (var i = 0; i < bits.Length; i++)
                hash = (hash * 31) + bits[i];

            return hash;
        }
    }

    private static int ComputeSqlServerTextChecksum(string text, bool binary)
    {
        unchecked
        {
            var hash = 17;
            foreach (var ch in text)
            {
                var normalized = binary ? ch : char.ToUpperInvariant(ch);
                hash = (hash * 31) + normalized;
            }

            return hash;
        }
    }

    private static int ComputeSqlServerBytesChecksum(ReadOnlySpan<byte> bytes)
    {
        unchecked
        {
            var hash = 17;
            foreach (var b in bytes)
                hash = (hash * 31) + b;

            return hash;
        }
    }

    internal static bool TryEvalErrorFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = string.Equals(fn.Name, "ERROR_MESSAGE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "ERROR_PROCEDURE", StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : 0;
        return true;
    }

    private bool TryEvalSqlServerStrFunction(
        QueryExecutionContext context,
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

        if (!_tryConvertNumericToDecimal(value, out var number))
        {
            result = null;
            return true;
        }

        var length = fn.Args.Count > 1 ? Convert.ToInt32(evalArg(1).ToDec(), CultureInfo.InvariantCulture) : 10;
        var decimals = fn.Args.Count > 2 ? Convert.ToInt32(evalArg(2).ToDec(), CultureInfo.InvariantCulture) : 0;
        decimals = Math.Min(16, Math.Max(0, decimals));

        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        var rounded = Math.Round(number, decimals, MidpointRounding.AwayFromZero);
        var text = rounded.ToString($"F{decimals}", CultureInfo.InvariantCulture);
        if (text.Length > length)
        {
            result = new string('*', length);
            return true;
        }

        result = text.PadLeft(length, ' ');
        return true;
    }

    private bool TryEvalSqlServerToDateTimeOffsetFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() expects value and offset.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        var offsetText = evalArg(1)?.ToString() ?? string.Empty;
        if (!_tryParseOffset(offsetText, out var offset))
        {
            result = null;
            return true;
        }

        if (!_tryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        result = new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified), offset);
        return true;
    }

    private bool TryEvalSqlServerSwitchOffsetFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException($"{fn.Name}() expects value and offset.");

        var baseValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        var offsetText = evalArg(1)?.ToString() ?? string.Empty;
        if (!_tryParseOffset(offsetText, out var offset))
        {
            result = null;
            return true;
        }

        DateTimeOffset dto;
        if (baseValue is DateTimeOffset directDto)
        {
            dto = directDto;
        }
        else if (!_tryParseCachedDateTimeOffset(baseValue!.ToString()!, DateTimeStyles.AllowWhiteSpaces, out dto))
        {
            result = null;
            return true;
        }

        result = dto.ToOffset(offset);
        return true;
    }
}
