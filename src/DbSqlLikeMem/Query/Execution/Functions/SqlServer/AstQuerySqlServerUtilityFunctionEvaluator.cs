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
        Register(handlers, TryEvalCharIndexFunction, "CHARINDEX");
        Register(handlers, TryEvalDataLengthFunction, "DATALENGTH");
        Register(handlers, TryEvalErrorFunctions, "ERROR_LINE", "ERROR_MESSAGE", "ERROR_NUMBER", "ERROR_PROCEDURE", "ERROR_SEVERITY", "ERROR_STATE");
        Register(handlers, TryEvalSqlServerGuidFunction, "NEWID", "NEWSEQUENTIALID");
        Register(handlers, TryEvalSqlServerLocalDateTimeFunction, "CURRENT_TIMESTAMP", "GETDATE", "SYSTEMDATE", "SYSDATETIME");
        Register(handlers, TryEvalSqlServerUtcDateTimeFunction, "SYSUTCDATETIME", "GETUTCDATE");
        Register(handlers, TryEvalSqlServerDateTimeOffsetFunction, "SYSDATETIMEOFFSET");
        Register(handlers, TryEvalSqlServerStringEscapeFunction, "STRING_ESCAPE");
        Register(handlers, TryEvalSqlServerStrFunction, "STR");
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

    internal bool TryEvalCurrentUserFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        out object? result)
    {
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

    private static bool TryEvalSqlServerGuidFunction(
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
        result = DateTime.Now;
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
        result = DateTime.UtcNow;
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
        result = DateTimeOffset.Now;
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

        result = AstQueryGeneralScalarFunctionEvaluator.FormatPrintf(
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
            _ => Encoding.Unicode.GetBytes(value!.ToString() ?? string.Empty)
        };

        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(input, 0, input.Length);

        result = output.ToArray();
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
        var hash = new HashCode();
        for (var i = 0; i < fn.Args.Count; i++)
        {
            var value = evalArg(i);
            if (value is null or DBNull)
            {
                hash.Add(0);
                continue;
            }

            if (value is byte[] bytes)
            {
                foreach (var b in bytes)
                    hash.Add(b);
                continue;
            }

            if (value is string text)
            {
                var normalized = isChecksum ? text.ToUpperInvariant() : text;
                foreach (var ch in normalized)
                    hash.Add(ch);
                continue;
            }

            hash.Add(value);
        }

        result = hash.ToHashCode();
        return true;
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
