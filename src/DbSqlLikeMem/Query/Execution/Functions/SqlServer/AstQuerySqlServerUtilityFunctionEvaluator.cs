namespace DbSqlLikeMem;

internal delegate bool AstQueryTryCoerceDateTime(object? value, out DateTime result);

internal delegate bool AstQueryTryParseOffset(string value, out TimeSpan offset);

internal delegate bool AstQueryTryParseCachedDateTimeOffset(string text, DateTimeStyles styles, out DateTimeOffset dto);

internal delegate bool AstQueryTryConvertNumericToDecimal(object? value, out decimal result);

internal delegate bool AstQueryTryEvalSqlServerUtilityFunction(
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
        Register(handlers, TryEvalSqlServerGuidFunction, "NEWID", "NEWSEQUENTIALID");
        Register(handlers, TryEvalSqlServerLocalDateTimeFunction, "CURRENT_TIMESTAMP", "GETDATE", "SYSTEMDATE", "SYSDATETIME");
        Register(handlers, TryEvalSqlServerUtcDateTimeFunction, "SYSUTCDATETIME", "GETUTCDATE");
        Register(handlers, TryEvalSqlServerDateTimeOffsetFunction, "SYSDATETIMEOFFSET");
        Register(handlers, TryEvalSqlServerStringEscapeFunction, "STRING_ESCAPE");
        Register(handlers, TryEvalSqlServerStrFunction, "STR");
        Register(handlers, TryEvalSqlServerToDateTimeOffsetFunction, "TODATETIMEOFFSET");
        Register(handlers, TryEvalSqlServerSwitchOffsetFunction, "SWITCHOFFSET");
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
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalSqlServerGuidFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = Guid.NewGuid().ToString("D");
        return true;
    }

    private static bool TryEvalSqlServerLocalDateTimeFunction(
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
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STRING_ESCAPE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para STRING_ESCAPE.");
        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("sqlazure", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

    private bool TryEvalSqlServerStrFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("STR", StringComparison.OrdinalIgnoreCase))
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
