namespace DbSqlLikeMem;

internal delegate bool AstQueryTryCoerceDateTime(object? value, out DateTime result);

internal delegate bool AstQueryTryParseOffset(string value, out TimeSpan offset);

internal delegate bool AstQueryTryParseCachedDateTimeOffset(string text, DateTimeStyles styles, out DateTimeOffset dto);

internal delegate bool AstQueryTryConvertNumericToDecimal(object? value, out decimal result);

internal sealed class AstQuerySqlServerUtilityFunctionEvaluator(
    Func<ISqlDialect?> getDialect,
    AstQueryTryConvertNumericToDecimal tryConvertNumericToDecimal,
    AstQueryTryCoerceDateTime tryCoerceDateTime,
    AstQueryTryParseOffset tryParseOffset,
    AstQueryTryParseCachedDateTimeOffset tryParseCachedDateTimeOffset)
{
    private readonly Func<ISqlDialect?> _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
    private readonly AstQueryTryConvertNumericToDecimal _tryConvertNumericToDecimal = tryConvertNumericToDecimal ?? throw new ArgumentNullException(nameof(tryConvertNumericToDecimal));
    private readonly AstQueryTryCoerceDateTime _tryCoerceDateTime = tryCoerceDateTime ?? throw new ArgumentNullException(nameof(tryCoerceDateTime));
    private readonly AstQueryTryParseOffset _tryParseOffset = tryParseOffset ?? throw new ArgumentNullException(nameof(tryParseOffset));
    private readonly AstQueryTryParseCachedDateTimeOffset _tryParseCachedDateTimeOffset = tryParseCachedDateTimeOffset ?? throw new ArgumentNullException(nameof(tryParseCachedDateTimeOffset));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalSqlServerGuidFunctions(fn, out result)
            || TryEvalSqlServerStringEscapeFunction(fn, evalArg, out result)
            || TryEvalSqlServerStrFunction(fn, evalArg, out result)
            || TryEvalSqlServerDateTimeOffsetFunctions(fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalSqlServerGuidFunctions(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!(fn.Name.Equals("NEWID", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NEWSEQUENTIALID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = Guid.NewGuid().ToString("D");
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

    private bool TryEvalSqlServerDateTimeOffsetFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("TODATETIMEOFFSET" or "SWITCHOFFSET"))
        {
            result = null;
            return false;
        }

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

        if (name == "TODATETIMEOFFSET")
        {
            if (!_tryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            result = new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified), offset);
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
