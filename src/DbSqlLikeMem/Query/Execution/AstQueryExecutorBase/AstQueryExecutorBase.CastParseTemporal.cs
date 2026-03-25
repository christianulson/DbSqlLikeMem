namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private object? EvalTryCast(FunctionCallExpr fn, Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2) return null;
        var v = evalArg(0);
        var type = fn.Args[1] is RawSqlExpr trx ? trx.Sql : (evalArg(1)?.ToString() ?? "");
        type = type.Trim();
        if (IsNullish(v)) return null;

        try
        {
            if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
            {
                if (v is long l) return (int)l;
                if (v is int i) return i;
                if (v is decimal d) return (int)d;
                if (int.TryParse(v!.ToString(), out var ix)) return ix;
                if (long.TryParse(v!.ToString(), out var lx)) return (int)lx;
                return null;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (v is decimal dd) return dd;
                if (decimal.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return dx;
                return null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (v is double dfx) return dfx;
                if (v is float ffx) return (double)ffx;
                if (v is decimal ddx) return (double)ddx;
                if (double.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var fx)) return fx;
                return null;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                return TryCoerceDateTime(v, out var dt) ? dt : null;
            }

            return v!.ToString();
        }
        catch
        {
            return null;
        }
    }

    private object? EvalParseFunction(FunctionCallExpr fn, Func<int, object?> evalArg, bool swallowErrors)
    {
        if (fn.Args.Count < 2)
            return swallowErrors ? null : throw new InvalidOperationException($"{fn.Name}() requires value and target type.");

        var value = evalArg(0);
        if (IsNullish(value))
            return null;

        var type = fn.Args[1] is RawSqlExpr rx ? rx.Sql : (evalArg(1)?.ToString() ?? string.Empty);
        type = type.Trim();
        var cultureName = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;

        try
        {
            var culture = string.IsNullOrWhiteSpace(cultureName)
                ? CultureInfo.InvariantCulture
                : CultureInfo.GetCultureInfo(cultureName!);

            if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para PARSE.")).IsIntegerCastTypeName(type))
            {
                if (int.TryParse(value!.ToString(), NumberStyles.Integer, culture, out var parsedInt))
                    return parsedInt;
                return null;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (decimal.TryParse(value!.ToString(), NumberStyles.Any, culture, out var parsedDecimal))
                    return parsedDecimal;
                return null;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (double.TryParse(value!.ToString(), NumberStyles.Any, culture, out var parsedDouble))
                    return parsedDouble;
                return null;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase))
            {
                if (TryParseCachedDateTime(value!.ToString()!, culture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate))
                    return parsedDate;
                return null;
            }

            return value!.ToString();
        }
        catch
        {
            if (swallowErrors)
                return null;
            throw;
        }
    }

    private object? EvalCast(FunctionCallExpr fn, Func<int, object?> evalArg)
    {
        if (fn.Args.Count < 2) return null;

        var v = evalArg(0);
        var type = fn.Args[1] is RawSqlExpr rx ? rx.Sql : (evalArg(1)?.ToString() ?? "");
        type = type.Trim();
        if (IsNullish(v)) return null;

        try
        {
            if ((Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.")).IsIntegerCastTypeName(type))
            {
                if (v is long l) return (int)l;
                if (v is int i) return i;
                if (v is decimal d) return (int)d;
                var text = v!.ToString()?.Trim() ?? string.Empty;
                if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ix)) return ix;
                if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var lx)) return (int)lx;
                if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return (int)dx;
                return 0;
            }

            if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
            {
                if (v is decimal dd) return dd;
                if (decimal.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var dx)) return dx;
                return 0m;
            }

            if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            {
                if (v is double dfx) return dfx;
                if (v is float ffx) return (double)ffx;
                if (v is decimal ddx) return (double)ddx;
                if (double.TryParse(v!.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var fx)) return fx;
                return 0d;
            }

            if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            {
                return TryCoerceDateTime(v, out var dt) ? dt : null;
            }

            if (type.Equals("JSON", StringComparison.OrdinalIgnoreCase))
            {
                static string? ValidateJsonOrNull(string? json)
                {
                    if (json is null || string.IsNullOrWhiteSpace(json))
                        return null;

                    var normalizedJson = json.Trim();

                    QueryJsonFunctionHelper.TryGetJsonRootElement(normalizedJson, out _);
                    return normalizedJson;
                }

                if (v is string s)
                    return ValidateJsonOrNull(s);

                if (v is System.Text.Json.JsonElement je)
                    return ValidateJsonOrNull(je.GetRawText());

                var serialized = JsonSerializer.Serialize(v);
                return ValidateJsonOrNull(serialized);
            }

            return v!.ToString();
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    internal static bool TryGetJsonAndPathArguments(
        Func<int, object?> evalArg,
        out object? json,
        out string? path)
    {
        json = evalArg(0);
        path = evalArg(1)?.ToString();
        return !IsNullish(json) && !string.IsNullOrWhiteSpace(path);
    }

    internal static void LogFunctionEvaluationFailure(Exception exception)
    {
#pragma warning disable CA1303 // Do not pass literals as localized parameters
        Console.WriteLine($"{nameof(AstQueryExecutorBase)}.{nameof(EvalFunction)}");
#pragma warning restore CA1303 // Do not pass literals as localized parameters
        Console.WriteLine(exception);
    }

    private string GetDateAddUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var unit = expr switch
        {
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => col.Name,
            LiteralExpr lit => lit.Value?.ToString() ?? string.Empty,
            _ => null
        };

        if (string.IsNullOrWhiteSpace(unit))
        {
            var eval = Eval(expr, row, group, ctes);
            unit = eval?.ToString() ?? string.Empty;
        }

        return unit!.Trim().ToUpperInvariant();
    }

    private TemporalUnit GetTemporalUnit(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => ResolveTemporalUnit(GetDateAddUnit(expr, row, group, ctes));

    internal static TemporalUnit ResolveTemporalUnit(string unit)
        => _temporalUnits.TryGetValue(unit, out var resolved)
            ? resolved
            : TemporalUnit.Unknown;

    internal static bool TryCoerceDecimal(object? value, out decimal result)
    {
        result = default;

        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
