namespace DbSqlLikeMem;

using System;
using System.Globalization;

internal static class AstQueryPostgresDateFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is "DATE_TRUNC")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("DATE_TRUNC() espera unidade e data.");

            var unitText = evalArg(0)?.ToString() ?? string.Empty;
            var value = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value) || string.IsNullOrWhiteSpace(unitText) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            var unit = AstQueryExecutorBase.ResolveTemporalUnit(unitText);
            result = AstQueryExecutorBase.TruncateDateTime(dateTime, unit);
            return true;
        }

        if (name is "DATE_PART")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("DATE_PART() espera unidade e data.");

            var unit = AstQueryExecutorBase.ResolveTemporalUnit(evalArg(0)?.ToString() ?? string.Empty);
            var value = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value) || unit == AstQueryExecutorBase.TemporalUnit.Unknown || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            var temporalPart = AstQueryExecutorBase.GetTemporalPartValue(dateTime, unit);
            result = temporalPart is null ? null : (double)temporalPart.Value;
            return true;
        }

        if (name is "AGE")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var left = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(left) || !AstQueryExecutorBase.TryCoerceDateTime(left, out var leftDate))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count == 1)
            {
                result = DateTime.Now - leftDate;
                return true;
            }

            var right = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(right) || !AstQueryExecutorBase.TryCoerceDateTime(right, out var rightDate))
            {
                result = null;
                return true;
            }

            result = leftDate - rightDate;
            return true;
        }

        if (name is "MAKE_INTERVAL")
        {
            var years = fn.Args.Count > 0 ? Convert.ToInt32(evalArg(0).ToDec()) : 0;
            var months = fn.Args.Count > 1 ? Convert.ToInt32(evalArg(1).ToDec()) : 0;
            var weeks = fn.Args.Count > 2 ? Convert.ToInt32(evalArg(2).ToDec()) : 0;
            var days = fn.Args.Count > 3 ? Convert.ToInt32(evalArg(3).ToDec()) : 0;
            var hours = fn.Args.Count > 4 ? Convert.ToInt32(evalArg(4).ToDec()) : 0;
            var mins = fn.Args.Count > 5 ? Convert.ToInt32(evalArg(5).ToDec()) : 0;
            var secs = fn.Args.Count > 6 ? Convert.ToDouble(evalArg(6), CultureInfo.InvariantCulture) : 0d;

            result = TimeSpan.FromDays((years * 365) + (months * 30) + (weeks * 7) + days)
                .Add(TimeSpan.FromHours(hours))
                .Add(TimeSpan.FromMinutes(mins))
                .Add(TimeSpan.FromSeconds(secs));
            return true;
        }

        if (name is "MAKE_DATE")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("MAKE_DATE() espera ano, mes e dia.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            result = new DateTime(year, month, day);
            return true;
        }

        if (name is "MAKE_TIME")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("MAKE_TIME() espera hora, minuto e segundo.");

            var hour = Convert.ToInt32(evalArg(0).ToDec());
            var minute = Convert.ToInt32(evalArg(1).ToDec());
            var second = Convert.ToInt32(evalArg(2).ToDec());
            result = new TimeSpan(hour, minute, second);
            return true;
        }

        if (name is "MAKE_TIMESTAMP")
        {
            if (fn.Args.Count < 6)
                throw new InvalidOperationException("MAKE_TIMESTAMP() espera data e hora.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            var hour = Convert.ToInt32(evalArg(3).ToDec());
            var minute = Convert.ToInt32(evalArg(4).ToDec());
            var second = Convert.ToInt32(evalArg(5).ToDec());
            result = new DateTime(year, month, day, hour, minute, second);
            return true;
        }

        if (name is "MAKE_TIMESTAMPTZ")
        {
            if (fn.Args.Count < 6)
                throw new InvalidOperationException("MAKE_TIMESTAMPTZ() espera data e hora.");

            var year = Convert.ToInt32(evalArg(0).ToDec());
            var month = Convert.ToInt32(evalArg(1).ToDec());
            var day = Convert.ToInt32(evalArg(2).ToDec());
            var hour = Convert.ToInt32(evalArg(3).ToDec());
            var minute = Convert.ToInt32(evalArg(4).ToDec());
            var second = Convert.ToInt32(evalArg(5).ToDec());
            result = new DateTimeOffset(year, month, day, hour, minute, second, DateTimeOffset.Now.Offset);
            return true;
        }

        if (name is "TO_DATE")
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

            if (value is DateTime dateValue)
            {
                result = dateValue.Date;
                return true;
            }

            var textValue = value?.ToString() ?? string.Empty;
            var maskValue = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (AstQueryFormatFunctionHelper.TryParseOracleDateTime(textValue, maskValue, out var parsed))
            {
                result = parsed.Date;
                return true;
            }

            if (AstQueryExecutorBase.TryParseCachedDateTime(textValue, DateTimeStyles.AllowWhiteSpaces, out var fallbackParsed))
            {
                result = fallbackParsed.Date;
                return true;
            }

            result = null;
            return true;
        }

        if (name is "TO_CHAR")
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

            if (value is DateTime dateValue)
            {
                if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                {
                    var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                    result = dateValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                }
                else
                {
                    result = dateValue.ToString(CultureInfo.InvariantCulture);
                }

                return true;
            }

            if (value is DateTimeOffset dtoValue)
            {
                if (fn.Args.Count > 1 && evalArg(1) is string fmt)
                {
                    var netFormat = AstQueryFormatFunctionHelper.NormalizeOracleFormatMask(fmt, out _);
                    result = dtoValue.ToString(netFormat ?? fmt, CultureInfo.InvariantCulture);
                }
                else
                {
                    result = dtoValue.ToString(CultureInfo.InvariantCulture);
                }

                return true;
            }

            if (AstQueryFormatFunctionHelper.IsNumericValue(value))
            {
                var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
                if (!string.IsNullOrWhiteSpace(mask))
                {
                    result = AstQueryFormatFunctionHelper.FormatPostgreSqlNumber(value!, mask!);
                    return true;
                }
            }

            result = value!.ToString();
            return true;
        }

        result = null;
        return false;
    }
}
