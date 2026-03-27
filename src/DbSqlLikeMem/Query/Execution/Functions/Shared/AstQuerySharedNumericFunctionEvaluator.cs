using System.Globalization;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySharedNumericFunctionEvaluator
{
    private static readonly Random _sharedRandom = new();
    private static readonly object _randomLock = new();

    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (string.Equals(fn.Name, "GREATEST", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "LEAST", StringComparison.OrdinalIgnoreCase))
            return TryEvalMinMaxFunction(context, fn, evalArg, out result);

        if (string.Equals(fn.Name, "ABS", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "ABSVAL", StringComparison.OrdinalIgnoreCase))
            return TryEvalAbsFunction(evalArg, out result);

        if (string.Equals(fn.Name, "BIN", StringComparison.OrdinalIgnoreCase))
            return TryEvalBinFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ACOS", StringComparison.OrdinalIgnoreCase))
            return TryEvalAcosFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ASIN", StringComparison.OrdinalIgnoreCase))
            return TryEvalAsinFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ATAN", StringComparison.OrdinalIgnoreCase))
            return TryEvalAtanFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ATAN2", StringComparison.OrdinalIgnoreCase))
            return TryEvalAtan2Function(fn, evalArg, out result);

        if (string.Equals(fn.Name, "CEIL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CEILING", StringComparison.OrdinalIgnoreCase))
            return TryEvalCeilingFunction(evalArg, out result);

        if (string.Equals(fn.Name, "DEGREES", StringComparison.OrdinalIgnoreCase))
            return TryEvalDegreesFunction(evalArg, out result);

        if (string.Equals(fn.Name, "COS", StringComparison.OrdinalIgnoreCase))
            return TryEvalCosFunction(evalArg, out result);

        if (string.Equals(fn.Name, "COT", StringComparison.OrdinalIgnoreCase))
            return TryEvalCotFunction(evalArg, out result);

        if (string.Equals(fn.Name, "EXP", StringComparison.OrdinalIgnoreCase))
            return TryEvalExpFunction(evalArg, out result);

        if (string.Equals(fn.Name, "LN", StringComparison.OrdinalIgnoreCase))
            return TryEvalNaturalLogFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "LOG", StringComparison.OrdinalIgnoreCase))
            return TryEvalLogFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "LOG10", StringComparison.OrdinalIgnoreCase))
            return TryEvalLog10Function(evalArg, out result);

        if (string.Equals(fn.Name, "FLOOR", StringComparison.OrdinalIgnoreCase))
            return TryEvalFloorFunction(evalArg, out result);

        if (string.Equals(fn.Name, "MOD", StringComparison.OrdinalIgnoreCase))
            return TryEvalModFunction(evalArg, out result);

        if (string.Equals(fn.Name, "PI", StringComparison.OrdinalIgnoreCase))
            return TryEvalPiFunction(out result);

        if (string.Equals(fn.Name, "POWER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "POW", StringComparison.OrdinalIgnoreCase))
            return TryEvalPowerFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "RADIANS", StringComparison.OrdinalIgnoreCase))
            return TryEvalRadiansFunction(evalArg, out result);

        if (string.Equals(fn.Name, "RAND", StringComparison.OrdinalIgnoreCase))
            return TryEvalRandFunction(evalArg, out result);

        if (string.Equals(fn.Name, "ROUND", StringComparison.OrdinalIgnoreCase))
            return TryEvalRoundFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "SIGN", StringComparison.OrdinalIgnoreCase))
            return TryEvalSignFunction(evalArg, out result);

        if (string.Equals(fn.Name, "SIN", StringComparison.OrdinalIgnoreCase))
            return TryEvalSinFunction(evalArg, out result);

        if (string.Equals(fn.Name, "SQRT", StringComparison.OrdinalIgnoreCase))
            return TryEvalSqrtFunction(evalArg, out result);

        if (string.Equals(fn.Name, "TAN", StringComparison.OrdinalIgnoreCase))
            return TryEvalTanFunction(evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalMinMaxFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        var isGreatest = string.Equals(fn.Name, "GREATEST", StringComparison.OrdinalIgnoreCase);
        var isLeast = string.Equals(fn.Name, "LEAST", StringComparison.OrdinalIgnoreCase);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        object? current = null;
        foreach (var index in Enumerable.Range(0, fn.Args.Count))
        {
            var value = evalArg(index);
            if (IsNullish(value))
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
            else if (isLeast && comparison > 0)
                current = value;
        }

        result = current;
        return true;
    }

    private static bool TryEvalDegreesFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var radians = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = radians * (180d / Math.PI);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAcosFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Acos(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAbsFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Abs(dec);
                return true;
            }

            if (value is float or double)
            {
                result = Math.Abs(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                return true;
            }

            result = Math.Abs(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAsinFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Asin(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAtanFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Atan(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAtan2Function(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ATAN2() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Atan2(
                Convert.ToDouble(left, CultureInfo.InvariantCulture),
                Convert.ToDouble(right, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCeilingFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Ceiling(dec);
                return true;
            }

            result = Math.Ceiling(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalExpFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = Math.Exp(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCosFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Cos(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCotFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var tangent = Math.Tan(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            result = tangent == 0d ? null : 1d / tangent;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalFloorFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Floor(dec);
                return true;
            }

            result = Math.Floor(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBinFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = Convert.ToString(number, 2);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalNaturalLogFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            if (number <= 0)
            {
                result = null;
                return true;
            }

            result = Math.Log(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalLogFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            return TryEvalNaturalLogFunction(fn, evalArg, out result);

        var value = evalArg(1);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var baseValue = evalArg(0);
        if (IsNullish(baseValue))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            var baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            if (number <= 0 || baseNumber <= 0 || baseNumber == 1d)
            {
                result = null;
                return true;
            }

            result = Math.Log(number, baseNumber);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalLog10Function(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number <= 0 ? null : Math.Log10(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalModFunction(Func<int, object?> evalArg, out object? result)
    {
        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            var r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            result = r == 0m ? null : l % r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalPiFunction(out object? result)
    {
        result = Math.PI;
        return true;
    }

    private static bool TryEvalPowerFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("POWER() espera base e expoente.");

        var baseValue = evalArg(0);
        var expValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(expValue))
        {
            result = null;
            return true;
        }

        try
        {
            var baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            var expNumber = Convert.ToDouble(expValue, CultureInfo.InvariantCulture);
            result = Math.Pow(baseNumber, expNumber);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalRadiansFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var degrees = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = degrees * (Math.PI / 180d);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalRandFunction(Func<int, object?> evalArg, out object? result)
    {
        var seedValue = evalArg(0);
        double next;
        if (IsNullish(seedValue))
        {
            lock (_randomLock)
                next = _sharedRandom.NextDouble();
        }
        else
        {
            var seeded = new Random(Convert.ToInt32(seedValue.ToDec()));
            next = seeded.NextDouble();
        }

        result = next;
        return true;
    }

    private static bool TryEvalRoundFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var decimals = fn.Args.Count > 1 ? evalArg(1) : null;
        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (IsNullish(decimals))
            {
                result = Math.Round(number, 0, MidpointRounding.AwayFromZero);
                return true;
            }

            var digits = Convert.ToInt32(decimals.ToDec());
            result = Math.Round(number, digits, MidpointRounding.AwayFromZero);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSignFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number == 0d ? 0 : (number > 0d ? 1 : -1);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSinFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Sin(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSqrtFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number < 0 ? null : Math.Sqrt(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalTanFunction(Func<int, object?> evalArg, out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Tan(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }
}
