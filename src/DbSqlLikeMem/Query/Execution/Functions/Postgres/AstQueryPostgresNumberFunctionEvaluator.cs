using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryPostgresNumberFunctionEvaluator
{
    internal static bool TryEvalToNumberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TO_NUMBER", StringComparison.OrdinalIgnoreCase))
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

        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            result = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (value is decimal or double or float)
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrWhiteSpace(text))
        {
            result = null;
            return true;
        }

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integerValue))
        {
            result = integerValue;
            return true;
        }

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
        {
            result = decimalValue;
            return true;
        }

        result = null;
        return true;
    }
}
