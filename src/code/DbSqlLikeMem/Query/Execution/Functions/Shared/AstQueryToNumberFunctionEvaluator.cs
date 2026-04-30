namespace DbSqlLikeMem;

internal static class AstQueryToNumberFunctionEvaluator
{
    internal static bool TryEvalToNumberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "TO_NUMBER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "DEC", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (value is string numberText)
        {
            var mask = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
            if (AstQueryFormatFunctionHelper.TryParseOracleNumber(numberText, mask, out var parsedNumber))
            {
                result = parsedNumber;
                return true;
            }
        }

        result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        return true;
    }
}
