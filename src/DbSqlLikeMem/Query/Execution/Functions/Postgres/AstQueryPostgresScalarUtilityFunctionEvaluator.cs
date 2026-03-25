namespace DbSqlLikeMem;

using System;
using System.Globalization;
using System.Linq;

internal static class AstQueryPostgresScalarUtilityFunctionEvaluator
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
        if (name is "NUM_NULLS")
        {
            result = Enumerable.Range(0, fn.Args.Count).Count(i => AstQueryExecutorBase.IsNullish(evalArg(i)));
            return true;
        }

        if (name is "NUM_NONNULLS")
        {
            result = Enumerable.Range(0, fn.Args.Count).Count(i => !AstQueryExecutorBase.IsNullish(evalArg(i)));
            return true;
        }

        if (name is "LCM")
        {
            if (fn.Args.Count < 2)
            {
                result = null;
                return true;
            }

            var leftValue = evalArg(0);
            var rightValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(leftValue) || AstQueryExecutorBase.IsNullish(rightValue))
            {
                result = null;
                return true;
            }

            var left = Math.Abs(Convert.ToInt64(leftValue.ToDec(), CultureInfo.InvariantCulture));
            var right = Math.Abs(Convert.ToInt64(rightValue.ToDec(), CultureInfo.InvariantCulture));
            if (left == 0 || right == 0)
            {
                result = 0L;
                return true;
            }

            result = checked((left / AstQueryGeneralScalarFunctionEvaluator.ComputeGreatestCommonDivisor(left, right)) * right);
            return true;
        }

        if (name is "MIN_SCALE")
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

            result = AstQueryGeneralScalarFunctionEvaluator.GetMinimumNumericScale(value!);
            return true;
        }

        if (name is "PARSE_IDENT")
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

            var text = value?.ToString() ?? string.Empty;
            if (!AstQueryGeneralScalarFunctionEvaluator.TryParsePostgresIdentifierParts(text, out var parts))
            {
                result = null;
                return true;
            }

            result = parts.ToArray();
            return true;
        }

        result = null;
        return false;
    }
}
