using System.Globalization;

namespace DbSqlLikeMem;

internal static class AstQueryBinaryExpressionHelper
{
    internal static bool TryEvalConcatBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        ISqlDialect? dialect,
        out object? result)
    {
        if (op != SqlBinaryOp.Concat)
        {
            result = null;
            return false;
        }

        var nullInputReturnsNull = dialect?.ConcatReturnsNullOnNullInput ?? true;
        if (left is null or DBNull || right is null or DBNull)
        {
            if (nullInputReturnsNull)
            {
                result = null;
                return true;
            }
        }

        var leftText = left is null or DBNull ? string.Empty : Convert.ToString(left, CultureInfo.InvariantCulture) ?? string.Empty;
        var rightText = right is null or DBNull ? string.Empty : Convert.ToString(right, CultureInfo.InvariantCulture) ?? string.Empty;
        result = string.Concat(leftText, rightText);
        return true;
    }

    internal static bool TryEvalNullSafeEqualityBinary(
        this QueryExecutionContext context,
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op != SqlBinaryOp.NullSafeEq)
        {
            result = null;
            return false;
        }

        if (left is null && right is null)
        {
            result = true;
            return true;
        }

        if (left is null || right is null)
        {
            result = false;
            return true;
        }

        result = context.Compare(left, right) == 0;
        return true;
    }

    internal static bool EvalComparisonBinary(
        this QueryExecutionContext context,
        SqlBinaryOp op,
        object left,
        object right)
    {
        var comparison = context.Compare(left, right);

        return op switch
        {
            SqlBinaryOp.Eq => comparison == 0,
            SqlBinaryOp.Neq => comparison != 0,
            SqlBinaryOp.Greater => comparison > 0,
            SqlBinaryOp.GreaterOrEqual => comparison >= 0,
            SqlBinaryOp.Less => comparison < 0,
            SqlBinaryOp.LessOrEqual => comparison <= 0,
            SqlBinaryOp.Regexp => AstQueryBinarySupportHelper.EvalRegexp(left, right, context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para REGEXP.")),
            SqlBinaryOp.SoundLike => AstQueryBinarySupportHelper.EvalSoundLike(left, right),
            _ => throw new InvalidOperationException($"Binary op não suportado: {op}")
        };
    }
}
