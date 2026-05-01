namespace DbSqlLikeMem;

internal static partial class CommandScalarExecutionPrelude
{
    /// <summary>
    /// EN: Constant expression evaluation entry point for scalar, case, binary, and boolean helpers.
    /// PT-br: Ponto de entrada de avaliacao de expressoes constantes para helpers escalares, case, binarios e booleanos.
    /// </summary>
    internal static class Expressions
    {
        internal static bool TryEvaluateScalar(
            QueryExecutionContext context,
            SqlExpr expr,
            out object? value)
        {
            return TryEvaluateConstantScalarExpression(context, expr, out value);
        }

        internal static bool TryEvaluateCase(
            QueryExecutionContext context,
            CaseExpr expr,
            out object? value)
        {
            return TryEvaluateConstantCaseExpression(context, expr, out value);
        }

        internal static bool TryEvaluateBinary(
            QueryExecutionContext context,
            SqlBinaryOp op,
            object? left,
            object? right,
            out object? value)
        {
            return TryEvaluateConstantBinaryExpression(context, op, left, right, out value);
        }

        internal static bool TryEvaluateBooleanExpression(
            QueryExecutionContext context,
            SqlExpr expr,
            out bool value)
        {
            return TryEvaluateConstantBooleanExpression(context, expr, out value);
        }

        internal static bool TryEvaluateBoolean(
            object? value,
            out bool result)
        {
            return TryCoerceBooleanValue(value, out result);
        }

        internal static bool TryEvaluateDecimal(
            object? value,
            out decimal result)
        {
            return TryConvertDecimal(value, out result);
        }

        internal static bool TryConstantEquality(
            object? left,
            object? right)
        {
            return TryConstantEqualityCore(left, right);
        }

        private static bool TryEvaluateConstantBooleanExpression(
            QueryExecutionContext context,
            SqlExpr expr,
            out bool value)
        {
            value = false;

            if (!TryEvaluateConstantScalarExpression(context, expr, out var scalarValue))
                return false;

            return TryCoerceBooleanValue(scalarValue, out value);
        }

        private static bool TryEvaluateConstantBinaryExpression(
            QueryExecutionContext context,
            SqlBinaryOp op,
            object? left,
            object? right,
            out object? value)
        {
            value = null;

            switch (op)
            {
                case SqlBinaryOp.And:
                    return TryCombineConstantBooleans(left, right, useAnd: true, out value);
                case SqlBinaryOp.Or:
                    return TryCombineConstantBooleans(left, right, useAnd: false, out value);
                case SqlBinaryOp.Add:
                case SqlBinaryOp.Subtract:
                case SqlBinaryOp.Multiply:
                case SqlBinaryOp.Divide:
                    return TryEvaluateConstantArithmeticBinaryExpression(op, left, right, out value);
                case SqlBinaryOp.Concat:
                    return TryEvaluateConstantConcatBinaryExpression(context, left, right, out value);
                case SqlBinaryOp.Eq:
                case SqlBinaryOp.Neq:
                case SqlBinaryOp.Greater:
                case SqlBinaryOp.GreaterOrEqual:
                case SqlBinaryOp.Less:
                case SqlBinaryOp.LessOrEqual:
                case SqlBinaryOp.NullSafeEq:
                    return TryEvaluateConstantComparisonBinaryExpression(op, left, right, out value);
                default:
                    return false;
            }
        }

        private static bool TryCombineConstantBooleans(
            object? left,
            object? right,
            bool useAnd,
            out object? value)
        {
            value = null;

            if (!TryCoerceBooleanValue(left, out var leftBool) || !TryCoerceBooleanValue(right, out var rightBool))
                return false;

            value = useAnd ? leftBool && rightBool : leftBool || rightBool;
            return true;
        }

        private static bool TryEvaluateConstantArithmeticBinaryExpression(
            SqlBinaryOp op,
            object? left,
            object? right,
            out object? value)
        {
            value = null;

            if (IsNullish(left) || IsNullish(right))
                return true;

            if (!TryConvertDecimal(left, out var leftNumber) || !TryConvertDecimal(right, out var rightNumber))
                return false;

            value = op switch
            {
                SqlBinaryOp.Add => leftNumber + rightNumber,
                SqlBinaryOp.Subtract => leftNumber - rightNumber,
                SqlBinaryOp.Multiply => leftNumber * rightNumber,
                SqlBinaryOp.Divide => rightNumber == 0 ? null : leftNumber / rightNumber,
                _ => null
            };
            return true;
        }

        private static bool TryEvaluateConstantConcatBinaryExpression(
            QueryExecutionContext context,
            object? left,
            object? right,
            out object? value)
        {
            value = null;

            if (IsNullish(left) || IsNullish(right))
                return true;

            var leftText = left?.ToString() ?? string.Empty;
            var rightText = right?.ToString() ?? string.Empty;
            value = string.Concat(leftText, rightText);
            return true;
        }

        private static bool TryEvaluateConstantComparisonBinaryExpression(
            SqlBinaryOp op,
            object? left,
            object? right,
            out object? value)
        {
            value = null;

            if (op == SqlBinaryOp.NullSafeEq)
            {
                value = IsNullish(left) && IsNullish(right)
                    || (!IsNullish(left) && !IsNullish(right) && Equals(left, right));
                return true;
            }

            if (TryConvertDecimal(left, out var leftNumber) && TryConvertDecimal(right, out var rightNumber))
            {
                var comparison = leftNumber.CompareTo(rightNumber);
                value = op switch
                {
                    SqlBinaryOp.Eq => comparison == 0,
                    SqlBinaryOp.Neq => comparison != 0,
                    SqlBinaryOp.Greater => comparison > 0,
                    SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                    SqlBinaryOp.Less => comparison < 0,
                    SqlBinaryOp.LessOrEqual => comparison <= 0,
                    _ => false
                };
                return true;
            }

            if (left is string leftText && right is string rightText)
            {
                var comparison = StringComparer.OrdinalIgnoreCase.Compare(leftText, rightText);
                value = op switch
                {
                    SqlBinaryOp.Eq => comparison == 0,
                    SqlBinaryOp.Neq => comparison != 0,
                    SqlBinaryOp.Greater => comparison > 0,
                    SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                    SqlBinaryOp.Less => comparison < 0,
                    SqlBinaryOp.LessOrEqual => comparison <= 0,
                    _ => false
                };
                return true;
            }

            if (left is IComparable leftComparable && right is not null && left.GetType() == right.GetType())
            {
                var comparison = leftComparable.CompareTo(right);
                value = op switch
                {
                    SqlBinaryOp.Eq => comparison == 0,
                    SqlBinaryOp.Neq => comparison != 0,
                    SqlBinaryOp.Greater => comparison > 0,
                    SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                    SqlBinaryOp.Less => comparison < 0,
                    SqlBinaryOp.LessOrEqual => comparison <= 0,
                    _ => false
                };
                return true;
            }

            return false;
        }
    }
}
