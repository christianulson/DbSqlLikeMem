namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private object? EvalLike(
        LikeExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var left = Eval(expression.Left, row, group, ctes)?.ToString() ?? string.Empty;
        var pattern = Eval(expression.Pattern, row, group, ctes)?.ToString() ?? string.Empty;
        var escape = expression.Escape is null
            ? null
            : Eval(expression.Escape, row, group, ctes)?.ToString();
        return left.Like(pattern, context, escape, expression.CaseInsensitive ? true : null);
    }

    private object? EvalNot(
        UnaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (expression.Expr is InExpr notInExpression)
            return EvalNotIn(notInExpression, row, group, ctes);

        return !Eval(expression.Expr, row, group, ctes).ToBool();
    }

    private object? EvalJsonAccess(
        JsonAccessExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (!Dialect!.SupportsJsonArrowOperators)
            throw SqlUnsupported.ForDialect(Dialect, "JSON -> / ->> / #> / #>> operators");

        var mapped = MapJsonAccess(expression);
        return Eval(mapped, row, group, ctes);
    }

    private object?[] EvalRowExpression(
        RowExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => [.. expression.Items.Select(item => Eval(item, row, group, ctes))];

    private object? EvalBetween(
        BetweenExpr b,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var ge = new BinaryExpr(SqlBinaryOp.GreaterOrEqual, b.Expr, b.Low);
        var le = new BinaryExpr(SqlBinaryOp.LessOrEqual, b.Expr, b.High);
        var and = new BinaryExpr(SqlBinaryOp.And, ge, le);

        var res = Eval(and, row, group, ctes);

        if (b.Negated)
            return res is null ? null : !(bool)res;

        return res;
    }

    private object? EvalBinary(
        BinaryExpr b,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (TryEvalLogicalBinary(b, row, group, ctes, out var logicalResult))
            return logicalResult;

        if (TryEvaluateCorrelatedCountComparisonFast(b, row, ctes, out var countComparisonResult))
            return countComparisonResult;

        var l = Eval(b.Left, row, group, ctes);
        var r = Eval(b.Right, row, group, ctes);

        if (TryEvalConcatBinary(b.Op, l, r, out var concatResult))
            return concatResult;

        if (TryEvalArithmeticBinary(b.Op, l, r, out var arithmeticResult))
            return arithmeticResult;

        if (TryEvalNullSafeEqualityBinary(b.Op, l, r, out var nullSafeEqualityResult))
            return nullSafeEqualityResult;

        if (l is null || l is DBNull || r is null || r is DBNull)
            return false;

        return EvalComparisonBinary(b.Op, l, r);
    }

    private bool TryEvalLogicalBinary(
        BinaryExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = expression.Op switch
        {
            SqlBinaryOp.And => Eval(expression.Left, row, group, ctes).ToBool()
                && Eval(expression.Right, row, group, ctes).ToBool(),
            SqlBinaryOp.Or => Eval(expression.Left, row, group, ctes).ToBool()
                || Eval(expression.Right, row, group, ctes).ToBool(),
            _ => null
        };

        return expression.Op is SqlBinaryOp.And or SqlBinaryOp.Or;
    }

    private static bool TryEvalArithmeticBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op is not (SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide))
        {
            result = null;
            return false;
        }

        if (left is null || right is null)
        {
            result = null;
            return true;
        }

        if (TryEvalDateIntervalArithmeticBinary(op, left, right, out result))
            return true;

        var leftNumber = ConvertBinaryArithmeticOperandToDecimal(left);
        var rightNumber = ConvertBinaryArithmeticOperandToDecimal(right);
        result = op switch
        {
            SqlBinaryOp.Add => leftNumber + rightNumber,
            SqlBinaryOp.Subtract => leftNumber - rightNumber,
            SqlBinaryOp.Multiply => leftNumber * rightNumber,
            SqlBinaryOp.Divide => rightNumber == 0m ? null : leftNumber / rightNumber,
            _ => throw new InvalidOperationException("op aritmético inválido")
        };
        return true;
    }

    private bool TryEvalConcatBinary(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? result)
    {
        if (op != SqlBinaryOp.Concat)
        {
            result = null;
            return false;
        }

        var nullInputReturnsNull = Dialect?.ConcatReturnsNullOnNullInput ?? true;
        if (left is null or DBNull || right is null or DBNull)
        {
            if (nullInputReturnsNull)
            {
                result = null;
                return true;
            }
        }

        var leftText = left is null or DBNull ? string.Empty : left.ToString() ?? string.Empty;
        var rightText = right is null or DBNull ? string.Empty : right.ToString() ?? string.Empty;
        result = string.Concat(leftText, rightText);
        return true;
    }

    private static bool TryEvalDateIntervalArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        result = null;

        if (!TryCoerceDateTime(left, out var dateTime))
            return false;

        if (right is IntervalValue interval)
        {
            result = op switch
            {
                SqlBinaryOp.Add => dateTime.Add(interval.Span),
                SqlBinaryOp.Subtract => dateTime.Subtract(interval.Span),
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
            return true;
        }

        if (TryCoerceDateTime(right, out var rightDateTime) && op == SqlBinaryOp.Subtract)
        {
            result = (decimal)(dateTime.Date - rightDateTime.Date).TotalDays;
            return true;
        }

        if (!TryConvertNumericToDouble(right, out var dayOffset))
            return false;

        result = op switch
        {
            SqlBinaryOp.Add => dateTime.AddDays(dayOffset),
            SqlBinaryOp.Subtract => dateTime.AddDays(-dayOffset),
            _ => throw new InvalidOperationException("op aritmético inválido")
        };
        return true;
    }

    internal static bool TryConvertNumericToDouble(object? value, out double result)
    {
        result = 0d;
        if (value is null || value is DBNull)
            return false;

        try
        {
            result = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    internal static bool TryConvertNumericToDecimal(object? value, out decimal result)
    {
        result = 0m;
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

    private static decimal ConvertBinaryArithmeticOperandToDecimal(object value)
    {
        if (value is decimal decimalValue) return decimalValue;
        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
            return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        if (value is float singleValue) return Convert.ToDecimal(singleValue, CultureInfo.InvariantCulture);
        if (value is double doubleValue) return Convert.ToDecimal(doubleValue, CultureInfo.InvariantCulture);
        if (value is string text
            && decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedValue))
            return parsedValue;

        throw new InvalidOperationException($"Não consigo converter '{value}' para número.");
    }

    private bool TryEvalNullSafeEqualityBinary(
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

        result = left.Compare(right, _context) == 0;
        return true;
    }

    private bool EvalComparisonBinary(SqlBinaryOp op, object left, object right)
    {
        var comparison = left.Compare(right, _context);

        return op switch
        {
            SqlBinaryOp.Eq => comparison == 0,
            SqlBinaryOp.Neq => comparison != 0,
            SqlBinaryOp.Greater => comparison > 0,
            SqlBinaryOp.GreaterOrEqual => comparison >= 0,
            SqlBinaryOp.Less => comparison < 0,
            SqlBinaryOp.LessOrEqual => comparison <= 0,
            SqlBinaryOp.Regexp => EvalRegexp(left, right, Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para REGEXP.")),
            SqlBinaryOp.SoundLike => EvalSoundLike(left, right),
            _ => throw new InvalidOperationException($"Binary op não suportado: {op}")
        };
    }

    private static bool EvalSoundLike(object left, object right)
    {
        var leftSoundex = ComputeSoundex(left.ToString() ?? string.Empty);
        var rightSoundex = ComputeSoundex(right.ToString() ?? string.Empty);
        return leftSoundex == rightSoundex;
    }

    private static bool EvalRegexp(object l, object r, ISqlDialect dialect)
    {
        try
        {
            var options = RegexOptions.CultureInvariant;
            if (dialect.RegexIsCaseInsensitive)
                options |= RegexOptions.IgnoreCase;

            return Regex.IsMatch(l.ToString() ?? "", r.ToString() ?? "", options);
        }
        catch (ArgumentException)
        {
            if (dialect.RegexInvalidPatternEvaluatesToFalse)
                return false;
            throw;
        }
    }

    private static bool IsSqlNullLike(object? value)
        => value is null or DBNull;

    /// <summary>
    /// EN: Checks whether an object array contains at least one SQL NULL-like value.
    /// PT: Verifica se um array de objetos contém ao menos um valor SQL nulo.
    /// </summary>
    private static bool HasNullElement(object?[] values)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (values[i] is null or DBNull)
                return true;
        }

        return false;
    }
}
