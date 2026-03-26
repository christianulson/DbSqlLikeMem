namespace DbSqlLikeMem;

internal static class AstQueryBinaryArithmeticHelper
{
    internal static bool TryEvalArithmeticBinary(
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

    internal static bool TryEvalDateIntervalArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        result = null;

        if (!AstQueryExecutionRuntimeHelper.TryCoerceDateTime(left, out var dateTime))
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

        if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(right, out var rightDateTime) && op == SqlBinaryOp.Subtract)
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
}
