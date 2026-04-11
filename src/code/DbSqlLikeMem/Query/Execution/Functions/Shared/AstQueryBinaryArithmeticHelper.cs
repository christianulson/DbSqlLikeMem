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

        if (TryEvalIntegerArithmeticBinary(op, left, right, out result))
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

    private static bool TryEvalIntegerArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        result = null;

        if (!TryConvertBinaryArithmeticOperandToInt64(left, out var leftNumber)
            || !TryConvertBinaryArithmeticOperandToInt64(right, out var rightNumber))
        {
            return false;
        }

        try
        {
            result = op switch
            {
                SqlBinaryOp.Add => checked(leftNumber + rightNumber),
                SqlBinaryOp.Subtract => checked(leftNumber - rightNumber),
                SqlBinaryOp.Multiply => checked(leftNumber * rightNumber),
                SqlBinaryOp.Divide => rightNumber == 0
                    ? null
                    : Convert.ToDecimal(leftNumber, CultureInfo.InvariantCulture) / Convert.ToDecimal(rightNumber, CultureInfo.InvariantCulture),
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
            return true;
        }
        catch (OverflowException)
        {
            var leftDouble = Convert.ToDouble(left, CultureInfo.InvariantCulture);
            var rightDouble = Convert.ToDouble(right, CultureInfo.InvariantCulture);
            result = op switch
            {
                SqlBinaryOp.Add => leftDouble + rightDouble,
                SqlBinaryOp.Subtract => leftDouble - rightDouble,
                SqlBinaryOp.Multiply => leftDouble * rightDouble,
                SqlBinaryOp.Divide => rightDouble == 0d ? null : leftDouble / rightDouble,
                _ => throw new InvalidOperationException("op aritmético inválido")
            };
            return true;
        }
    }

    internal static bool TryEvalDateIntervalArithmeticBinary(
        SqlBinaryOp op,
        object left,
        object right,
        out object? result)
    {
        result = null;

        if (op is not (SqlBinaryOp.Add or SqlBinaryOp.Subtract))
            return false;

        if (TryConvertNumericToDouble(left, out _))
            return false;

        if (!AstQueryExecutionRuntimeHelper.TryCoerceDateTime(left, out var dateTime))
            return false;

        if (right is IntervalValue interval)
        {
            result = op == SqlBinaryOp.Add
                ? dateTime.Add(interval.Span)
                : dateTime.Subtract(interval.Span);
            return true;
        }

        if (AstQueryExecutionRuntimeHelper.TryCoerceDateTime(right, out var rightDateTime) && op == SqlBinaryOp.Subtract)
        {
            result = (decimal)(dateTime.Date - rightDateTime.Date).TotalDays;
            return true;
        }

        if (!TryConvertNumericToDouble(right, out var dayOffset))
            return false;

        if (op == SqlBinaryOp.Add)
        {
            result = dateTime.AddDays(dayOffset);
            return true;
        }

        if (op == SqlBinaryOp.Subtract)
        {
            result = dateTime.AddDays(-dayOffset);
            return true;
        }

        result = null;
        return false;
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

    private static bool TryConvertBinaryArithmeticOperandToInt64(object value, out long result)
    {
        switch (value)
        {
            case sbyte sb:
                result = sb;
                return true;
            case byte b:
                result = b;
                return true;
            case short s:
                result = s;
                return true;
            case ushort us:
                result = us;
                return true;
            case int i:
                result = i;
                return true;
            case uint ui:
                result = ui;
                return true;
            case long l:
                result = l;
                return true;
            case ulong ul when ul <= long.MaxValue:
                result = (long)ul;
                return true;
            case bool b:
                result = b ? 1 : 0;
                return true;
            case string text when long.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out result):
                return true;
            default:
                result = default;
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
