namespace DbSqlLikeMem;

internal static class AstQueryComparisonSupport
{
    internal static int CompareSql(object? a, object? b, QueryExecutionContext context)
    {
        if (AstQueryBinarySupportHelper.IsSqlNullLike(a) && AstQueryBinarySupportHelper.IsSqlNullLike(b))
            return 0;

        if (AstQueryBinarySupportHelper.IsSqlNullLike(a))
            return -1;

        if (AstQueryBinarySupportHelper.IsSqlNullLike(b))
            return 1;

        return a!.Compare(b!, context);
    }

    internal static SqlBinaryOp ReverseComparisonOperator(SqlBinaryOp op)
        => op switch
        {
            SqlBinaryOp.Greater => SqlBinaryOp.Less,
            SqlBinaryOp.GreaterOrEqual => SqlBinaryOp.LessOrEqual,
            SqlBinaryOp.Less => SqlBinaryOp.Greater,
            SqlBinaryOp.LessOrEqual => SqlBinaryOp.GreaterOrEqual,
            _ => op
        };

    internal static bool TryGetDecimalLiteral(SqlExpr expr, out decimal value)
    {
        value = default;

        if (expr is not LiteralExpr literal)
            return false;

        if (literal.Value is decimal decimalValue)
        {
            value = decimalValue;
            return true;
        }

        if (literal.Value is IConvertible convertible)
        {
            try
            {
                value = Convert.ToDecimal(convertible, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }
}
