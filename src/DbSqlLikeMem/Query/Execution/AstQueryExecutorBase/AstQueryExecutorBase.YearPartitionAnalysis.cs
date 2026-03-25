namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private bool TryCollectYearBound(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
        {
            if (!TryCollectYearBound(andExpr.Left, src, partitionedColumnName, out var leftLow, out var leftHigh))
                return false;

            if (!TryCollectYearBound(andExpr.Right, src, partitionedColumnName, out var rightLow, out var rightHigh))
                return false;

            lowerBound = MaxNullable(leftLow, rightLow);
            upperBound = MinNullable(leftHigh, rightHigh);
            return lowerBound.HasValue && upperBound.HasValue && lowerBound.Value <= upperBound.Value;
        }

        if (expr is BinaryExpr orExpr && orExpr.Op == SqlBinaryOp.Or)
        {
            var leftRanges = new List<(int? Low, int? High)>();
            var rightRanges = new List<(int? Low, int? High)>();
            var leftOk = TryCollectYearBound(orExpr.Left, src, partitionedColumnName, out var leftLow, out var leftHigh);
            var rightOk = TryCollectYearBound(orExpr.Right, src, partitionedColumnName, out var rightLow, out var rightHigh);

            if (leftOk && leftLow.HasValue && leftHigh.HasValue)
                leftRanges.Add((leftLow, leftHigh));
            if (rightOk && rightLow.HasValue && rightHigh.HasValue)
                rightRanges.Add((rightLow, rightHigh));

            if (leftRanges.Count == 0 || rightRanges.Count == 0)
                return false;

            lowerBound = MinNullable(leftRanges[0].Low, rightRanges[0].Low);
            upperBound = MaxNullable(leftRanges[0].High, rightRanges[0].High);
            return lowerBound.HasValue && upperBound.HasValue;
        }

        if (TryResolveYearComparisonBound(expr, src, partitionedColumnName, out var low, out var high))
        {
            lowerBound = low;
            upperBound = high;
            return true;
        }

        if (expr is BetweenExpr between
            && !between.Negated
            && (TryGetYearPartitionFunctionInfo(between.Expr, src, partitionedColumnName, out _)
                || TryResolveColumnName(between.Expr, src, out var columnName)
                    && string.Equals(columnName, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
            && TryResolveConstantValue(between.Low, out var lowValue)
            && TryResolveConstantValue(between.High, out var highValue)
            && TryResolvePartitionYearConstant(lowValue, out var lowYear)
            && TryResolvePartitionYearConstant(highValue, out var highYear)
            && lowYear <= highYear)
        {
            lowerBound = lowYear;
            upperBound = highYear;
            return true;
        }

        return false;
    }

    private bool TryResolveYearComparisonBound(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        if (expr is not BinaryExpr cmp
            || cmp.Op is not (SqlBinaryOp.Greater or SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less or SqlBinaryOp.LessOrEqual))
        {
            return false;
        }

        if (TryGetYearPartitionFunctionInfo(cmp.Left, src, partitionedColumnName, out _)
            && TryResolveConstantValue(cmp.Right, out var rightValue)
            && TryResolvePartitionYearConstant(rightValue, out var rightYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, rightYear, functionOnLeft: true, out lowerBound, out upperBound);
        }

        if (TryResolveColumnName(cmp.Left, src, out var leftColumn)
            && string.Equals(leftColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase)
            && TryResolveConstantValue(cmp.Right, out rightValue)
            && TryResolvePartitionYearConstant(rightValue, out rightYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, rightYear, functionOnLeft: true, out lowerBound, out upperBound);
        }

        if (TryGetYearPartitionFunctionInfo(cmp.Right, src, partitionedColumnName, out _)
            && TryResolveConstantValue(cmp.Left, out var leftValue)
            && TryResolvePartitionYearConstant(leftValue, out var leftYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, leftYear, functionOnLeft: false, out lowerBound, out upperBound);
        }

        if (TryResolveColumnName(cmp.Right, src, out var rightColumn)
            && string.Equals(rightColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase)
            && TryResolveConstantValue(cmp.Left, out leftValue)
            && TryResolvePartitionYearConstant(leftValue, out leftYear))
        {
            return TryBuildYearComparisonBound(cmp.Op, leftYear, functionOnLeft: false, out lowerBound, out upperBound);
        }

        return false;
    }

    private static bool TryBuildYearComparisonBound(
        SqlBinaryOp op,
        int year,
        bool functionOnLeft,
        out int? lowerBound,
        out int? upperBound)
    {
        lowerBound = null;
        upperBound = null;

        const int MinYear = 1;
        const int MaxYear = 9999;

        static int ClampYear(int value)
            => value < MinYear ? MinYear : value > MaxYear ? MaxYear : value;

        int LowerFromGreater() => ClampYear(year + 1);
        int UpperFromLess() => ClampYear(year - 1);

        if (functionOnLeft)
        {
            switch (op)
            {
                case SqlBinaryOp.Greater:
                    lowerBound = LowerFromGreater();
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.GreaterOrEqual:
                    lowerBound = ClampYear(year);
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.Less:
                    lowerBound = MinYear;
                    upperBound = UpperFromLess();
                    return true;
                case SqlBinaryOp.LessOrEqual:
                    lowerBound = MinYear;
                    upperBound = ClampYear(year);
                    return true;
            }
        }
        else
        {
            switch (op)
            {
                case SqlBinaryOp.Greater:
                    lowerBound = MinYear;
                    upperBound = UpperFromLess();
                    return true;
                case SqlBinaryOp.GreaterOrEqual:
                    lowerBound = MinYear;
                    upperBound = ClampYear(year);
                    return true;
                case SqlBinaryOp.Less:
                    lowerBound = LowerFromGreater();
                    upperBound = MaxYear;
                    return true;
                case SqlBinaryOp.LessOrEqual:
                    lowerBound = ClampYear(year);
                    upperBound = MaxYear;
                    return true;
            }
        }

        return false;
    }

    private static int? MaxNullable(int? left, int? right)
        => left.HasValue && right.HasValue
            ? Math.Max(left.Value, right.Value)
            : left ?? right;

    private static int? MinNullable(int? left, int? right)
        => left.HasValue && right.HasValue
            ? Math.Min(left.Value, right.Value)
            : left ?? right;

    private bool TryGetPartitionValue(
        SqlExpr maybeColumn,
        SqlExpr maybeValue,
        Source src,
        string partitionedColumnName,
        out object? value)
    {
        value = null;
        if (!TryResolveColumnName(maybeColumn, src, out var resolvedColumn)
            || !string.Equals(resolvedColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!TryResolveConstantValue(maybeValue, out value))
            return false;

        return true;
    }

    private bool TryGetColumnAndValue(
        SqlExpr maybeColumn,
        SqlExpr maybeValue,
        Source src,
        out string column,
        out object? value)
    {
        column = "";
        value = null;

        if (!TryResolveColumnName(maybeColumn, src, out var resolvedColumn))
            return false;

        if (!TryResolveConstantValue(maybeValue, out value))
            return false;

        column = resolvedColumn;
        return true;
    }

    private static bool TryResolveColumnName(
        SqlExpr expr,
        Source src,
        out string column)
    {
        column = "";

        switch (expr)
        {
            case IdentifierExpr id:
                {
                    var dot = id.Name.IndexOf('.');
                    if (dot < 0)
                    {
                        column = id.Name.NormalizeName();
                        return true;
                    }

                    var qualifier = id.Name[..dot].NormalizeName();
                    var sourceAlias = src.Alias.NormalizeName();
                    var sourceName = src.Name.NormalizeName();
                    if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                        && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                        return false;

                    column = id.Name[(dot + 1)..].NormalizeName();
                    return true;
                }

            case ColumnExpr col:
                {
                    if (!string.IsNullOrWhiteSpace(col.Qualifier))
                    {
                        var qualifier = col.Qualifier.NormalizeName();
                        var sourceAlias = src.Alias.NormalizeName();
                        var sourceName = src.Name.NormalizeName();
                        if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                            && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }

                    column = col.Name.NormalizeName();
                    return true;
                }

            default:
                return false;
        }
    }

    private bool TryResolveConstantValue(
        SqlExpr expr,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr l:
                value = l.Value;
                return true;
            case ParameterExpr p:
                value = ResolveParam(p.Name);
                return true;
            default:
                value = null;
                return false;
        }
    }
}
