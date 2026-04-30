using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryPartitionHelper(
    Func<SqlExpr, (bool Success, object? Value)> resolveConstantValue)
{
    private readonly Func<SqlExpr, (bool Success, object? Value)> _resolveConstantValue = resolveConstantValue;

    internal Source ApplyPartitionPruning(Source src, SqlExpr? where)
    {
        if (where is null || src.Physical is not TableMock tableMock)
            return src;

        if (TryCollectColumnEqualities(where, src, out var equalsByColumn)
            && equalsByColumn.Count > 0
            && tableMock.TryInferRequestedPartitionNames(equalsByColumn, out var eqPartitions))
        {
            return src.WithRequestedPartitions(eqPartitions);
        }

        if (TryCollectPartitionCandidateValues(where, src, tableMock, out var partitionCandidateValues)
            && tableMock.TryInferRequestedPartitionNames(partitionCandidateValues, out var inferredPartitions))
        {
            return src.WithRequestedPartitions(inferredPartitions);
        }

        if (TryCollectPartitionYearBoundRanges(where, src, tableMock, out var yearBoundRanges)
            && tableMock.TryInferRequestedPartitionNamesForRanges(yearBoundRanges, out var yearBoundPartitions))
        {
            return src.WithRequestedPartitions(yearBoundPartitions);
        }

        if (TryCollectPartitionRangeValues(where, src, tableMock, out var rangeValues)
            && tableMock.TryInferRequestedPartitionNamesForRanges(rangeValues, out var rangePartitions))
        {
            return src.WithRequestedPartitions(rangePartitions);
        }

        return src;
    }

    internal bool TryCollectColumnEqualities(
        SqlExpr where,
        Source src,
        out Dictionary<string, object?> equalsByColumn)
    {
        equalsByColumn = new Dictionary<string, object?>(4, StringComparer.OrdinalIgnoreCase);
        return Walk(where, ref equalsByColumn);

        bool Walk(SqlExpr expr, ref Dictionary<string, object?> eqCol)
        {
            if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
                return Walk(andExpr.Left, ref eqCol) && Walk(andExpr.Right, ref eqCol);

            if (expr is not BinaryExpr eq || eq.Op != SqlBinaryOp.Eq)
                return false;

            if (TryGetColumnAndValue(eq.Left, eq.Right, src, out var column, out var value)
                || TryGetColumnAndValue(eq.Right, eq.Left, src, out column, out value))
            {
                eqCol[column] = value;
                return true;
            }

            return false;
        }
    }

    private bool TryCollectPartitionCandidateValues(
        SqlExpr where,
        Source src,
        TableMock tableMock,
        out List<object?> rawValues)
    {
        rawValues = new List<object?>(4);
        if (!tableMock.TryGetPartitionedColumnName(out var partitionedColumnName))
            return false;

        return Walk(where, ref rawValues);

        bool Walk(SqlExpr expr, ref List<object?> values)
        {
            if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
            {
                var leftValues = new List<object?>(4);
                var rightValues = new List<object?>(4);
                var leftOk = Walk(andExpr.Left, ref leftValues);
                var rightOk = Walk(andExpr.Right, ref rightValues);

                if (leftOk)
                    values.AddRange(leftValues);
                if (rightOk)
                    values.AddRange(rightValues);
                return leftOk || rightOk;
            }

            if (expr is BinaryExpr orExpr && orExpr.Op == SqlBinaryOp.Or)
            {
                var leftValues = new List<object?>(4);
                var rightValues = new List<object?>(4);
                var leftOk = Walk(orExpr.Left, ref leftValues);
                var rightOk = Walk(orExpr.Right, ref rightValues);

                if (leftOk && rightOk)
                {
                    values.AddRange(leftValues);
                    values.AddRange(rightValues);
                    return true;
                }

                return false;
            }

            if (TryCollectPartitionValues(expr, src, partitionedColumnName, out var localValues))
            {
                values.AddRange(localValues);
                return true;
            }

            return false;
        }
    }

    private bool TryCollectPartitionValues(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out List<object?> values)
    {
        values = new List<object?>(1);

        if (expr is BinaryExpr eq && eq.Op == SqlBinaryOp.Eq)
        {
            if (TryGetPartitionValue(eq.Left, eq.Right, src, partitionedColumnName, out var value)
                || TryGetPartitionValue(eq.Right, eq.Left, src, partitionedColumnName, out value))
            {
                values.Add(value);
                return true;
            }

            if (TryGetPartitionYearFunctionValue(eq.Left, eq.Right, src, partitionedColumnName, out value)
                || TryGetPartitionYearFunctionValue(eq.Right, eq.Left, src, partitionedColumnName, out value))
            {
                values.Add(value);
                return true;
            }

            return false;
        }

        if (expr is InExpr inExpr)
        {
            if (!TryResolveColumnName(inExpr.Left, src, out var columnName)
                || !string.Equals(columnName, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
            {
                if (!TryGetYearPartitionFunctionInfo(inExpr.Left, src, partitionedColumnName, out _))
                    return false;
            }

            foreach (var item in inExpr.Items)
            {
                if (!TryResolveConstantValue(item, out var itemValue))
                    return false;

                values.Add(itemValue);
            }

            return values.Count > 0;
        }

        return false;
    }

    internal static bool TryResolvePartitionYearConstant(object? rawValue, out int year)
    {
        switch (rawValue)
        {
            case DateTime dateTime:
                year = dateTime.Year;
                return true;
            case DateTimeOffset dateTimeOffset:
                year = dateTimeOffset.Year;
                return true;
            case int intValue:
                year = intValue;
                return true;
            case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                year = (int)longValue;
                return true;
            case decimal decimalValue when decimalValue >= int.MinValue && decimalValue <= int.MaxValue:
                year = (int)decimalValue;
                return true;
            default:
                if (rawValue is string text)
                {
                    if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate))
                    {
                        year = parsedDate.Year;
                        return true;
                    }

                    if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedYear))
                    {
                        year = parsedYear;
                        return true;
                    }
                }

                year = default;
                return false;
        }
    }

    private bool TryGetPartitionYearFunctionValue(
        SqlExpr maybeFunction,
        SqlExpr maybeValue,
        Source src,
        string partitionedColumnName,
        out object? value)
    {
        value = null;
        if (!TryGetYearPartitionFunctionInfo(maybeFunction, src, partitionedColumnName, out _))
            return false;

        if (!TryResolveConstantValue(maybeValue, out value))
            return false;

        return true;
    }

    internal bool TryGetYearPartitionFunctionInfo(
        SqlExpr expr,
        Source src,
        string partitionedColumnName,
        out SqlExpr columnExpr)
    {
        columnExpr = null!;
        if (expr is FunctionCallExpr yearFn
            && yearFn.Name.Equals(SqlConst.YEAR, StringComparison.OrdinalIgnoreCase)
            && yearFn.Args.Count == 1)
        {
            if (!TryResolveColumnName(yearFn.Args[0], src, out var resolvedColumn)
                || !string.Equals(resolvedColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            columnExpr = yearFn.Args[0];
            return true;
        }

        if (expr is CallExpr extractCall
            && extractCall.Name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase)
            && extractCall.Args.Count == 2
            && extractCall.Args[0] is RawSqlExpr unitSql
            && unitSql.Sql.Equals(SqlConst.YEAR, StringComparison.OrdinalIgnoreCase))
        {
            if (!TryResolveColumnName(extractCall.Args[1], src, out var resolvedColumn)
                || !string.Equals(resolvedColumn, partitionedColumnName, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            columnExpr = extractCall.Args[1];
            return true;
        }

        return false;
    }

    private bool TryCollectPartitionRangeValues(
        SqlExpr where,
        Source src,
        TableMock tableMock,
        out List<(object? Low, object? High)> rangeValues)
    {
        rangeValues = new List<(object? Low, object? High)>(4);
        if (!tableMock.TryGetPartitionedColumnName(out var partitionedColumnName))
            return false;

        return Walk(where, ref rangeValues);

        bool Walk(SqlExpr expr, ref List<(object? Low, object? High)> values)
        {
            if (expr is BinaryExpr andExpr && andExpr.Op == SqlBinaryOp.And)
            {
                var leftValues = new List<(object? Low, object? High)>(4);
                var rightValues = new List<(object? Low, object? High)>(4);
                var leftOk = Walk(andExpr.Left, ref leftValues);
                var rightOk = Walk(andExpr.Right, ref rightValues);

                if (leftOk)
                    values.AddRange(leftValues);
                if (rightOk)
                    values.AddRange(rightValues);
                return leftOk || rightOk;
            }

            if (expr is BinaryExpr orExpr && orExpr.Op == SqlBinaryOp.Or)
            {
                var leftValues = new List<(object? Low, object? High)>(4);
                var rightValues = new List<(object? Low, object? High)>(4);
                var leftOk = Walk(orExpr.Left, ref leftValues);
                var rightOk = Walk(orExpr.Right, ref rightValues);

                if (leftOk && rightOk)
                {
                    values.AddRange(leftValues);
                    values.AddRange(rightValues);
                    return true;
                }

                return false;
            }

            if (expr is BetweenExpr between
                && (TryResolveColumnName(between.Expr, src, out var columnName)
                    && string.Equals(columnName, partitionedColumnName, StringComparison.OrdinalIgnoreCase)
                    || TryGetYearPartitionFunctionInfo(between.Expr, src, partitionedColumnName, out _))
                && TryResolveConstantValue(between.Low, out var lowValue)
                && TryResolveConstantValue(between.High, out var highValue)
                && !between.Negated)
            {
                values.Add((lowValue, highValue));
                return true;
            }

            return false;
        }
    }

    private bool TryCollectPartitionYearBoundRanges(
        SqlExpr where,
        Source src,
        TableMock tableMock,
        out List<(object? Low, object? High)> ranges)
    {
        ranges = new List<(object? Low, object? High)>(4);
        if (!tableMock.TryGetPartitionedColumnName(out var partitionedColumnName))
            return false;

        return Walk(where, ref ranges);

        bool Walk(SqlExpr expr, ref List<(object? Low, object? High)> values)
        {
            if (expr is BinaryExpr orExpr && orExpr.Op == SqlBinaryOp.Or)
            {
                var leftValues = new List<(object? Low, object? High)>(4);
                var rightValues = new List<(object? Low, object? High)>(4);
                var leftOk = Walk(orExpr.Left, ref leftValues);
                var rightOk = Walk(orExpr.Right, ref rightValues);

                if (!leftOk || !rightOk)
                    return false;

                values.AddRange(leftValues);
                values.AddRange(rightValues);
                return values.Count > 0;
            }

            if (!TryCollectYearBound(expr, src, partitionedColumnName, out var lowerBound, out var upperBound))
                return false;

            if (!lowerBound.HasValue || !upperBound.HasValue || lowerBound.Value > upperBound.Value)
                return false;

            values.Add((lowerBound.Value, upperBound.Value));
            return true;
        }
    }

    internal bool TryCollectYearBound(
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
            var leftRanges = new List<(int? Low, int? High)>(2);
            var rightRanges = new List<(int? Low, int? High)>(2);
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
        var resolved = _resolveConstantValue(expr);
        value = resolved.Value;
        return resolved.Success;
    }
}
