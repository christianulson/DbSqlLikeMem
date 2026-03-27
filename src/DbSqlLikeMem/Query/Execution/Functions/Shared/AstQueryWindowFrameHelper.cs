using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryWindowFrameHelper
{
    internal static RowsFrameRange ResolveWindowFrameRange(
        this WindowPartitionExecutionContext context,
        WindowFrameSpec? frame,
        List<EvalRow> part,
        int rowIndex,
        IReadOnlyList<WindowOrderItem> orderBy,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        Dictionary<EvalRow, object?[]>? precomputedOrderValuesByRow = null)
    {
        if (part.Count == 0)
            return RowsFrameRange.Empty;

        if (frame is null || frame.Unit == WindowFrameUnit.Rows)
            return WindowFrameRangeResolver.ResolveRowsFrameRange(frame, part.Count, rowIndex);

        if (orderBy.Count == 0)
            throw new InvalidOperationException($"Window frame unit '{frame.Unit}' requires ORDER BY in OVER clause.");

        var orderValuesByRow = precomputedOrderValuesByRow ?? WindowOrderValueHelper.BuildWindowOrderValuesByRow(
            part,
            orderBy,
            (expr, row) => eval(expr, row, null, ctes));
        return context.ResolveRowsFrameRange(
            frame,
            part,
            rowIndex,
            orderBy,
            orderValuesByRow
            );
    }

    internal static Func<EvalRow, object?>? TryCreateWindowValueSelector(
        this WindowPartitionExecutionContext context,
        SqlExpr valueExpr,
        EvalRow sampleRow)
    {
        if (valueExpr is IdentifierExpr identifier)
        {
            if (context.IsReservedWindowValueIdentifier(identifier.Name)
                || !sampleRow.TryGetSingleSource(out var singleSource)
                || singleSource is null
                || !singleSource.ContainsColumnName(identifier.Name))
            {
                return null;
            }

            var columnName = identifier.Name;
            return row => row.GetByName(columnName);
        }

        if (valueExpr is ColumnExpr column)
        {
            if (string.IsNullOrWhiteSpace(column.Qualifier))
            {
                if (!sampleRow.TryGetSingleSource(out var singleSource)
                    || singleSource is null
                    || !singleSource.ContainsColumnName(column.Name))
                {
                    return null;
                }

                var columnName = column.Name;
                return row => row.GetByName(columnName);
            }

            return row => QueryRowValueHelper.ResolveColumn(column.Qualifier, column.Name, row);
        }

        return null;
    }
}
