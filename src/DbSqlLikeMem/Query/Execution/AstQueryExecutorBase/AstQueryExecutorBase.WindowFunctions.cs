namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private void ComputeWindowSlots(
        List<WindowSlot> slots,
        List<EvalRow> rows,
        IDictionary<string, Source> ctes)
    {
        if (slots.Count == 0 || rows.Count == 0)
            return;

        foreach (var slotGroup in GroupWindowSlotsBySpec(slots))
        {
            var spec = slotGroup[0].Expr.Spec;
            var partitions = WindowPartitionHelper.BuildPartitions(
                slotGroup[0].Expr,
                rows,
                (expr, row) => Eval(expr, row, null, ctes),
                value => QueryRowValueHelper.NormalizeDistinctKey(value, _context));

            foreach (var part in partitions.Values)
            {
                var orderValuesByRow = WindowPartitionHelper.SortPartition(
                    part,
                    spec.OrderBy,
                    (expr, row) => Eval(expr, row, null, ctes),
                    CompareSql);
                var partitionContext = new WindowPartitionExecutionContext(this, part, spec, ctes, orderValuesByRow);

                foreach (var slot in slotGroup)
                {
                    var w = slot.Expr;
                    var dialect = Dialect ?? throw new InvalidOperationException("Dialect is required for window function validation.");
                    var windowDefinition = w.ResolvedWindowFunction;
                    if (windowDefinition is null
                        && dialect.TryGetWindowFunctionDefinition(w, out var resolvedWindowDefinition))
                    {
                        windowDefinition = resolvedWindowDefinition;
                    }

                    var isRowNumber = dialect.IsRowNumberWindowFunction(w.Name);
                    var isRank = w.Name.Equals("RANK", StringComparison.OrdinalIgnoreCase);
                    var isDenseRank = w.Name.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase);
                    var isNtile = w.Name.Equals("NTILE", StringComparison.OrdinalIgnoreCase);
                    var isPercentRank = w.Name.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase);
                    var isCumeDist = w.Name.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase);
                    var isLag = w.Name.Equals("LAG", StringComparison.OrdinalIgnoreCase);
                    var isLead = w.Name.Equals("LEAD", StringComparison.OrdinalIgnoreCase);
                    var isFirstValue = w.Name.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isLastValue = w.Name.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase);
                    var isNthValue = w.Name.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);

                    var resolvedWindowDefinition2 = windowDefinition
                        ?? throw SqlUnsupported.ForDialect(dialect, $"window functions ({w.Name})");

                    if (resolvedWindowDefinition2.RequiresOrderBy && w.Spec.OrderBy.Count == 0)
                        throw new InvalidOperationException($"Window function '{w.Name}' requires ORDER BY in OVER clause.");

                    if (isRowNumber)
                    {
                        long rn = 1;
                        foreach (var r in part)
                        {
                            slot.Map[r] = rn;
                            rn++;
                        }
                        continue;
                    }

                    if (isNtile)
                    {
                        FillNtile(slot.Map, partitionContext, w, ctes);
                        continue;
                    }

                    if (isPercentRank || isCumeDist)
                    {
                        FillPercentRankOrCumeDist(slot.Map, partitionContext, isPercentRank);
                        continue;
                    }

                    if (isLag || isLead)
                    {
                        FillLagOrLead(slot.Map, partitionContext, w, ctes, isLead);
                        continue;
                    }

                    if (isFirstValue || isLastValue)
                    {
                        FillFirstOrLastValue(slot.Map, partitionContext, w, ctes, isLastValue);
                        continue;
                    }

                    if (isNthValue)
                    {
                        FillNthValue(slot.Map, partitionContext, w, ctes);
                        continue;
                    }

                    FillRankOrDenseRank(slot.Map, partitionContext, isRank);
                }
            }
        }
    }

    /// <summary>
    /// EN: Fills FIRST_VALUE/LAST_VALUE results for all rows in the current partition.
    /// PT: Preenche os resultados de FIRST_VALUE/LAST_VALUE para todas as linhas da partição atual.
    /// </summary>
    private void FillFirstOrLastValue(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillLast)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetRow = part[fillLast ? part.Count - 1 : 0];
            var value = valueSelector is null
                ? Eval(valueExpr, targetRow, null, ctes)
                : valueSelector(targetRow);
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = fillLast ? frameRange.EndIndex : frameRange.StartIndex;
            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }

    /// <summary>
    /// EN: Fills NTH_VALUE results using the resolved 1-based index in the ordered partition.
    /// PT: Preenche os resultados de NTH_VALUE usando o índice 1-based resolvido na partição ordenada.
    /// </summary>
    private void FillNthValue(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        var nth = ResolveNthValueIndex(windowFunctionExpr.Args, part[0], ctes);
        if (nth <= 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            var targetIndex = nth - 1;
            var value = targetIndex < part.Count
                ? valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex])
                : null;
            foreach (var row in part)
                map[row] = value;

            return;
        }

        var valuesByTargetIndex = new Dictionary<int, object?>(Math.Max(1, Math.Min(part.Count, 8)));
        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty)
            {
                map[part[i]] = null;
                continue;
            }

            var targetIndex = frameRange.StartIndex + (nth - 1);
            if (targetIndex > frameRange.EndIndex)
            {
                map[part[i]] = null;
                continue;
            }

            if (!valuesByTargetIndex.TryGetValue(targetIndex, out var value))
            {
                value = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                valuesByTargetIndex[targetIndex] = value;
            }

            map[part[i]] = value;
        }
    }

    /// <summary>
    /// EN: Resolves row index boundaries for ROWS/RANGE/GROUPS window frames for the current row.
    /// PT: Resolve os limites de índice de linha para frames ROWS/RANGE/GROUPS da janela na linha atual.
    /// </summary>
    private RowsFrameRange ResolveWindowFrameRange(
        WindowFrameSpec? frame,
        List<EvalRow> part,
        int rowIndex,
        IReadOnlyList<WindowOrderItem> orderBy,
        IDictionary<string, Source> ctes,
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
            (expr, row) => Eval(expr, row, null, ctes));
        return WindowFrameRangeResolver.Resolve(
            frame,
            part,
            rowIndex,
            orderBy,
            orderValuesByRow,
            (left, right) => WindowOrderValueHelper.WindowOrderValuesEqual(left, right, CompareSql));
    }

    private static bool TryReadIntLiteral(SqlExpr expr, out int value)
    {
        value = default;
        if (expr is not LiteralExpr lit)
            return false;

        var raw = lit.Value;
        if (raw is null || raw is DBNull)
            return false;

        if (raw is IConvertible)
        {
            try
            {
                value = Convert.ToInt32(raw, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    private static bool TryReadLongLiteral(SqlExpr expr, out long value)
    {
        value = default;
        if (expr is not LiteralExpr lit)
            return false;

        var raw = lit.Value;
        if (raw is null || raw is DBNull)
            return false;

        if (raw is IConvertible)
        {
            try
            {
                value = Convert.ToInt64(raw, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// EN: Resolves NTH_VALUE index from literal or evaluated expression with safe fallback.
    /// PT: Resolve o índice do NTH_VALUE a partir de literal ou expressão avaliada com fallback seguro.
    /// </summary>
    private int ResolveNthValueIndex(
        IReadOnlyList<SqlExpr> args,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = Eval(args[1], sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt32(evaluated, CultureInfo.InvariantCulture);
                return parsed > 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
    }

    /// <summary>
    /// EN: Fills LAG/LEAD values for rows in the current ordered partition.
    /// PT: Preenche valores de LAG/LEAD para as linhas da partição ordenada atual.
    /// </summary>
    private void FillLagOrLead(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes,
        bool fillLead)
    {
        var part = partitionContext.Part;
        if (part.Count == 0 || windowFunctionExpr.Args.Count == 0)
            return;

        var valueExpr = windowFunctionExpr.Args[0];
        var offset = ResolveLagLeadOffset(windowFunctionExpr.Args, part[0], ctes);
        var defaultExpr = windowFunctionExpr.Args.Count >= 3 ? windowFunctionExpr.Args[2] : null;
        var valueSelector = TryCreateWindowValueSelector(valueExpr, part[0]);
        var hasWholePartitionFrame = windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition();

        if (hasWholePartitionFrame)
        {
            if (offset == 0)
            {
                foreach (var currentRow in part)
                    map[currentRow] = valueSelector is null
                        ? Eval(valueExpr, currentRow, null, ctes)
                        : valueSelector(currentRow);

                return;
            }

            for (int i = 0; i < part.Count; i++)
            {
                var targetIndex = fillLead ? i + offset : i - offset;
                var currentRow = part[i];
                map[currentRow] = targetIndex >= 0 && targetIndex < part.Count
                    ? valueSelector is null
                        ? Eval(valueExpr, part[targetIndex], null, ctes)
                        : valueSelector(part[targetIndex])
                    : defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
            }

            return;
        }

        if (offset == 0)
        {
            for (int i = 0; i < part.Count; i++)
            {
                var currentRow = part[i];
                var frameRange = partitionContext.GetFrameRange(i);
                if (frameRange.IsEmpty || i < frameRange.StartIndex || i > frameRange.EndIndex)
                {
                    map[currentRow] = defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
                    continue;
                }

                map[currentRow] = valueSelector is null
                    ? Eval(valueExpr, currentRow, null, ctes)
                    : valueSelector(currentRow);
            }

            return;
        }

        for (int i = 0; i < part.Count; i++)
        {
            var targetIndex = fillLead ? i + offset : i - offset;
            var currentRow = part[i];
            var frameRange = partitionContext.GetFrameRange(i);

            if (!frameRange.IsEmpty && targetIndex >= frameRange.StartIndex && targetIndex <= frameRange.EndIndex)
            {
                map[currentRow] = valueSelector is null
                    ? Eval(valueExpr, part[targetIndex], null, ctes)
                    : valueSelector(part[targetIndex]);
                continue;
            }

            map[currentRow] = defaultExpr is null ? null : Eval(defaultExpr, currentRow, null, ctes);
        }
    }

    /// <summary>
    /// EN: Builds a direct value accessor for simple window-value references when the row shape is unambiguous.
    /// PT: Monta um acesso direto ao valor para referencias simples de janela quando a forma da linha e inequivoca.
    /// </summary>
    private Func<EvalRow, object?>? TryCreateWindowValueSelector(
        SqlExpr valueExpr,
        EvalRow sampleRow)
    {
        if (valueExpr is IdentifierExpr identifier)
        {
            if (Dialect is null
                || IsReservedWindowValueIdentifier(identifier.Name)
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

    /// <summary>
    /// EN: Identifies reserved identifiers that must keep the generic window-value evaluation path.
    /// PT: Identifica identificadores reservados que precisam manter o caminho generico de avaliacao de valor de janela.
    /// </summary>
    private bool IsReservedWindowValueIdentifier(string name)
        => AstQueryReservedIdentifierHelper.IsReservedWindowValueIdentifier(Dialect, name);

    /// <summary>
    /// EN: Resolves LAG/LEAD offset from literal or evaluated expression with safe fallback.
    /// PT: Resolve o offset de LAG/LEAD a partir de literal ou expressão avaliada com fallback seguro.
    /// </summary>
    private int ResolveLagLeadOffset(
        IReadOnlyList<SqlExpr> args,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral >= 0)
            return parsedLiteral;

        var evaluated = Eval(args[1], sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt32(evaluated, CultureInfo.InvariantCulture);
                return parsed >= 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
    }

    /// <summary>
    /// EN: Fills RANK/DENSE_RANK using per-row ROWS frame boundaries when present.
    /// PT: Preenche RANK/DENSE_RANK usando limites de frame ROWS por linha quando presentes.
    /// </summary>
    private void FillRankOrDenseRank(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        bool fillRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        if (partitionContext.CoversWholePartition())
        {
            var denseRank = 1L;
            foreach (var peerGroup in partitionContext.GetPeerGroups())
            {
                var value = fillRank ? peerGroup.Start + 1L : denseRank;
                for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                    map[part[i]] = value;

                denseRank++;
            }

            return;
        }

        var orderValuesByRow = partitionContext.GetRequiredOrderValuesByRow();

        for (var i = 0; i < part.Count; i++)
        {
            var frameRange = partitionContext.GetFrameRange(i);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, i))
            {
                map[part[i]] = null;
                continue;
            }

            var currentValues = orderValuesByRow[part[i]];
            long rank = 1;
            long denseRank = 1;
            object?[]? prevValues = null;

            for (var frameIndex = frameRange.StartIndex; frameIndex <= frameRange.EndIndex; frameIndex++)
            {
                var frameValues = orderValuesByRow[part[frameIndex]];
                if (prevValues is not null && !WindowOrderValueHelper.WindowOrderValuesEqual(prevValues, frameValues, CompareSql))
                {
                    rank = (frameIndex - frameRange.StartIndex) + 1;
                    denseRank++;
                }

                if (WindowOrderValueHelper.WindowOrderValuesEqual(frameValues, currentValues, CompareSql))
                    break;

                prevValues = frameValues;
            }

            map[part[i]] = fillRank ? rank : denseRank;
        }
    }

    /// <summary>
    /// EN: Computes and fills PERCENT_RANK or CUME_DIST values for the current partition.
    /// PT: Calcula e preenche valores de PERCENT_RANK ou CUME_DIST para a partição atual.
    /// </summary>
    private void FillPercentRankOrCumeDist(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        bool fillPercentRank)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        if (partitionContext.CoversWholePartition())
        {
            foreach (var peerGroup in partitionContext.GetPeerGroups())
            {
                var peerCount = peerGroup.End - peerGroup.Start + 1;
                var value = fillPercentRank
                    ? part.Count <= 1 ? 0d : (double)peerGroup.Start / (part.Count - 1)
                    : (double)(peerGroup.Start + peerCount) / part.Count;

                for (var i = peerGroup.Start; i <= peerGroup.End; i++)
                    map[part[i]] = value;
            }

            return;
        }

        var orderValuesByRow = partitionContext.GetRequiredOrderValuesByRow();

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var row = part[rowIndex];
            var frameRange = partitionContext.GetFrameRange(rowIndex);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, rowIndex))
            {
                map[row] = null;
                continue;
            }

            var frameCount = frameRange.EndIndex - frameRange.StartIndex + 1;
            var currentValues = orderValuesByRow[row];
            long lessThanCount = 0;
            long peerCount = 0;

            for (var frameIndex = frameRange.StartIndex; frameIndex <= frameRange.EndIndex; frameIndex++)
            {
                var frameValues = orderValuesByRow[part[frameIndex]];
                if (WindowOrderValueHelper.WindowOrderValuesEqual(frameValues, currentValues, CompareSql))
                {
                    peerCount++;
                    continue;
                }

                if (frameIndex < rowIndex)
                    lessThanCount++;
            }

            var rankInFrame = lessThanCount + 1;
            if (fillPercentRank)
            {
                map[row] = frameCount <= 1 ? 0d : ((double)(rankInFrame - 1)) / (frameCount - 1);
            }
            else
            {
                map[row] = (double)(lessThanCount + peerCount) / frameCount;
            }
        }
    }

    /// <summary>
    /// EN: Fills NTILE values honoring per-row ROWS frame boundaries when present.
    /// PT: Preenche valores de NTILE respeitando os limites de frame ROWS por linha quando presentes.
    /// </summary>
    private void FillNtile(
        Dictionary<EvalRow, object?> map,
        WindowPartitionExecutionContext partitionContext,
        WindowFunctionExpr windowFunctionExpr,
        IDictionary<string, Source> ctes)
    {
        var part = partitionContext.Part;
        if (part.Count == 0)
            return;

        var bucketCount = ResolveNtileBucketCount(windowFunctionExpr, part.Count, part[0], ctes);
        if (bucketCount <= 0)
            return;

        if (windowFunctionExpr.Spec.Frame is null || partitionContext.CoversWholePartition())
        {
            for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
                map[part[rowIndex]] = (rowIndex * bucketCount) / part.Count + 1;

            return;
        }

        for (var rowIndex = 0; rowIndex < part.Count; rowIndex++)
        {
            var frameRange = partitionContext.GetFrameRange(rowIndex);
            if (frameRange.IsEmpty || !WindowOrderValueHelper.RowsFrameContainsRow(frameRange, rowIndex))
            {
                map[part[rowIndex]] = null;
                continue;
            }

            var frameSize = frameRange.EndIndex - frameRange.StartIndex + 1;
            var positionInFrame = (rowIndex - frameRange.StartIndex) + 1;
            var tile = ((positionInFrame - 1) * bucketCount) / frameSize + 1;
            map[part[rowIndex]] = tile;
        }
    }

    /// <summary>
    /// EN: Resolves the bucket count argument for NTILE from literal or evaluated expression.
    /// PT: Resolve o argumento de quantidade de buckets do NTILE a partir de literal ou expressão avaliada.
    /// </summary>
    private long ResolveNtileBucketCount(
        WindowFunctionExpr windowFunctionExpr,
        int partitionSize,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes)
    {
        if (partitionSize <= 0)
            return 0;

        if (windowFunctionExpr.Args.Count == 0)
            return 1;

        var arg = windowFunctionExpr.Args[0];
        if (TryReadLongLiteral(arg, out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = Eval(arg, sampleRow, null, ctes);
        if (evaluated is null || evaluated is DBNull)
            return 1;

        if (evaluated is IConvertible)
        {
            try
            {
                var parsed = Convert.ToInt64(evaluated, CultureInfo.InvariantCulture);
                return parsed > 0 ? parsed : 1;
            }
            catch
            {
                return 1;
            }
        }

        return 1;
    }

    private int CompareSql(object? a, object? b)
    {
        if (IsNullish(a) && IsNullish(b)) return 0;
        if (IsNullish(a)) return -1;
        if (IsNullish(b)) return 1;

        return a!.Compare(b!, _context);
    }
}
