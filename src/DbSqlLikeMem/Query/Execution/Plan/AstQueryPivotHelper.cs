using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQueryPivotHelper(
    QueryExecutionContext context,
    Func<string, SqlExpr> parseExpr,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
    Func<Source, Dictionary<string, object?>, EvalRow> createSourceEvalRow)
{
    private readonly Func<string, SqlExpr> _parseExpr = parseExpr;
    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval = eval;
    private readonly Func<Source, Dictionary<string, object?>, EvalRow> _createSourceEvalRow = createSourceEvalRow;

    internal Source ApplyTableTransformsIfNeeded(
        Source source,
        SqlPivotSpec? pivot,
        SqlUnpivotSpec? unpivot,
        IDictionary<string, Source> ctes)
    {
        source = ApplyPivotIfNeeded(source, pivot, ctes);
        source = ApplyUnpivotIfNeeded(source, unpivot);
        return source;
    }

    private Source ApplyPivotIfNeeded(Source source, SqlPivotSpec? pivot, IDictionary<string, Source> ctes)
    {
        if (pivot is null)
            return source;

        var inputRows = MaterializeSourceRows(source);

        var forExpr = _parseExpr(pivot.ForColumnRaw);
        var aggArgExpr = _parseExpr(pivot.AggregateArgRaw);

        var forValues = inputRows.ToDictionary(
            r => r,
            r => _eval(forExpr, r, null, ctes),
            ReferenceEqualityComparer<EvalRow>.Instance);

        var inItems = pivot.InItems
            .Select(i => new { i.Alias, Value = _eval(_parseExpr(i.ValueRaw), EvalRow.Empty(), null, ctes) })
            .ToList();

        var forColumnNormalized = pivot.ForColumnRaw[(pivot.ForColumnRaw.LastIndexOf('.') + 1)..];
        var aggregateArgNormalized = pivot.AggregateArgRaw[(pivot.AggregateArgRaw.LastIndexOf('.') + 1)..];
        var groupColumns = source.ColumnNames
            .Where(c => !c.Equals(pivot.ForColumnRaw, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(forColumnNormalized, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(pivot.AggregateArgRaw, StringComparison.OrdinalIgnoreCase)
                        && !c.Equals(aggregateArgNormalized, StringComparison.OrdinalIgnoreCase))
            .ToList();

        static string BuildPivotGroupKey(EvalRow row, IReadOnlyList<string> columns)
        {
            var builder = new StringBuilder(columns.Count * 8);
            for (var i = 0; i < columns.Count; i++)
            {
                if (i > 0)
                    builder.Append('\u001F');

                builder.Append(row.GetByName(columns[i])?.ToString() ?? "<null>");
            }

            return builder.ToString();
        }

        var grouped = inputRows.GroupBy(r => BuildPivotGroupKey(r, groupColumns)).ToList();
        var result = new TableResultMock();

        for (var i = 0; i < groupColumns.Count; i++)
        {
            var dbType = TryGetSourceColumnDbType(source, groupColumns[i]) ?? DbType.Object;
            var isNullable = TryGetSourceColumnIsNullable(source, groupColumns[i]) ?? true;
            result.Columns.Add(new TableResultColMock(source.Alias, groupColumns[i], groupColumns[i], i, dbType, isNullable));
        }

        var pivotAggregateDbType = GetPivotAggregateResultDbType(pivot.AggregateFunction, aggArgExpr, source, context.Dialect);
        for (var i = 0; i < inItems.Count; i++)
            result.Columns.Add(new TableResultColMock(source.Alias, inItems[i].Alias, inItems[i].Alias, groupColumns.Count + i, pivotAggregateDbType, true));

        foreach (var group in grouped)
        {
            var first = group.First();
            var outRow = new Dictionary<int, object?>();

            for (var i = 0; i < groupColumns.Count; i++)
                outRow[i] = first.GetByName(groupColumns[i]);

            for (var i = 0; i < inItems.Count; i++)
            {
                var bucket = group.Where(r => forValues[r].EqualsSql(inItems[i].Value, context)).ToList();
                var aggregated = AggregatePivotBucket(pivot.AggregateFunction, aggArgExpr, bucket, ctes, context.Dialect);
                outRow[groupColumns.Count + i] = CoercePivotAggregateValue(aggregated, pivot.AggregateFunction, pivotAggregateDbType);
            }

            result.Add(outRow);
        }

        return Source.FromResult(source.Name, source.Alias, result);
    }

    private static DbType GetPivotAggregateResultDbType(string aggregateFunction, SqlExpr aggArgExpr, Source source, ISqlDialect dialect)
        => aggregateFunction.ToUpperInvariant() switch
        {
            SqlConst.COUNT => DbType.Int32,
            SqlConst.COUNT_BIG => DbType.Int64,
            SqlConst.STDEV or SqlConst.STDEVP or SqlConst.VAR or SqlConst.VARP => DbType.Double,
            SqlConst.SUM => PromotePivotSumResultDbType(TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object),
            SqlConst.AVG => PromotePivotAvgResultDbType(TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object, dialect),
            SqlConst.MIN or SqlConst.MAX => TryGetPivotAggregateArgumentDbType(aggArgExpr, source) ?? DbType.Object,
            _ => DbType.Object
        };

    private static DbType PromotePivotSumResultDbType(DbType inputType)
        => inputType switch
        {
            DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32 => DbType.Int32,
            DbType.UInt32 or DbType.Int64 or DbType.UInt64 => DbType.Int64,
            DbType.Single or DbType.Double => DbType.Double,
            DbType.Currency or DbType.Decimal or DbType.VarNumeric => DbType.Decimal,
            _ => inputType
        };

    private static DbType PromotePivotAvgResultDbType(DbType inputType, ISqlDialect dialect)
    {
        if (dialect.PivotAvgReturnsDecimalForIntegralInputs && IsIntegralPivotDbType(inputType))
            return DbType.Decimal;

        return inputType switch
        {
            DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32 => DbType.Int32,
            DbType.UInt32 or DbType.Int64 or DbType.UInt64 => DbType.Int64,
            DbType.Currency or DbType.Decimal or DbType.VarNumeric => DbType.Decimal,
            DbType.Single or DbType.Double => DbType.Double,
            _ => inputType
        };
    }

    private static bool IsIntegralPivotDbType(DbType inputType)
        => inputType is DbType.Byte
            or DbType.SByte
            or DbType.Int16
            or DbType.UInt16
            or DbType.Int32
            or DbType.UInt32
            or DbType.Int64
            or DbType.UInt64;

    private static object? CoercePivotAggregateValue(object? value, string aggregateFunction, DbType targetDbType)
    {
        if (value is null)
            return null;

        if (aggregateFunction.Equals(SqlConst.SUM, StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals(SqlConst.AVG, StringComparison.OrdinalIgnoreCase))
        {
            return targetDbType switch
            {
                DbType.Int32 => CoercePivotIntegerLikeValue<int>(value),
                DbType.Int64 => CoercePivotIntegerLikeValue<long>(value),
                DbType.Double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
                _ => value
            };
        }

        return value;
    }

    private static TInteger CoercePivotIntegerLikeValue<TInteger>(object value)
        where TInteger : struct
    {
        var decimalValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
        var truncated = decimal.Truncate(decimalValue);

        if (typeof(TInteger) == typeof(int))
            return (TInteger)(object)decimal.ToInt32(truncated);

        if (typeof(TInteger) == typeof(long))
            return (TInteger)(object)decimal.ToInt64(truncated);

        throw new NotSupportedException($"Integer-like coercion to '{typeof(TInteger).Name}' is not supported.");
    }

    private static DbType? TryGetPivotAggregateArgumentDbType(SqlExpr aggArgExpr, Source source)
    {
        if (aggArgExpr is not IdentifierExpr identifier)
            return null;

        return ResolveSourceColumnDbType(source, identifier.Name);
    }

    private static DbType? TryGetSourceColumnDbType(Source source, string columnName)
        => TryGetSourceColumnMetadata(source, columnName)?.DbType;

    private static bool? TryGetSourceColumnIsNullable(Source source, string columnName)
        => TryGetSourceColumnMetadata(source, columnName)?.IsNullable;

    private static TableResultColMock? TryGetSourceColumnMetadata(Source source, string columnName)
    {
        var normalizedColumnName = columnName[(columnName.LastIndexOf('.') + 1)..];

        return source.TryGetColumnMetadata(columnName, out var qualifiedMetadata)
            ? qualifiedMetadata
            : source.TryGetColumnMetadata(normalizedColumnName, out var metadata)
                ? metadata
                : null;
    }

    private static DbType? ResolveSourceColumnDbType(Source source, string columnName)
    {
        var metadataDbType = TryGetSourceColumnDbType(source, columnName);
        var sampledDbType = TryInferSourceColumnDbTypeFromRows(source, columnName);

        if (sampledDbType is DbType.Int32
            && metadataDbType is DbType.Decimal or DbType.Object or null)
        {
            return DbType.Int32;
        }

        if (sampledDbType is DbType.Double
            && metadataDbType is DbType.Decimal or DbType.Object or null)
        {
            return DbType.Double;
        }

        return metadataDbType ?? sampledDbType;
    }

    private static DbType? TryInferSourceColumnDbTypeFromRows(Source source, string columnName)
    {
        var normalizedColumnName = columnName[(columnName.LastIndexOf('.') + 1)..];
        var qualifiedColumnName = $"{source.Alias}.{normalizedColumnName}";

        foreach (var row in source.Rows())
        {
            if (!row.TryGetValue(qualifiedColumnName, out var value) || value is null or DBNull)
                continue;

            if (value is decimal dec
                && decimal.Truncate(dec) == dec
                && dec >= int.MinValue
                && dec <= int.MaxValue)
            {
                return DbType.Int32;
            }

            var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
            if (type == typeof(float) || type == typeof(double))
                return DbType.Double;

            try
            {
                return type.ConvertTypeToDbType();
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        return null;
    }

    private static DbType? ResolveUnpivotValueDbType(Source source, SqlUnpivotSpec unpivot)
    {
        DbType? resolved = null;
        foreach (var item in unpivot.InItems)
        {
            var next = ResolveSourceColumnDbType(source, item.SourceColumnName) ?? DbType.Object;
            if (resolved is null)
            {
                resolved = next;
                continue;
            }

            if (resolved != next)
                return DbType.Object;
        }

        return resolved;
    }

    private Source ApplyUnpivotIfNeeded(Source source, SqlUnpivotSpec? unpivot)
    {
        if (unpivot is null)
            return source;

        var inputRows = MaterializeSourceRows(source);
        var inColumns = new HashSet<string>(
            unpivot.InItems.Select(static item => item.SourceColumnName),
            StringComparer.OrdinalIgnoreCase);

        foreach (var item in unpivot.InItems)
        {
            if (!source.ContainsColumnName(item.SourceColumnName))
                throw new InvalidOperationException($"UNPIVOT source column '{item.SourceColumnName}' was not found in the input rowset.");
        }

        var groupColumns = source.ColumnNames
            .Where(column => !inColumns.Contains(column))
            .ToList();

        var result = new TableResultMock();
        for (var index = 0; index < groupColumns.Count; index++)
        {
            var dbType = ResolveSourceColumnDbType(source, groupColumns[index]) ?? DbType.Object;
            var isNullable = TryGetSourceColumnIsNullable(source, groupColumns[index]) ?? true;
            result.Columns.Add(new TableResultColMock(source.Alias, groupColumns[index], groupColumns[index], index, dbType, isNullable));
        }

        result.Columns.Add(new TableResultColMock(source.Alias, unpivot.NameColumnName, unpivot.NameColumnName, groupColumns.Count, DbType.String, false));
        var unpivotValueDbType = ResolveUnpivotValueDbType(source, unpivot) ?? DbType.Object;
        result.Columns.Add(new TableResultColMock(source.Alias, unpivot.ValueColumnName, unpivot.ValueColumnName, groupColumns.Count + 1, unpivotValueDbType, false));

        foreach (var row in inputRows)
        {
            foreach (var item in unpivot.InItems)
            {
                var value = row.GetByName(item.SourceColumnName);
                if (IsNullish(value))
                    continue;

                var outRow = new Dictionary<int, object?>();
                for (var index = 0; index < groupColumns.Count; index++)
                    outRow[index] = row.GetByName(groupColumns[index]);

                outRow[groupColumns.Count] = item.OutputName;
                outRow[groupColumns.Count + 1] = value;
                result.Add(outRow);
            }
        }

        return Source.FromResult(source.Name, source.Alias, result);
    }

    private List<EvalRow> MaterializeSourceRows(Source source)
        => [.. source.Rows().Select(fields => _createSourceEvalRow(source, fields))];

    private object? AggregatePivotBucket(string aggregateFunction, SqlExpr aggArgExpr, List<EvalRow> rows, IDictionary<string, Source> ctes, ISqlDialect dialect)
    {
        if (aggregateFunction.Equals(SqlConst.COUNT, StringComparison.OrdinalIgnoreCase))
        {
            if (aggArgExpr is StarExpr)
                return rows.Count;

            var count = 0;
            foreach (var row in rows)
            {
                var value = _eval(aggArgExpr, row, null, ctes);
                if (!IsNullish(value))
                    count++;
            }

            return count;
        }

        if (aggregateFunction.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase))
        {
            if (aggArgExpr is StarExpr)
                return (long)rows.Count;

            long count = 0;
            foreach (var row in rows)
            {
                var value = _eval(aggArgExpr, row, null, ctes);
                if (!IsNullish(value))
                    count++;
            }

            return count;
        }

        if (aggregateFunction.Equals(SqlConst.SUM, StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals(SqlConst.AVG, StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals(SqlConst.MIN, StringComparison.OrdinalIgnoreCase)
            || aggregateFunction.Equals(SqlConst.MAX, StringComparison.OrdinalIgnoreCase))
        {
            var group = new EvalGroup(rows);
            var aggregateExpr = new FunctionCallExpr(aggregateFunction, [aggArgExpr])
                .BindScalarFunctionDefinition(dialect);
            return _eval(aggregateExpr, group.Rows[0], group, ctes);
        }

        if (aggregateFunction.Equals(SqlConst.STDEV, StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: true, squareRoot: true);

        if (aggregateFunction.Equals(SqlConst.STDEVP, StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: false, squareRoot: true);

        if (aggregateFunction.Equals(SqlConst.VAR, StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: true, squareRoot: false);

        if (aggregateFunction.Equals(SqlConst.VARP, StringComparison.OrdinalIgnoreCase))
            return EvaluatePivotVarianceAggregate(rows, aggArgExpr, ctes, sample: false, squareRoot: false);

        throw new NotSupportedException($"PIVOT aggregate '{aggregateFunction}' not supported yet.");
    }

    private double? EvaluatePivotVarianceAggregate(
        IReadOnlyList<EvalRow> rows,
        SqlExpr aggArgExpr,
        IDictionary<string, Source> ctes,
        bool sample,
        bool squareRoot)
    {
        var values = new List<double>(rows.Count);
        foreach (var row in rows)
        {
            var rawValue = _eval(aggArgExpr, row, null, ctes);
            if (IsNullish(rawValue))
                continue;

            values.Add(Convert.ToDouble(rawValue, CultureInfo.InvariantCulture));
        }

        if (values.Count == 0)
            return null;

        if (sample && values.Count < 2)
            return null;

        var mean = values.Average();
        var sumOfSquaredDifferences = 0d;
        foreach (var value in values)
        {
            var difference = value - mean;
            sumOfSquaredDifferences += difference * difference;
        }

        var divisor = sample ? values.Count - 1 : values.Count;
        if (divisor <= 0)
            return null;

        var variance = sumOfSquaredDifferences / divisor;
        return squareRoot ? Math.Sqrt(variance) : variance;
    }

    private static bool IsNullish(object? v) => v is null || v is DBNull;
}
