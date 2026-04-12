using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryAggregateEvaluator
{
    internal static bool TryParseScalarCountAggregate(
        string exprRaw,
        Func<string, SqlExpr> parseExpr,
        out SqlExpr countArg,
        out bool isCountBig)
    {
        countArg = default!;
        isCountBig = false;
        if (string.IsNullOrWhiteSpace(exprRaw))
            return false;

        var parsed = parseExpr(exprRaw);
        if (parsed is FunctionCallExpr fn
            && fn.Args.Count == 1
            && (fn.Name.Equals(SqlConst.COUNT, StringComparison.OrdinalIgnoreCase)
                || fn.Name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase)))
        {
            countArg = fn.Args[0];
            isCountBig = fn.Name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase);
            return true;
        }

        if (parsed is CallExpr call
            && call.Args.Count == 1
            && (call.Name.Equals(SqlConst.COUNT, StringComparison.OrdinalIgnoreCase)
                || call.Name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase)))
        {
            countArg = call.Args[0];
            isCountBig = call.Name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase);
            return true;
        }

        return false;
    }

    internal static bool TryParseStringAggregateCall(
        string exprRaw,
        Func<string, SqlExpr> parseExpr,
        out CallExpr call)
    {
        call = null!;
        if (string.IsNullOrWhiteSpace(exprRaw))
            return false;

        SqlExpr expr;
        try
        {
            expr = parseExpr(exprRaw);
        }
        catch
        {
            return false;
        }

        if (expr is not CallExpr parsedCall)
            return false;

        if (parsedCall.Name is not (SqlConst.GROUP_CONCAT or SqlConst.STRING_AGG or SqlConst.LISTAGG or SqlConst.LIST))
            return false;

        if (parsedCall.WithinGroupOrderBy is not null)
            return false;

        call = parsedCall;
        return true;
    }

    internal static object? EvalAggregate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var name = fn.Name.ToUpperInvariant();

        if (TryEvalAggregateCount(context, fn, group, ctes, eval, name, out var countValue))
            return countValue;

        if (name is SqlConst.JSON_GROUP_OBJECT or SqlConst.JSON_OBJECTAGG)
        {
            if (name == SqlConst.JSON_OBJECTAGG
                && !context.Dialect.TryGetScalarFunctionDefinition(name, out _))
            {
                throw context.NotSupported(name);
            }

            return EvalJsonGroupObjectAggregate(fn, group, ctes, eval);
        }

        if (name is SqlConst.JSON_OBJECT_AGG
            or SqlConst.JSON_OBJECT_AGG_STRICT
            or SqlConst.JSON_OBJECT_AGG_UNIQUE
            or SqlConst.JSON_OBJECT_AGG_UNIQUE_STRICT
            or SqlConst.JSONB_OBJECT_AGG
            or SqlConst.JSONB_OBJECT_AGG_STRICT
            or SqlConst.JSONB_OBJECT_AGG_UNIQUE
            or SqlConst.JSONB_OBJECT_AGG_UNIQUE_STRICT)
            return EvalJsonGroupObjectAggregate(fn, group, ctes, eval);

        if (name is "CORR"
            or "CORR_K"
            or "CORR_S"
            or "COVAR_POP"
            or "COVAR_SAMP"
            or "COVARIANCE"
            or "COVARIANCE_SAMP"
            or "CORRELATION")
        {
            var normalized = name switch
            {
                "COVARIANCE" => "COVAR_POP",
                "COVARIANCE_SAMP" => "COVAR_SAMP",
                "CORRELATION" => "CORR",
                _ => name
            };
            return EvalCorrelationAggregate(fn, group, ctes, eval, normalized);
        }

        if (name is "GROUP_ID")
            return 0;

        if (name.StartsWith("APPROX_", StringComparison.OrdinalIgnoreCase))
        {
            var definition = fn.ResolvedScalarFunction;
            if (definition is null || !definition.AllowsCall)
            {
                throw context.NotSupported(name);
            }

            return EvalApproxAggregate(fn, group, ctes, eval, name);
        }

        if (name.StartsWith("REGR_", StringComparison.OrdinalIgnoreCase))
            return EvalRegressionAggregate(fn, group, ctes, eval, name);

        if (name.StartsWith("STATS_", StringComparison.OrdinalIgnoreCase))
            return null;

        if (name is "STD" or "STDDEV" or "STDDEV_POP" or "STDDEV_SAMP")
        {
            var normalizedName = name == "STD" ? "STDDEV_POP" : name;
            return EvalStdDevAggregate(context, fn, group, ctes, eval, normalizedName);
        }

        if (name is "RATIO_TO_REPORT")
            return null;

        if (name is "MEDIAN" or "PERCENTILE" or "PERCENTILE_CONT" or "PERCENTILE_DISC")
        {
            if (!context.Dialect.SupportsSqlServerAggregateFunction(name))
            {
                throw context.NotSupported(name);
            }

            return EvalPercentileAggregate(fn, group, ctes, eval, name);
        }

        if (name is SqlConst.CHECKSUM_AGG)
        {
            if (!(fn.ResolvedScalarFunction?.AllowsCall
                ?? (context.Dialect.TryGetScalarFunctionDefinition(fn, out var checksumDefinition)
                    && checksumDefinition is not null
                    && checksumDefinition.AllowsCall)))
                throw context.NotSupported(name);
        }

        if (name is SqlConst.GROUP_CONCAT or SqlConst.STRING_AGG or SqlConst.LISTAGG or SqlConst.LIST)
        {
            var separator = GetAggregateSeparator(fn.Args, group, ctes, eval);
            var defaultSeparator = GetStringAggregateDefaultSeparator(name) ?? string.Empty;
            return EvalSimpleStringAggregate(context, fn, group, ctes, eval, separator, defaultSeparator);
        }

        var values = TryGetAggregateValues(context, fn, group, ctes, eval);
        if (values is null)
            return null;

        if (values.Count == 0)
            return name == SqlConst.TOTAL ? 0d : null;

        return EvalCollectedAggregateValues(fn, group, ctes, eval, name, values);
    }

    internal static object? EvalAggregate(
        this QueryExecutionContext context,
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (fn.Name is SqlConst.GROUP_CONCAT or SqlConst.STRING_AGG or SqlConst.LISTAGG or SqlConst.LIST)
        {
            var separator = GetAggregateSeparator(fn.Args, group, ctes, eval);
            var defaultSeparator = GetStringAggregateDefaultSeparator(fn.Name) ?? string.Empty;
            return EvalSimpleStringAggregate(context, fn, group, ctes, eval, separator, defaultSeparator);
        }

        var shim = fn.ResolvedScalarFunction is not null
            ? new FunctionCallExpr(fn.Name, fn.Args, fn.Distinct).BindScalarFunctionDefinition(fn.ResolvedScalarFunction)
            : new FunctionCallExpr(fn.Name, fn.Args, fn.Distinct);
        return context.EvalAggregate(shim, group, ctes, eval);
    }

    private static string? EvalSimpleStringAggregate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        object? separatorObj,
        string? defaultSeparator)
        => EvalSimpleStringAggregate(
            context,
            new CallExpr(fn.Name, fn.Args, fn.Distinct).BindScalarFunctionDefinition(fn.ResolvedScalarFunction),
            group,
            ctes,
            eval,
            separatorObj,
            defaultSeparator);

    private static object? EvalCollectedAggregateValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name,
        IReadOnlyList<object?> values)
    {
        var separator = GetAggregateSeparator(fn.Args, group, ctes, eval);
        var result = name switch
        {
            SqlConst.SUM => AggregateNumericValues(values, AggregateNumericOperation.Sum),
            SqlConst.AVG => AggregateNumericValues(values, AggregateNumericOperation.Average),
            SqlConst.MIN => AggregateMinMaxValues(values, useMax: false),
            SqlConst.MAX => AggregateMinMaxValues(values, useMax: true),
            SqlConst.CHECKSUM_AGG => AggregateChecksumValues(values, binary: false),
            SqlConst.GROUP_CONCAT => EvalStringAggregate(values, separator, ","),
            SqlConst.STRING_AGG => EvalStringAggregate(values, separator, ","),
            SqlConst.LISTAGG => EvalStringAggregate(values, separator, string.Empty),
            SqlConst.LIST => EvalStringAggregate(values, separator, ","),
            SqlConst.ANY_VALUE => AggregateAnyValue(values),
            SqlConst.BIT_AND => AggregateBitwiseValues(values, BitwiseAggregateOperation.And),
            SqlConst.BIT_OR => AggregateBitwiseValues(values, BitwiseAggregateOperation.Or),
            SqlConst.BIT_XOR => AggregateBitwiseValues(values, BitwiseAggregateOperation.Xor),
            SqlConst.JSON_ARRAYAGG => EvalJsonArrayAggregate(values),
            SqlConst.JSON_AGG => EvalJsonArrayAggregate(values),
            SqlConst.JSONB_AGG => EvalJsonArrayAggregate(values),
            SqlConst.ARRAY_AGG => AggregateCollect(values),
            SqlConst.BOOL_AND => AggregateBoolValues(values, useAnd: true),
            SqlConst.EVERY => AggregateBoolValues(values, useAnd: true),
            SqlConst.BOOL_OR => AggregateBoolValues(values, useAnd: false),
            SqlConst.COLLECT => AggregateCollect(values),
            SqlConst.TOTAL => AggregateTotal(values),
            SqlConst.STDEV => AggregateVariance(values, sample: true) is double stdev ? Math.Sqrt(stdev) : null,
            SqlConst.STDEVP => AggregateVariance(values, sample: false) is double stdevp ? Math.Sqrt(stdevp) : null,
            SqlConst.VAR => AggregateVariance(values, sample: true),
            SqlConst.VARP => AggregateVariance(values, sample: false),
            SqlConst.VAR_POP => AggregateVariance(values, sample: false),
            SqlConst.VARIANCE => AggregateVariance(values, sample: false),
            SqlConst.VARIANCE_SAMP => AggregateVariance(values, sample: true),
            SqlConst.VAR_SAMP => AggregateVariance(values, sample: true),
            SqlConst.CV => AggregateCoefficientOfVariation(values),
            _ => null
        };

        if (name == SqlConst.SUM
            && fn.Args.Count > 0
            && ContainsParameter(fn.Args[0], "cutoff"))
        {
            Console.WriteLine($"[AggDebug][SUM][final] result={FormatDebugValue(result)}");
        }

        return result;
    }

    private static object? EvalJsonGroupObjectAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (fn.Args.Count < 2)
            return null;

        var obj = new System.Text.Json.Nodes.JsonObject();
        foreach (var row in group.Rows)
        {
            var keyValue = eval(fn.Args[0], row, null, ctes);
            if (IsNullish(keyValue))
                continue;

            var key = keyValue?.ToString() ?? string.Empty;
            var value = eval(fn.Args[1], row, null, ctes);
            obj[key] = AstQueryJsonPathFunctionEvaluator.CreateJsonNodeFromValue(value);
        }

        return obj.ToJsonString();
    }

    private static object? EvalPercentileAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<double>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var value = eval(fn.Args[0], row, null, ctes);
            if (IsNullish(value))
                continue;

            if (TryConvertNumericToDouble(value, out var numeric))
                values.Add(numeric);
        }

        if (values.Count == 0)
            return null;

        values.Sort();

        var percentile = 0.5d;
        if (fn.Args.Count > 1)
        {
            var percentileValue = eval(fn.Args[1], group.Rows[0], null, ctes);
            if (IsNullish(percentileValue) || !TryConvertNumericToDouble(percentileValue, out percentile))
                return null;
        }

        if (percentile < 0d)
            percentile = 0d;
        else if (percentile > 1d)
            percentile = 1d;
        var isDiscrete = name.Equals("PERCENTILE_DISC", StringComparison.OrdinalIgnoreCase);
        if (name.Equals("MEDIAN", StringComparison.OrdinalIgnoreCase))
            percentile = 0.5d;

        if (isDiscrete)
        {
            var index = (int)Math.Ceiling(percentile * values.Count) - 1;
            if (index < 0)
                index = 0;
            if (index >= values.Count)
                index = values.Count - 1;
            return values[index];
        }

        var rank = percentile * (values.Count - 1);
        var lowerIndex = (int)Math.Floor(rank);
        var upperIndex = (int)Math.Ceiling(rank);
        if (lowerIndex == upperIndex)
            return values[lowerIndex];

        var fraction = rank - lowerIndex;
        return values[lowerIndex] + (values[upperIndex] - values[lowerIndex]) * fraction;
    }

    private static object? EvalCorrelationAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = eval(fn.Args[0], row, null, ctes);
            var yValue = eval(fn.Args[1], row, null, ctes);
            if (IsNullish(xValue) || IsNullish(yValue))
                continue;

            try
            {
                var x = Convert.ToDouble(xValue, CultureInfo.InvariantCulture);
                var y = Convert.ToDouble(yValue, CultureInfo.InvariantCulture);
                pairs.Add((x, y));
            }
            catch
            {
                return null;
            }
        }

        if (pairs.Count == 0)
            return null;

        var sumX = 0d;
        var sumY = 0d;
        for (var i = 0; i < pairs.Count; i++)
        {
            sumX += pairs[i].X;
            sumY += pairs[i].Y;
        }

        var meanX = sumX / pairs.Count;
        var meanY = sumY / pairs.Count;
        var sumXY = 0d;
        var sumXX = 0d;
        var sumYY = 0d;
        for (var i = 0; i < pairs.Count; i++)
        {
            var dx = pairs[i].X - meanX;
            var dy = pairs[i].Y - meanY;
            sumXY += dx * dy;
            sumXX += dx * dx;
            sumYY += dy * dy;
        }

        if (name is "COVAR_POP")
            return sumXY / pairs.Count;

        if (name is "COVAR_SAMP")
            return pairs.Count < 2 ? null : sumXY / (pairs.Count - 1);

        if (sumXX == 0d || sumYY == 0d)
            return null;

        return sumXY / Math.Sqrt(sumXX * sumYY);
    }

    private static object? EvalApproxAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        if (name is "APPROX_MEDIAN")
            return EvalPercentileAggregate(fn, group, ctes, eval, "MEDIAN");

        if (name is "APPROX_PERCENTILE" or "APPROX_PERCENTILE_AGG" or "APPROX_PERCENTILE_DETAIL")
            return EvalPercentileAggregate(fn, group, ctes, eval, "PERCENTILE_CONT");

        if (name is "APPROX_COUNT_DISTINCT" or "APPROX_COUNT_DISTINCT_AGG" or "APPROX_COUNT_DISTINCT_DETAIL")
        {
            var set = new HashSet<object?>(EqualityComparer<object?>.Default);
            foreach (var row in group.Rows)
            {
                var value = eval(fn.Args[0], row, null, ctes);
                if (!IsNullish(value))
                    set.Add(value);
            }
            return set.Count;
        }

        return null;
    }

    private static object? EvalRegressionAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = eval(fn.Args[0], row, null, ctes);
            var yValue = eval(fn.Args[1], row, null, ctes);
            if (IsNullish(xValue) || IsNullish(yValue))
                continue;

            try
            {
                var x = Convert.ToDouble(xValue, CultureInfo.InvariantCulture);
                var y = Convert.ToDouble(yValue, CultureInfo.InvariantCulture);
                pairs.Add((x, y));
            }
            catch
            {
                return null;
            }
        }

        if (pairs.Count == 0)
            return null;

        var sumX = 0d;
        var sumY = 0d;
        for (var i = 0; i < pairs.Count; i++)
        {
            sumX += pairs[i].X;
            sumY += pairs[i].Y;
        }

        var meanX = sumX / pairs.Count;
        var meanY = sumY / pairs.Count;
        var sumXX = 0d;
        var sumYY = 0d;
        var sumXY = 0d;
        for (var i = 0; i < pairs.Count; i++)
        {
            var dx = pairs[i].X - meanX;
            var dy = pairs[i].Y - meanY;
            sumXX += dx * dx;
            sumYY += dy * dy;
            sumXY += dx * dy;
        }

        return name switch
        {
            "REGR_COUNT" => pairs.Count,
            "REGR_AVGX" => meanX,
            "REGR_AVGY" => meanY,
            "REGR_SXX" => sumXX,
            "REGR_SYY" => sumYY,
            "REGR_SXY" => sumXY,
            "REGR_SLOPE" => sumXX == 0 ? null : sumXY / sumXX,
            "REGR_INTERCEPT" => sumXX == 0 ? null : meanY - (sumXY / sumXX) * meanX,
            "REGR_ICPT" => sumXX == 0 ? null : meanY - (sumXY / sumXX) * meanX,
            "REGR_R2" => (sumXX == 0 || sumYY == 0) ? null : (sumXY * sumXY) / (sumXX * sumYY),
            _ => null
        };
    }

    private static object? EvalStdDevAggregate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name)
    {
        var values = TryGetAggregateValues(context, fn, group, ctes, eval);
        if (values is null)
            return null;

        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            if (IsNullish(values[i]))
                continue;

            count++;
            var x = Convert.ToDouble(values[i], CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        var denominator = name == "STDDEV_SAMP" ? count - 1 : count;
        if (denominator <= 0)
            return null;

        var variance = m2 / denominator;
        return Math.Sqrt(variance);
    }

    private static object? AggregateAnyValue(IReadOnlyList<object?> values)
    {
        foreach (var value in values)
        {
            if (!IsNullish(value))
                return value;
        }

        return null;
    }

    private enum BitwiseAggregateOperation
    {
        And,
        Or,
        Xor
    }

    private static object? AggregateBitwiseValues(IReadOnlyList<object?> values, BitwiseAggregateOperation operation)
    {
        var hasValue = false;
        var acc = 0L;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            var next = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            if (!hasValue)
            {
                acc = next;
                hasValue = true;
                continue;
            }

            acc = operation switch
            {
                BitwiseAggregateOperation.And => acc & next,
                BitwiseAggregateOperation.Or => acc | next,
                BitwiseAggregateOperation.Xor => acc ^ next,
                _ => acc
            };
        }

        return hasValue ? acc : null;
    }

    private static object? EvalJsonArrayAggregate(IReadOnlyList<object?> values)
    {
        if (values.Count == 0)
            return null;

        return AstQueryJsonSharedFunctionEvaluator.BuildJsonArray(values);
    }

    private static object? AggregateCollect(IReadOnlyList<object?> values)
    {
        if (values.Count == 0)
            return null;

        object?[] filtered = new object?[values.Count];
        var filteredCount = 0;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!IsNullish(value))
                filtered[filteredCount++] = value;
        }

        if (filteredCount == 0)
            return null;

        if (filteredCount == filtered.Length)
            return filtered;

        Array.Resize(ref filtered, filteredCount);
        return filtered;
    }

    private static object? AggregateVariance(IReadOnlyList<object?> values, bool sample)
    {
        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            count++;
            var x = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        if (sample && count < 2)
            return null;

        var divisor = sample ? count - 1 : count;
        return m2 / divisor;
    }

    private static object? AggregateCoefficientOfVariation(IReadOnlyList<object?> values)
    {
        var mean = 0d;
        var m2 = 0d;
        var count = 0;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            count++;
            var x = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            var delta = x - mean;
            mean += delta / count;
            m2 += delta * (x - mean);
        }

        if (count == 0)
            return null;

        if (IsNearlyZero(mean))
            return null;

        var variance = m2 / count;

        var stdDev = Math.Sqrt(variance);
        return stdDev / mean;
    }

    private static object? AggregateBoolValues(IReadOnlyList<object?> values, bool useAnd)
    {
        var hasValue = false;
        var acc = useAnd;

        foreach (var value in values)
        {
            if (IsNullish(value))
                continue;

            hasValue = true;
            var current = value!.ToBool();
            acc = useAnd ? acc && current : acc || current;
        }

        return hasValue ? acc : null;
    }

    private enum AggregateNumericOperation
    {
        Sum,
        Average,
        Min,
        Max
    }

    private static object? AggregateNumericValues(IReadOnlyList<object?> values, AggregateNumericOperation operation)
    {
        if (values.Count == 0)
            return null;

        if (operation == AggregateNumericOperation.Sum
            && TryAggregateIntegralSum(values, out var integralSum))
        {
            return integralSum;
        }

        var useDouble = false;
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] is float or double)
            {
                useDouble = true;
                break;
            }
        }

        if (useDouble)
        {
            var numericValues = new double[values.Count];
            for (var i = 0; i < values.Count; i++)
                numericValues[i] = Convert.ToDouble(values[i], CultureInfo.InvariantCulture);

            double sum = 0d;
            double min = numericValues[0];
            double max = numericValues[0];
            for (var i = 0; i < numericValues.Length; i++)
            {
                var current = numericValues[i];
                sum += current;
                if (current < min)
                    min = current;
                if (current > max)
                    max = current;
            }

            return operation switch
            {
                AggregateNumericOperation.Sum => sum,
                AggregateNumericOperation.Average => sum / numericValues.Length,
                AggregateNumericOperation.Min => min,
                AggregateNumericOperation.Max => max,
                _ => null
            };
        }

        var decimalValues = new decimal[values.Count];
        for (var i = 0; i < values.Count; i++)
            decimalValues[i] = values[i]!.ToDec();

        decimal decimalSum = 0m;
        decimal decimalMin = decimalValues[0];
        decimal decimalMax = decimalValues[0];
        for (var i = 0; i < decimalValues.Length; i++)
        {
            var current = decimalValues[i];
            decimalSum += current;
            if (current < decimalMin)
                decimalMin = current;
            if (current > decimalMax)
                decimalMax = current;
        }

        return operation switch
        {
            AggregateNumericOperation.Sum => decimalSum,
            AggregateNumericOperation.Average => decimalSum / decimalValues.Length,
            AggregateNumericOperation.Min => decimalMin,
            AggregateNumericOperation.Max => decimalMax,
            _ => null
        };
    }

    private static bool TryAggregateIntegralSum(IReadOnlyList<object?> values, out object? result)
    {
        result = null;

        long sum = 0;
        try
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (!TryConvertIntegralAggregateValue(values[i], out var numeric))
                    return false;

                sum = checked(sum + numeric);
            }
        }
        catch (OverflowException)
        {
            return false;
        }

        result = sum;
        return true;
    }

    private static bool TryConvertIntegralAggregateValue(object? value, out long result)
    {
        result = default;
        if (value is null || value is DBNull)
            return false;

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
                return false;
        }
    }

    private static object? AggregateMinMaxValues(IReadOnlyList<object?> values, bool useMax)
    {
        if (values.Count == 0)
            return null;

        var best = values[0];
        for (var i = 1; i < values.Count; i++)
        {
            var current = values[i];
            var comparison = CompareAggregateValues(current, best);
            if (useMax ? comparison > 0 : comparison < 0)
            {
                best = current;
            }
        }

        return best;
    }

    private static int CompareAggregateValues(object? left, object? right)
    {
        if (ReferenceEquals(left, right))
            return 0;

        if (left is null)
            return -1;

        if (right is null)
            return 1;

        if (TryConvertNumericToDouble(left, out var leftDouble)
            && TryConvertNumericToDouble(right, out var rightDouble))
        {
            return leftDouble.CompareTo(rightDouble);
        }

        if (left is string leftText && right is string rightText)
            return StringComparer.Ordinal.Compare(leftText, rightText);

        if (left is DateTime leftDateTime && right is DateTime rightDateTime)
            return leftDateTime.CompareTo(rightDateTime);

        if (left is DateTimeOffset leftOffset && right is DateTimeOffset rightOffset)
            return leftOffset.CompareTo(rightOffset);

        if (left is TimeSpan leftSpan && right is TimeSpan rightSpan)
            return leftSpan.CompareTo(rightSpan);

        if (left is string || right is string)
        {
            var leftTextFallback1 = Convert.ToString(left, CultureInfo.InvariantCulture) ?? string.Empty;
            var rightTextFallback1 = Convert.ToString(right, CultureInfo.InvariantCulture) ?? string.Empty;
            return StringComparer.Ordinal.Compare(leftTextFallback1, rightTextFallback1);
        }

        if (left is IComparable leftComparable && left.GetType() == right.GetType())
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                return leftComparable.CompareTo(right);
            }
            catch
            {
                // Falls through to the string fallback for mixed or provider-specific values.
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        if (right is IComparable rightComparable && right.GetType() == left.GetType())
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                return -rightComparable.CompareTo(left);
            }
            catch
            {
                // Falls through to the string fallback for mixed or provider-specific values.
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        var leftTextFallback = Convert.ToString(left, CultureInfo.InvariantCulture) ?? string.Empty;
        var rightTextFallback = Convert.ToString(right, CultureInfo.InvariantCulture) ?? string.Empty;
        return StringComparer.Ordinal.Compare(leftTextFallback, rightTextFallback);
    }

    private static object? AggregateTotal(IReadOnlyList<object?> values)
    {
        var total = 0d;
        var hasValue = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            total += Convert.ToDouble(value, CultureInfo.InvariantCulture);
            hasValue = true;
        }

        return hasValue ? total : 0d;
    }

    private static object? AggregateChecksumValues(IReadOnlyList<object?> values, bool binary)
    {
        var hash = new HashCode();
        var hasValue = false;
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (IsNullish(value))
                continue;

            hasValue = true;
            if (value is byte[] bytes)
            {
                foreach (var b in bytes)
                    hash.Add(b);
                continue;
            }

            if (value is string text)
            {
                var normalized = binary ? text : text.ToUpperInvariant();
                foreach (var ch in normalized)
                    hash.Add(ch);
                continue;
            }

            hash.Add(value);
        }

        if (!hasValue)
            return null;

        return hash.ToHashCode();
    }

    private static string? EvalStringAggregate(
        IReadOnlyList<object?> values,
        object? separatorObj,
        string? defaultSeparator)
    {
        if (values.Count == 0)
            return null;

        var separator = separatorObj?.ToString() ?? defaultSeparator ?? string.Empty;
        var hasSeparator = separator.Length > 0;
        StringBuilder? builder = null;
        if (hasSeparator)
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(values[i], out var text))
                    continue;

                if (builder is null)
                {
                    builder = StringBuilderCache.Acquire(text.Length + separator.Length);
                    builder.Append(text);
                    continue;
                }

                builder.Append(separator);
                builder.Append(text);
            }
        }
        else
        {
            for (var i = 0; i < values.Count; i++)
            {
                if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(values[i], out var text))
                    continue;

                if (builder is null)
                {
                    builder = StringBuilderCache.Acquire(text.Length);
                    builder.Append(text);
                    continue;
                }

                builder.Append(text);
            }
        }

        return builder is null
            ? null
            : StringBuilderCache.GetStringAndRelease(builder);
    }

    private static string? EvalSimpleStringAggregate(
        QueryExecutionContext context,
        CallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        object? separatorObj,
        string? defaultSeparator)
    {
        if (fn.Args.Count == 0)
            return null;

        var hasDirectValueSelector = TryCreateStringAggregateValueSelector(fn.Args[0], out var valueSelector);
        List<AstQueryExecutorBase.EvalRow> rows = group.Rows;
        var rowCount = rows.Count;
        if (rowCount == 0)
            return null;

        if (fn.WithinGroupOrderBy is { Count: > 0 } orderBy && rowCount > 1)
            rows = OrderStringAggregateRows(context, orderBy, group.Rows, ctes, eval);

        rowCount = rows.Count;
        if (rowCount == 1)
        {
            var singleValue = hasDirectValueSelector
                ? valueSelector!(rows[0])
                : eval(fn.Args[0], rows[0], null, ctes);
            if (fn.Distinct)
            {
                if (!AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(singleValue, useOrdinalTextComparison: true, out var singleText1, out _))
                    return null;

                return singleText1;
            }

            if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(singleValue, out var singleText))
                return null;

            return singleText;
        }

        var separator = separatorObj?.ToString() ?? defaultSeparator ?? string.Empty;
        var hasSeparator = separator.Length > 0;
        StringBuilder? builder = null;
        var hasValue = false;
        var estimatedCapacity = EstimateStringAggregateCapacity(rowCount, separator.Length);
        HashSet<string>? seen = fn.Distinct && rowCount > 1
            ? new HashSet<string>(StringComparer.Ordinal)
            : null;

        if (!hasDirectValueSelector)
        {
            if (hasSeparator)
            {
                for (var i = 0; i < rowCount; i++)
                {
                    var value = eval(fn.Args[0], rows[i], null, ctes);
                    if (IsNullish(value))
                        continue;

                    var text = string.Empty;
                    if (seen is not null)
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison: true, out text, out var key)
                            || !seen.Add(key))
                            continue;
                    }
                    else
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(value, out text))
                            continue;
                    }

                    if (!hasValue)
                    {
                        builder = StringBuilderCache.Acquire(Math.Max(estimatedCapacity, text.Length));
                        builder.Append(text);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(text);
                }
            }
            else
            {
                for (var i = 0; i < rowCount; i++)
                {
                    var value = eval(fn.Args[0], rows[i], null, ctes);
                    if (IsNullish(value))
                        continue;

                    var text = string.Empty;
                    if (seen is not null)
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison: true, out text, out var key)
                            || !seen.Add(key))
                            continue;
                    }
                    else
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(value, out text))
                            continue;
                    }

                    if (!hasValue)
                    {
                        builder = StringBuilderCache.Acquire(Math.Max(estimatedCapacity, text.Length));
                        builder.Append(text);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(text);
                }
            }
        }
        else
        {
            if (hasSeparator)
            {
                for (var i = 0; i < rowCount; i++)
                {
                    var value = valueSelector!(rows[i]);
                    if (IsNullish(value))
                        continue;

                    var text = string.Empty;
                    if (seen is not null)
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison: true, out text, out var key)
                            || !seen.Add(key))
                            continue;
                    }
                    else
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(value, out text))
                            continue;
                    }

                    if (!hasValue)
                    {
                        builder = StringBuilderCache.Acquire(Math.Max(estimatedCapacity, text.Length));
                        builder.Append(text);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(separator);
                    builder.Append(text);
                }
            }
            else
            {
                for (var i = 0; i < rowCount; i++)
                {
                    var value = valueSelector!(rows[i]);
                    if (IsNullish(value))
                        continue;

                    var text = string.Empty;
                    if (seen is not null)
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateKeyAndText(value, useOrdinalTextComparison: true, out text, out var key)
                            || !seen.Add(key))
                            continue;
                    }
                    else
                    {
                        if (!AstQueryAggregateKeyHelper.TryGetStringAggregateText(value, out text))
                            continue;
                    }

                    if (!hasValue)
                    {
                        builder = StringBuilderCache.Acquire(Math.Max(estimatedCapacity, text.Length));
                        builder.Append(text);
                        hasValue = true;
                        continue;
                    }

                    builder!.Append(text);
                }
            }
        }

        return hasValue ? StringBuilderCache.GetStringAndRelease(builder!) : null;
    }

    private static List<AstQueryExecutorBase.EvalRow> OrderStringAggregateRows(
        QueryExecutionContext context,
        IReadOnlyList<WindowOrderItem> orderBy,
        List<AstQueryExecutorBase.EvalRow> rows,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var rowCount = rows.Count;
        var orderedIndexes = new int[rowCount];
        for (var i = 0; i < rowCount; i++)
            orderedIndexes[i] = i;

        var orderValuesByIndex = WindowOrderValueHelper.BuildWindowOrderValuesByIndex(
            rows,
            orderBy,
            (expr, row) => eval(expr, row, null, ctes));
        var orderByCount = orderBy.Count;

        if (orderByCount == 1)
        {
            var orderItem = orderBy[0];
            Array.Sort(orderedIndexes, (leftIndex, rightIndex) =>
            {
                var comparison = context.CompareSql(orderValuesByIndex[leftIndex][0], orderValuesByIndex[rightIndex][0]);
                if (comparison != 0)
                    return orderItem.Desc ? -comparison : comparison;

                return leftIndex.CompareTo(rightIndex);
            });
            var orderedRows1 = new List<AstQueryExecutorBase.EvalRow>(rowCount);
            for (var i = 0; i < rowCount; i++)
                orderedRows1.Add(rows[orderedIndexes[i]]);

            return orderedRows1;
        }

        Array.Sort(orderedIndexes, (leftIndex, rightIndex) =>
        {
            var leftValues = orderValuesByIndex[leftIndex];
            var rightValues = orderValuesByIndex[rightIndex];
            for (var i = 0; i < orderByCount; i++)
            {
                var comparison = context.CompareSql(leftValues[i], rightValues[i]);
                if (comparison != 0)
                    return orderBy[i].Desc ? -comparison : comparison;
            }

            return leftIndex.CompareTo(rightIndex);
        });

        var orderedRows2 = new List<AstQueryExecutorBase.EvalRow>(rowCount);
        for (var i = 0; i < rowCount; i++)
            orderedRows2.Add(rows[orderedIndexes[i]]);

        return orderedRows2;
    }

    private static string? GetStringAggregateDefaultSeparator(string name)
        => name == SqlConst.LISTAGG ? string.Empty : ",";

    private static object? GetAggregateSeparator(
        IReadOnlyList<SqlExpr> args,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (args.Count <= 1 || group.Rows.Count == 0)
            return null;

        if (TryGetAggregateSeparatorText(args[1], out var separatorText))
            return separatorText;

        return eval(args[1], group.Rows[0], null, ctes);
    }

    private static bool TryGetAggregateSeparatorText(SqlExpr expr, out string separator)
    {
        separator = string.Empty;

        switch (expr)
        {
            case LiteralExpr literal:
                separator = literal.Value?.ToString() ?? string.Empty;
                return true;
            case RawSqlExpr raw:
                separator = raw.Sql;
                return true;
            case IdentifierExpr identifier:
                separator = identifier.Name;
                return true;
            case ColumnExpr column:
                separator = column.Name;
                return true;
            default:
                return false;
        }
    }

    private static int EstimateStringAggregateCapacity(int rowCount, int separatorLength)
    {
        if (rowCount <= 1)
            return 16;

        var estimated = rowCount * Math.Max(12, separatorLength + 8);
        return Math.Min(estimated, 256 * 1024);
    }

    private static class StringBuilderCache
    {
        [ThreadStatic]
        private static StringBuilder? _cachedInstance;

        internal static StringBuilder Acquire(int capacity)
        {
            var builder = _cachedInstance;
            if (builder is null)
                return new StringBuilder(capacity);

            _cachedInstance = null;
            builder.Clear();
            if (builder.Capacity < capacity)
                builder.Capacity = capacity;

            return builder;
        }

        internal static string GetStringAndRelease(StringBuilder builder)
        {
            var result = builder.ToString();
            if (builder.Capacity <= 64 * 1024)
                _cachedInstance = builder;
            return result;
        }
    }

    internal static int GetKnownRowCount(IEnumerable<EvalRow> rows, int defaultValue = 0)
    {
        if (rows is ICollection<EvalRow> collection)
            return collection.Count;

        if (rows is IReadOnlyCollection<EvalRow> readOnlyCollection)
            return readOnlyCollection.Count;

        return defaultValue;
    }

    private static bool TryCreateStringAggregateValueSelector(
        SqlExpr expr,
        out Func<EvalRow, object?> selector)
    {
        switch (expr)
        {
            case ColumnExpr column:
                selector = row => QueryRowValueHelper.ResolveColumn(column.Qualifier, column.Name, row);
                return true;
            case IdentifierExpr identifier:
                selector = row => QueryRowValueHelper.ResolveIdentifier(identifier.Name, row);
                return true;
            case LiteralExpr literal:
                selector = _ => literal.Value;
                return true;
            default:
                selector = null!;
                return false;
        }
    }

    private static bool TryEvalAggregateCount(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        string name,
        out object? value)
    {
        if (name != SqlConst.COUNT && name != SqlConst.COUNT_BIG)
        {
            value = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            value = CreateCountAggregateResult(context, name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase), group.Rows.Count);
            return true;
        }

        if (fn.Args.Count == 1 && fn.Args[0] is StarExpr)
        {
            value = CreateCountAggregateResult(context, name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase), group.Rows.Count);
            return true;
        }

        var distinct = fn.Distinct;
        HashSet<string>? seen = distinct ? new HashSet<string>(StringComparer.Ordinal) : null;
        long c = 0;
        foreach (var r in group.Rows)
        {
            var v = eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v))
            {
                if (seen is not null)
                {
                    var key = context.NormalizeDistinctKey(v);
                    if (!seen.Add(key))
                        continue;
                }

                c++;
            }
        }

        value = CreateCountAggregateResult(context, name.Equals(SqlConst.COUNT_BIG, StringComparison.OrdinalIgnoreCase), c);
        return true;
    }

    internal static object CreateCountAggregateResult(
        QueryExecutionContext context,
        bool isCountBig,
        long value)
    {
        if (!isCountBig
            && context.Dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
        {
            return checked((int)value);
        }

        if (!isCountBig
            && context.Dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase))
        {
            return (decimal)value;
        }

        return value;
    }

    private static List<object?>? TryGetAggregateValues(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<object?>(group.Rows.Count);
        HashSet<string>? seen = null;
        if (fn.Distinct)
        {
            seen = new HashSet<string>(StringComparer.Ordinal);
        }
        var traceGroupedCaseWhen = fn.Name.Equals(SqlConst.SUM, StringComparison.OrdinalIgnoreCase)
            && fn.Args.Count > 0
            && ContainsParameter(fn.Args[0], "cutoff");
        var rowIndex = 0;
        foreach (var r in group.Rows)
        {
            var v = eval(fn.Args[0], r, null, ctes);
            if (traceGroupedCaseWhen)
            {
                Console.WriteLine(
                    $"[AggDebug][SUM][{rowIndex}] value={v ?? "NULL"} row={string.Join(", ", r.Fields.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}"))}");
            }
            if (!IsNullish(v))
            {
                if (seen is not null)
                {
                    var key = context.NormalizeDistinctKey(v);
                    if (!seen.Add(key))
                        continue;
                }
                values.Add(v);
            }

            rowIndex++;
        }
        return values;
    }

    private static bool ContainsParameter(SqlExpr expression, string parameterName)
        => expression switch
        {
            ParameterExpr parameter => parameter.Name.TrimStart('@', ':', '?')
                .Equals(parameterName, StringComparison.OrdinalIgnoreCase),
            BinaryExpr binary => ContainsParameter(binary.Left, parameterName) || ContainsParameter(binary.Right, parameterName),
            UnaryExpr unary => ContainsParameter(unary.Expr, parameterName),
            CaseExpr caseExpr => (caseExpr.BaseExpr is not null && ContainsParameter(caseExpr.BaseExpr, parameterName))
                || caseExpr.Whens.Any(when => ContainsParameter(when.When, parameterName) || ContainsParameter(when.Then, parameterName))
                || (caseExpr.ElseExpr is not null && ContainsParameter(caseExpr.ElseExpr, parameterName)),
            FunctionCallExpr functionCall => functionCall.Args.Any(arg => ContainsParameter(arg, parameterName)),
            CallExpr call => call.Args.Any(arg => ContainsParameter(arg, parameterName)),
            LikeExpr likeExpr => ContainsParameter(likeExpr.Left, parameterName)
                || ContainsParameter(likeExpr.Pattern, parameterName)
                || (likeExpr.Escape is not null && ContainsParameter(likeExpr.Escape, parameterName)),
            InExpr inExpr => ContainsParameter(inExpr.Left, parameterName)
                || inExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            IsNullExpr isNullExpr => ContainsParameter(isNullExpr.Expr, parameterName),
            BetweenExpr betweenExpr => ContainsParameter(betweenExpr.Expr, parameterName)
                || ContainsParameter(betweenExpr.Low, parameterName)
                || ContainsParameter(betweenExpr.High, parameterName),
            RowExpr rowExpr => rowExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            _ => false
        };

    private static string FormatDebugValue(object? value)
        => value is null or DBNull
            ? "NULL"
            : $"{value} ({value.GetType().Name})";

    private static bool IsNearlyZero(double value, double epsilon = 1e-12)
    {
        return Math.Abs(value) <= epsilon * Math.Max(1.0, Math.Abs(value));
    }
}
