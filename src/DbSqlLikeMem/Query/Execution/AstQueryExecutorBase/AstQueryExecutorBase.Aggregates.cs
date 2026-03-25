namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private object? EvalCollectedAggregateValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name,
        IReadOnlyList<object?> values)
    {
        var separator = GetAggregateSeparator(fn, group, ctes);
        return name switch
        {
            "SUM" => AggregateNumericValues(values, AggregateNumericOperation.Sum),
            "AVG" => AggregateNumericValues(values, AggregateNumericOperation.Average),
            "MIN" => AggregateNumericValues(values, AggregateNumericOperation.Min),
            "MAX" => AggregateNumericValues(values, AggregateNumericOperation.Max),
            "CHECKSUM_AGG" => AggregateChecksumValues(values, binary: false),
            "GROUP_CONCAT" => EvalStringAggregate(values, separator, ","),
            "STRING_AGG" => EvalStringAggregate(values, separator, ","),
            "LISTAGG" => EvalStringAggregate(values, separator, string.Empty),
            "ANY_VALUE" => AggregateAnyValue(values),
            "BIT_AND" => AggregateBitwiseValues(values, BitwiseAggregateOperation.And),
            "BIT_OR" => AggregateBitwiseValues(values, BitwiseAggregateOperation.Or),
            "BIT_XOR" => AggregateBitwiseValues(values, BitwiseAggregateOperation.Xor),
            "JSON_ARRAYAGG" => EvalJsonArrayAggregate(values),
            "JSON_AGG" => EvalJsonArrayAggregate(values),
            "JSONB_AGG" => EvalJsonArrayAggregate(values),
            "ARRAY_AGG" => AggregateCollect(values),
            "BOOL_AND" => AggregateBoolValues(values, useAnd: true),
            "EVERY" => AggregateBoolValues(values, useAnd: true),
            "BOOL_OR" => AggregateBoolValues(values, useAnd: false),
            "COLLECT" => AggregateCollect(values),
            "TOTAL" => AggregateTotal(values),
            "STDEV" => AggregateVariance(values, sample: true) is double stdev ? Math.Sqrt(stdev) : null,
            "STDEVP" => AggregateVariance(values, sample: false) is double stdevp ? Math.Sqrt(stdevp) : null,
            "VAR" => AggregateVariance(values, sample: true),
            "VARP" => AggregateVariance(values, sample: false),
            "VAR_POP" => AggregateVariance(values, sample: false),
            "VARIANCE" => AggregateVariance(values, sample: false),
            "VARIANCE_SAMP" => AggregateVariance(values, sample: true),
            "VAR_SAMP" => AggregateVariance(values, sample: true),
            "CV" => AggregateCoefficientOfVariation(values),
            _ => null
        };
    }

    private static int? AggregateChecksumValues(IReadOnlyList<object?> values, bool binary)
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

    private object? EvalJsonGroupObjectAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count < 2)
            return null;

        var obj = new System.Text.Json.Nodes.JsonObject();
        foreach (var row in group.Rows)
        {
            var keyValue = Eval(fn.Args[0], row, null, ctes);
            if (IsNullish(keyValue))
                continue;

            var key = keyValue?.ToString() ?? string.Empty;
            var value = Eval(fn.Args[1], row, null, ctes);
            obj[key] = CreateJsonNodeFromValue(value);
        }

        return obj.ToJsonString();
    }

    private object? EvalPercentileAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<double>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var value = Eval(fn.Args[0], row, null, ctes);
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
            var percentileValue = Eval(fn.Args[1], group.Rows[0], null, ctes);
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

    private object? EvalCorrelationAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = Eval(fn.Args[0], row, null, ctes);
            var yValue = Eval(fn.Args[1], row, null, ctes);
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

        var meanX = pairs.Average(p => p.X);
        var meanY = pairs.Average(p => p.Y);
        var sumXY = pairs.Sum(p => (p.X - meanX) * (p.Y - meanY));

        if (name is "COVAR_POP")
            return sumXY / pairs.Count;

        if (name is "COVAR_SAMP")
            return pairs.Count < 2 ? null : sumXY / (pairs.Count - 1);

        var sumXX = pairs.Sum(p =>
        {
            var dx = p.X - meanX;
            return dx * dx;
        });
        var sumYY = pairs.Sum(p =>
        {
            var dy = p.Y - meanY;
            return dy * dy;
        });

        if (sumXX == 0d || sumYY == 0d)
            return null;

        return sumXY / Math.Sqrt(sumXX * sumYY);
    }

    private object? EvalApproxAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count == 0)
            return null;

        if (name is "APPROX_MEDIAN")
            return EvalPercentileAggregate(fn, group, ctes, "MEDIAN");

        if (name is "APPROX_PERCENTILE" or "APPROX_PERCENTILE_AGG" or "APPROX_PERCENTILE_DETAIL")
            return EvalPercentileAggregate(fn, group, ctes, "PERCENTILE_CONT");

        if (name is "APPROX_COUNT_DISTINCT" or "APPROX_COUNT_DISTINCT_AGG" or "APPROX_COUNT_DISTINCT_DETAIL")
        {
            var set = new HashSet<object?>();
            foreach (var row in group.Rows)
            {
                var value = Eval(fn.Args[0], row, null, ctes);
                if (!IsNullish(value))
                    set.Add(value);
            }
            return set.Count;
        }

        return null;
    }

    private object? EvalRegressionAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        if (fn.Args.Count < 2)
            return null;

        var pairs = new List<(double X, double Y)>(group.Rows.Count);
        foreach (var row in group.Rows)
        {
            var xValue = Eval(fn.Args[0], row, null, ctes);
            var yValue = Eval(fn.Args[1], row, null, ctes);
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

        var meanX = pairs.Average(p => p.X);
        var meanY = pairs.Average(p => p.Y);
        var sumXX = pairs.Sum(p =>
        {
            var dx = p.X - meanX;
            return dx * dx;
        });
        var sumYY = pairs.Sum(p =>
        {
            var dy = p.Y - meanY;
            return dy * dy;
        });
        var sumXY = pairs.Sum(p => (p.X - meanX) * (p.Y - meanY));

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

    private object? EvalStdDevAggregate(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes,
        string name)
    {
        var values = TryGetAggregateValues(fn, group, ctes);
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


        return BuildJsonArray(values);
    }

    private static object? AggregateCollect(IReadOnlyList<object?> values)
    {
        if (values.Count == 0)
            return null;

        var filtered = new List<object?>(values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!IsNullish(value))
                filtered.Add(value);
        }

        return filtered.Count == 0 ? null : filtered.ToArray();
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

    private static object? AggregateNumericValues(IReadOnlyList<object?> values, AggregateNumericOperation operation)
    {
        if (values.Count == 0)
            return null;

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

    private enum AggregateNumericOperation
    {
        Sum,
        Average,
        Min,
        Max
    }

    private List<object?>? TryGetAggregateValues(
        FunctionCallExpr fn,
        EvalGroup group,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        var values = new List<object?>(group.Rows.Count);
        foreach (var r in group.Rows)
        {
            var v = Eval(fn.Args[0], r, null, ctes);
            if (!IsNullish(v))
                values.Add(v);
        }
        return values;
    }

    private static bool IsNearlyZero(double value, double epsilon = 1e-12)
    {
        // absolute + relative tolerance: handles values near zero and large magnitudes
        return Math.Abs(value) <= epsilon * Math.Max(1.0, Math.Abs(value));
    }
}
