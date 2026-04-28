using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQuerySubqueryLookupSupport
{
    internal static bool TryBuildCorrelatedLookupCompositeKey(
        IReadOnlyList<CorrelatedLookupKeyPair> keyPairs,
        EvalRow row,
        IDictionary<string, Source> ctes,
        bool useInnerSide,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        out string key)
    {
        key = string.Empty;

        var keyPairCount = keyPairs.Count;
        if (keyPairCount == 0)
            return false;

        if (keyPairCount == 1)
        {
            var pair = keyPairs[0];
            var expr = useInnerSide ? pair.InnerExpr : pair.OuterExpr;
            var value = eval(expr, row, null, ctes);
            if (value is null || value is DBNull)
                return false;

            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            key = BuildLookupScalarKeyComponent(component);
            return true;
        }

        var sb = new StringBuilder(keyPairCount * 32);
        if (useInnerSide)
        {
            for (var i = 0; i < keyPairCount; i++)
            {
                var pair = keyPairs[i];
                var expr = pair.InnerExpr;
                var value = eval(expr, row, null, ctes);
                if (value is null || value is DBNull)
                    return false;

                if (!TryCreateInLookupScalarKey(value, null, out var component))
                    return false;

                AppendLookupScalarKeyComponent(sb, component);
            }
        }
        else
        {
            for (var i = 0; i < keyPairCount; i++)
            {
                var pair = keyPairs[i];
                var expr = pair.OuterExpr;
                var value = eval(expr, row, null, ctes);
                if (value is null || value is DBNull)
                    return false;

                if (!TryCreateInLookupScalarKey(value, null, out var component))
                    return false;

                AppendLookupScalarKeyComponent(sb, component);
            }
        }

        key = sb.ToString();
        return true;
    }

    internal static string BuildCorrelatedSubqueryCacheKey(string operation, string? subquerySql, EvalRow row)
    {
        var cacheKey = string.Concat(operation, '\u001F', subquerySql ?? string.Empty);
        var cachedKeys = row.CorrelatedCacheKeys;
        if (cachedKeys is not null && cachedKeys.TryGetValue(cacheKey, out var cachedValue))
            return cachedValue;

        var built = AstCorrelatedSubqueryCacheKeyBuilder.Build(operation, subquerySql, row);
        row.CorrelatedCacheKeys ??= new Dictionary<string, string>(StringComparer.Ordinal);
        row.CorrelatedCacheKeys[cacheKey] = built;
        return built;
    }

    internal static SqlQueryBase LimitToSingleRow(SqlQueryBase query)
        => query switch
        {
            SqlSelectQuery select => select with
            {
                RowLimit = CreateSingleRowLimit(select.RowLimit)
            },
            SqlUnionQuery union => union with
            {
                RowLimit = CreateSingleRowLimit(union.RowLimit)
            },
            _ => query
        };

    private static SqlRowLimit CreateSingleRowLimit(SqlRowLimit? current)
        => current switch
        {
            SqlLimitOffset limit => new SqlLimitOffset(new LiteralExpr(1m), limit.Offset),
            SqlTop => new SqlTop(new LiteralExpr(1m)),
            SqlFetch fetch => new SqlFetch(new LiteralExpr(1m), fetch.Offset),
            _ => new SqlLimitOffset(new LiteralExpr(1m), null)
        };

    internal static bool TryCreateInLookupScalarKey(object? value, ISqlDialect? dialect, out InLookupScalarKey key)
    {
        key = default;

        if (value is null or DBNull)
            return false;

        if (value is byte[] || value is object?[])
            return false;

        if (value is string text)
        {
            key = new InLookupScalarKey("s", NormalizeLookupText(text, dialect));
            return true;
        }

        if (value is char character)
        {
            key = new InLookupScalarKey("s", NormalizeLookupText(character.ToString(), dialect));
            return true;
        }

        if (value is bool boolean)
        {
            key = new InLookupScalarKey("b", boolean ? "1" : "0");
            return true;
        }

        if (TryConvertLookupNumericValue(value, out var numericValue))
        {
            key = new InLookupScalarKey("n", numericValue.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is DateTime dateTime)
        {
            key = new InLookupScalarKey("dt", dateTime.Ticks.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is DateTimeOffset dateTimeOffset)
        {
            key = new InLookupScalarKey("dto", dateTimeOffset.Ticks.ToString(CultureInfo.InvariantCulture));
            return true;
        }

        if (value is Guid guid)
        {
            key = new InLookupScalarKey("g", guid.ToString("D"));
            return true;
        }

        var type = value.GetType();
        if (type.IsEnum)
        {
            key = new InLookupScalarKey("e", Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture));
            return true;
        }

        return false;
    }

    internal static bool TryConvertLookupNumericValue(object value, out decimal numericValue)
    {
        try
        {
            switch (value)
            {
                case byte byteValue:
                    numericValue = byteValue;
                    return true;
                case sbyte sbyteValue:
                    numericValue = sbyteValue;
                    return true;
                case short shortValue:
                    numericValue = shortValue;
                    return true;
                case ushort ushortValue:
                    numericValue = ushortValue;
                    return true;
                case int intValue:
                    numericValue = intValue;
                    return true;
                case uint uintValue:
                    numericValue = uintValue;
                    return true;
                case long longValue:
                    numericValue = longValue;
                    return true;
                case ulong ulongValue:
                    numericValue = ulongValue;
                    return true;
                case float floatValue:
                    numericValue = Convert.ToDecimal(floatValue, CultureInfo.InvariantCulture);
                    return true;
                case double doubleValue:
                    numericValue = Convert.ToDecimal(doubleValue, CultureInfo.InvariantCulture);
                    return true;
                case decimal decimalValue:
                    numericValue = decimalValue;
                    return true;
                case string textValue when decimal.TryParse(textValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed):
                    numericValue = parsed;
                    return true;
                default:
                    numericValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                    return true;
            }
        }
        catch
        {
            numericValue = default;
            return false;
        }
    }

    internal static bool TryBuildInLookupCompositeKey(
        IReadOnlyList<object?> values,
        out string key)
    {
        key = string.Empty;

        var valuesCount = values.Count;
        if (valuesCount == 0)
            return false;

        var sb = new StringBuilder(valuesCount * 32);
        for (var i = 0; i < valuesCount; i++)
        {
            var value = values[i];
            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            AppendLookupScalarKeyComponent(sb, component);
        }

        key = sb.ToString();
        return true;
    }

    internal static void AppendLookupScalarKeyComponent(StringBuilder sb, InLookupScalarKey component)
    {
        var kindLength = component.Kind.Length;
        var valueLength = component.Value.Length;

        if (sb.Length > 0)
            sb.Append('|');

        sb.Append(kindLength);
        sb.Append(':');
        sb.Append(component.Kind);
        sb.Append(';');
        sb.Append(valueLength);
        sb.Append(':');
        sb.Append(component.Value);
    }

    private static string BuildLookupScalarKeyComponent(InLookupScalarKey component)
        => string.Concat(
            component.Kind.Length.ToString(CultureInfo.InvariantCulture),
            ":",
            component.Kind,
            ";",
            component.Value.Length.ToString(CultureInfo.InvariantCulture),
            ":",
            component.Value);

    internal static string BuildLookupScalarKeyString(InLookupScalarKey component)
        => BuildLookupScalarKeyComponent(component);

    internal static string NormalizeLookupText(string value, ISqlDialect? dialect)
        => (dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase) == StringComparison.Ordinal
            ? value
            : value.ToUpperInvariant();

    internal static void FlattenConjuncts(SqlExpr expr, List<SqlExpr> conjuncts)
    {
        if (expr is BinaryExpr binary && binary.Op == SqlBinaryOp.And)
        {
            FlattenConjuncts(binary.Left, conjuncts);
            FlattenConjuncts(binary.Right, conjuncts);
            return;
        }

        conjuncts.Add(expr);
    }

    internal static SqlExpr CombineConjuncts(IReadOnlyList<SqlExpr> conjuncts)
    {
        var conjunctCount = conjuncts.Count;
        if (conjunctCount == 0)
            throw new InvalidOperationException("Nenhum conjuncto para combinar.");

        if (conjunctCount == 1)
            return conjuncts[0];

        var combined = conjuncts[0];
        for (var i = 1; i < conjunctCount; i++)
            combined = new BinaryExpr(SqlBinaryOp.And, combined, conjuncts[i]);

        return combined;
    }
}
