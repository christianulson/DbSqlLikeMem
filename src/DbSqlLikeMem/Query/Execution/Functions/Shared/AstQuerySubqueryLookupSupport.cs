using System.Text;
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

        if (keyPairs.Count == 0)
            return false;

        var sb = new StringBuilder();
        for (var i = 0; i < keyPairs.Count; i++)
        {
            var expr = useInnerSide ? keyPairs[i].InnerExpr : keyPairs[i].OuterExpr;
            var value = eval(expr, row, null, ctes);
            if (value is null || value is DBNull)
                return false;

            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            AppendLookupScalarKeyComponent(sb, component);
        }

        key = sb.ToString();
        return true;
    }

    internal static string BuildCorrelatedSubqueryCacheKey(string operation, string? subquerySql, EvalRow row)
        => AstCorrelatedSubqueryCacheKeyBuilder.Build(operation, subquerySql, row);

    internal static SqlSelectQuery LimitToSingleRow(SqlSelectQuery query)
        => query with
        {
            RowLimit = query.RowLimit switch
            {
                SqlLimitOffset limit => new SqlLimitOffset(new LiteralExpr(1m), limit.Offset),
                SqlTop => new SqlTop(new LiteralExpr(1m)),
                SqlFetch fetch => new SqlFetch(new LiteralExpr(1m), fetch.Offset),
                _ => new SqlLimitOffset(new LiteralExpr(1m), null)
            }
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

        if (values.Count == 0)
            return false;

        var sb = new StringBuilder();
        foreach (var value in values)
        {
            if (!TryCreateInLookupScalarKey(value, null, out var component))
                return false;

            AppendLookupScalarKeyComponent(sb, component);
        }

        key = sb.ToString();
        return true;
    }

    internal static void AppendLookupScalarKeyComponent(StringBuilder sb, InLookupScalarKey component)
    {
        if (sb.Length > 0)
            sb.Append('|');

        sb.Append(component.Kind.Length);
        sb.Append(':');
        sb.Append(component.Kind);
        sb.Append(';');
        sb.Append(component.Value.Length);
        sb.Append(':');
        sb.Append(component.Value);
    }

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
        if (conjuncts.Count == 0)
            throw new InvalidOperationException("Nenhum conjuncto para combinar.");

        var combined = conjuncts[0];
        for (var i = 1; i < conjuncts.Count; i++)
            combined = new BinaryExpr(SqlBinaryOp.And, combined, conjuncts[i]);

        return combined;
    }
}
