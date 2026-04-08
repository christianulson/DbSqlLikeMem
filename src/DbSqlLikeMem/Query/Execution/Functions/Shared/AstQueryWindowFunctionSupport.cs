using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryWindowFunctionSupport
{
    internal enum WindowFunctionKind
    {
        Other,
        RowNumber,
        Rank,
        DenseRank,
        Ntile,
        PercentRank,
        CumeDist,
        Lag,
        Lead,
        FirstValue,
        LastValue,
        NthValue,
        Count
    }

    internal static bool SupportsWindowFrame(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return AggregateFunctionCatalog.Contains(functionName)
            || functionName.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase);
    }

    internal static WindowFunctionKind ClassifyWindowFunction(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return WindowFunctionKind.Other;

        if (functionName.Equals("RANK", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.Rank;

        if (functionName.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.DenseRank;

        if (functionName.Equals("NTILE", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.Ntile;

        if (functionName.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.PercentRank;

        if (functionName.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.CumeDist;

        if (functionName.Equals("LAG", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.Lag;

        if (functionName.Equals("LEAD", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.Lead;

        if (functionName.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.FirstValue;

        if (functionName.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.LastValue;

        if (functionName.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.NthValue;

        if (functionName.Equals("COUNT", StringComparison.OrdinalIgnoreCase))
            return WindowFunctionKind.Count;

        return functionName.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase)
            ? WindowFunctionKind.RowNumber
            : WindowFunctionKind.Other;
    }

    internal static bool TryReadIntLiteral(SqlExpr expr, out int value)
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

    internal static bool TryReadLongLiteral(SqlExpr expr, out long value)
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

    internal static int ResolveNthValueIndex(
        IReadOnlyList<SqlExpr> args,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = eval(args[1], sampleRow, null, ctes);
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

    internal static int ResolveLagLeadOffset(
        IReadOnlyList<SqlExpr> args,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (args.Count < 2)
            return 1;

        if (TryReadIntLiteral(args[1], out var parsedLiteral) && parsedLiteral >= 0)
            return parsedLiteral;

        var evaluated = eval(args[1], sampleRow, null, ctes);
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

    internal static long ResolveNtileBucketCount(
        WindowFunctionExpr windowFunctionExpr,
        int partitionSize,
        EvalRow sampleRow,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (partitionSize <= 0)
            return 0;

        if (windowFunctionExpr.Args.Count == 0)
            return 1;

        var arg = windowFunctionExpr.Args[0];
        if (TryReadLongLiteral(arg, out var parsedLiteral) && parsedLiteral > 0)
            return parsedLiteral;

        var evaluated = eval(arg, sampleRow, null, ctes);
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
}
