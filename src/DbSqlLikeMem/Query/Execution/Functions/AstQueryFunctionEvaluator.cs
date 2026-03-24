using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalUserDefinedScalarFunction(
    FunctionCallExpr fn,
    EvalRow row,
    EvalGroup? group,
    IDictionary<string, Source> ctes,
    out object? result);

internal delegate bool AstQueryTryEvalFunctionFamily(
    FunctionCallExpr fn,
    EvalRow row,
    EvalGroup? group,
    IDictionary<string, Source> ctes,
    ISqlDialect dialect,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQueryFunctionEvaluator(
    Func<string, bool> isAggregateFunction,
    Func<FunctionCallExpr, EvalGroup, IDictionary<string, Source>, object?> evalAggregate,
    AstQueryTryEvalUserDefinedScalarFunction tryEvalUserDefinedScalarFunction,
    AstQueryGeneralScalarFunctionHandler tryEvalBoundScalarFunction,
    AstQueryTryEvalFunctionFamily tryEvalNonSqlServerScalarFunctionFamily,
    AstQueryTryEvalFunctionFamily tryEvalSqlServerAndCompatibilityFunctionFamily,
    AstQueryTryEvalFunctionFamily tryEvalGeneralScalarFunctionFamily,
    AstQueryTryEvalFunctionFamily tryEvalCastStringAndDateTail)
{
    private readonly Func<string, bool> _isAggregateFunction = isAggregateFunction ?? throw new ArgumentNullException(nameof(isAggregateFunction));
    private readonly Func<FunctionCallExpr, EvalGroup, IDictionary<string, Source>, object?> _evalAggregate = evalAggregate ?? throw new ArgumentNullException(nameof(evalAggregate));
    private readonly AstQueryTryEvalUserDefinedScalarFunction _tryEvalUserDefinedScalarFunction = tryEvalUserDefinedScalarFunction ?? throw new ArgumentNullException(nameof(tryEvalUserDefinedScalarFunction));
    private readonly AstQueryGeneralScalarFunctionHandler _tryEvalBoundScalarFunction = tryEvalBoundScalarFunction ?? throw new ArgumentNullException(nameof(tryEvalBoundScalarFunction));
    private readonly AstQueryTryEvalFunctionFamily _tryEvalNonSqlServerScalarFunctionFamily = tryEvalNonSqlServerScalarFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalNonSqlServerScalarFunctionFamily));
    private readonly AstQueryTryEvalFunctionFamily _tryEvalSqlServerAndCompatibilityFunctionFamily = tryEvalSqlServerAndCompatibilityFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalSqlServerAndCompatibilityFunctionFamily));
    private readonly AstQueryTryEvalFunctionFamily _tryEvalGeneralScalarFunctionFamily = tryEvalGeneralScalarFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalGeneralScalarFunctionFamily));
    private readonly AstQueryTryEvalFunctionFamily _tryEvalCastStringAndDateTail = tryEvalCastStringAndDateTail ?? throw new ArgumentNullException(nameof(tryEvalCastStringAndDateTail));

    internal object? Evaluate(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        ISqlDialect dialect,
        DateTime localNow,
        DateTime utcNow,
        Func<int, object?> evalArg)
    {
        if (group is not null && _isAggregateFunction(fn.Name))
            return _evalAggregate(fn, group, ctes);

        if (_tryEvalUserDefinedScalarFunction(fn, row, group, ctes, out var userDefinedResult))
            return userDefinedResult;

        if (fn.Args.Count == 0
            && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(
                dialect,
                fn.Name,
                localNow,
                utcNow,
                out var temporalValue))
        {
            return temporalValue;
        }

        if (_tryEvalBoundScalarFunction(fn, dialect, evalArg, out var boundScalarResult))
            return boundScalarResult;

        if (_tryEvalNonSqlServerScalarFunctionFamily(fn, row, group, ctes, dialect, evalArg, out var nonSqlServerResult))
            return nonSqlServerResult;

        if (_tryEvalSqlServerAndCompatibilityFunctionFamily(fn, row, group, ctes, dialect, evalArg, out var sqlServerCompatibilityResult))
            return sqlServerCompatibilityResult;

        if (_tryEvalGeneralScalarFunctionFamily(fn, row, group, ctes, dialect, evalArg, out var generalScalarResult))
            return generalScalarResult;

        if (_tryEvalCastStringAndDateTail(fn, row, group, ctes, dialect, evalArg, out var castStringAndDateTailResult))
            return castStringAndDateTailResult;

        if (fn.Args.Count == 0
            && SqlTemporalFunctionEvaluator.IsKnownTemporalFunctionName(dialect, fn.Name))
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for dialect '{dialect.Name}'.");

        return null;
    }
}
