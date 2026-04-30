using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalUserDefinedScalarFunction(
    FunctionCallExpr fn,
    EvalRow row,
    EvalGroup? group,
    IDictionary<string, Source> ctes,
    out object? result);

internal delegate bool AstQueryTryEvalFunctionFamily(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    EvalRow row,
    EvalGroup? group,
    IDictionary<string, Source> ctes,
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
    private readonly AstQueryTryEvalFunctionFamily[] _tryEvalScalarFunctionFamilies =
    [
        tryEvalNonSqlServerScalarFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalNonSqlServerScalarFunctionFamily)),
        tryEvalSqlServerAndCompatibilityFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalSqlServerAndCompatibilityFunctionFamily)),
        tryEvalGeneralScalarFunctionFamily ?? throw new ArgumentNullException(nameof(tryEvalGeneralScalarFunctionFamily)),
        tryEvalCastStringAndDateTail ?? throw new ArgumentNullException(nameof(tryEvalCastStringAndDateTail))
    ];

    internal object? Evaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        DateTime localNow,
        DateTime utcNow,
        Func<int, object?> evalArg)
    {
        if (group is not null && _isAggregateFunction(fn.Name))
            return _evalAggregate(fn, group, ctes);

        if (_tryEvalUserDefinedScalarFunction(fn, row, group, ctes, out var userDefinedResult))
            return userDefinedResult;

        if (fn.Name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase))
        {
            Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit =
                (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes) =>
                    expr switch
                    {
                        RawSqlExpr raw => AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(raw.Sql),
                        IdentifierExpr identifier => AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(identifier.Name),
                        ColumnExpr column => AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(column.Name),
                        LiteralExpr literal => AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(Convert.ToString(literal.Value, CultureInfo.InvariantCulture) ?? string.Empty),
                        _ => AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(Convert.ToString(evalArg(0), CultureInfo.InvariantCulture) ?? string.Empty)
                    };

            if (AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(
                fn,
                row,
                group,
                ctes,
                evalArg,
                getTemporalUnit,
                AstQueryExecutionRuntimeHelper.ResolveTemporalUnit,
                out var extractResult))
            {
                return extractResult;
            }
        }

        if (IsSpecialSyntaxFunctionName(fn.Name))
        {
            if (_tryEvalScalarFunctionFamilies[^1](context, fn, row, group, ctes, evalArg, out var specialSyntaxResult))
                return specialSyntaxResult;
        }

        if (!IsSpecialSyntaxFunctionName(fn.Name)
            && _tryEvalBoundScalarFunction(context, fn, evalArg, out var boundScalarResult))
            return boundScalarResult;

        if (fn.Args.Count == 0
            && context.TryEvaluateZeroArgCall(
                fn.Name,
                localNow,
                utcNow,
                out var temporalValue))
        {
            return temporalValue;
        }

        foreach (var tryEvalScalarFunctionFamily in _tryEvalScalarFunctionFamilies)
        {
            if (tryEvalScalarFunctionFamily(context, fn, row, group, ctes, evalArg, out var scalarResult))
                return scalarResult;
        }

        if (fn.Args.Count == 0
            && context.IsKnownTemporalFunctionName(fn.Name))
            throw new InvalidOperationException($"Temporal function '{fn.Name}' is not supported for context.Dialect '{context.Dialect.Name}'.");

        return null;
    }

    private static bool IsSpecialSyntaxFunctionName(string name)
        => name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CONVERT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("EXTRACT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("PARSE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase);
}

