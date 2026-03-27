using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal sealed class AstQuerySqlServerCompatibilityFunctionEvaluator(
    AstQuerySqlServerSessionFunctionEvaluator sqlServerSessionFunctionEvaluator,
    AstQuerySqlServerDatabaseFunctionEvaluator sqlServerDatabaseFunctionEvaluator,
    AstQuerySqlServerIdentityFunctionEvaluator sqlServerIdentityFunctionEvaluator,
    AstQuerySqlServerUtilityFunctionEvaluator sqlServerUtilityFunctionEvaluator,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
    Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit,
    Func<string, TemporalUnit> resolveTemporalUnit)
{
    private readonly AstQuerySqlServerSessionFunctionEvaluator _sqlServerSessionFunctionEvaluator =
        sqlServerSessionFunctionEvaluator ?? throw new ArgumentNullException(nameof(sqlServerSessionFunctionEvaluator));

    private readonly AstQuerySqlServerDatabaseFunctionEvaluator _sqlServerDatabaseFunctionEvaluator =
        sqlServerDatabaseFunctionEvaluator ?? throw new ArgumentNullException(nameof(sqlServerDatabaseFunctionEvaluator));

    private readonly AstQuerySqlServerIdentityFunctionEvaluator _sqlServerIdentityFunctionEvaluator =
        sqlServerIdentityFunctionEvaluator ?? throw new ArgumentNullException(nameof(sqlServerIdentityFunctionEvaluator));

    private readonly AstQuerySqlServerUtilityFunctionEvaluator _sqlServerUtilityFunctionEvaluator =
        sqlServerUtilityFunctionEvaluator ?? throw new ArgumentNullException(nameof(sqlServerUtilityFunctionEvaluator));

    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> _eval =
        eval ?? throw new ArgumentNullException(nameof(eval));

    private readonly Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> _getTemporalUnit =
        getTemporalUnit ?? throw new ArgumentNullException(nameof(getTemporalUnit));

    private readonly Func<string, TemporalUnit> _resolveTemporalUnit =
        resolveTemporalUnit ?? throw new ArgumentNullException(nameof(resolveTemporalUnit));

    internal bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (context.TryEvalNumericFunction(fn, evalArg, out result)
            || _sqlServerUtilityFunctionEvaluator.TryEvalCurrentUserFunction(context, fn, out result)
            || _sqlServerUtilityFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result))
        {
            return true;
        }

        context.EnsureSupport(fn);

        if (_sqlServerSessionFunctionEvaluator.TryEvaluate(fn, evalArg, out result))
            return true;

        if (_sqlServerDatabaseFunctionEvaluator.TryEvaluate(fn, evalArg, out result)
            || _sqlServerIdentityFunctionEvaluator.TryEvaluate(fn, evalArg, out result)
            || _sqlServerUtilityFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result)
            || context.TryEvaluateSqlServerDateConstructionFunction(fn, evalArg, out result)
            || AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, _getTemporalUnit, _resolveTemporalUnit, out result)
            || context.TryEvaluateDb2DateFunction(fn, evalArg, _resolveTemporalUnit, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalDegreesFunction(context, fn, evalArg, out result)
            || context.TryEvaluate(fn, row, group, ctes, evalArg, _eval, _getTemporalUnit, out result))
        {
            return true;
        }

        if (!context.Dialect.SupportsEomonthFunction
            && !(fn.ResolvedScalarFunction?.AllowsCall
                ?? (context.Dialect.TryGetScalarFunctionDefinition(fn, out var eomonthDefinition)
                    && eomonthDefinition is not null
                    && eomonthDefinition.AllowsCall)))
        {
            throw SqlUnsupported.NotSupported(context.Dialect, "EOMONTH");
        }

        if (AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate(context, fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalDifferenceFunction(context, fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalExpFunction(context, fn, evalArg, out result)
            || AstQueryGeneralScalarFunctionEvaluator.TryEvalFloorFunction(context, fn, evalArg, out result)
            || _sqlServerUtilityFunctionEvaluator.TryEvaluate(fn, context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }
}
