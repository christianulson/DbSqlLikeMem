namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private AstQueryFunctionEvaluator FunctionEvaluator
        => _functionEvaluator ??= new AstQueryFunctionEvaluator(
            isAggregateFunction: AggregateFunctionCatalog.Contains,
            evalAggregate: (fn, group, ctes) => _context.EvalAggregate(fn, group, ctes, Eval),
            tryEvalUserDefinedScalarFunction: TryEvalUserDefinedScalarFunction,
            tryEvalBoundScalarFunction: TryEvalBoundScalarFunction,
            tryEvalNonSqlServerScalarFunctionFamily: TryEvalNonSqlServerScalarFunctionFamily,
            tryEvalSqlServerAndCompatibilityFunctionFamily: TryEvalSqlServerAndCompatibilityFunctionFamily,
            tryEvalGeneralScalarFunctionFamily: TryEvalGeneralScalarFunctionFamily,
            tryEvalCastStringAndDateTail: TryEvalCastStringAndDateTail);

    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => _context.EvalCase(c, row, group, ctes, Eval);

    private object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => _context.EvalFunction(
            fn,
            row,
            group,
            ctes,
            _context.EvaluationLocalNow,
            _context.EvaluationUtcNow,
            i => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null,
            FunctionEvaluator);

    internal static bool TryConvertNumericToDouble(object? value, out double result)
        => AstQueryBinaryArithmeticHelper.TryConvertNumericToDouble(value, out result);

    internal static bool TryConvertNumericToDecimal(object? value, out decimal result)
        => AstQueryBinaryArithmeticHelper.TryConvertNumericToDecimal(value, out result);

    private static bool TryEvalBoundScalarFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => context.TryEvalBoundScalarFunction(fn, evalArg, out result);

    private bool TryEvalNonSqlServerScalarFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
    {
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit =
            (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
                => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

        if (_context.TryEvaluate(
            fn,
            row,
            group,
            ctes,
            evalArg,
            Eval,
            getTemporalUnit,
            TryConvertNumericToInt64,
            TryConvertNumericToDouble,
            TryCoerceDateTime,
            TryParseExactCachedDateTime,
            out result))
        {
            return true;
        }

        if (AstQueryTemporalAccessorFunctionEvaluator.TryEvaluate(fn, row, group, ctes, evalArg, getTemporalUnit, AstQueryExecutionRuntimeHelper.ResolveTemporalUnit, out result))
        {
            return true;
        }

        if (AstQueryFirebirdContextFunctionEvaluator.TryEvaluate(_context, fn, evalArg, out result))
        {
            return true;
        }

        if (AstQueryOracleDb2ScalarFunctionEvaluator.TryEvaluate(_context, fn, evalArg, TryCoerceDateTime, out result)
            || AstQueryPostgresScalarFunctionEvaluator.TryEvaluateyPostgresScalarFunction(_context, fn, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalSqlServerAndCompatibilityFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => SqlServerCompatibilityFunctionEvaluator.TryEvaluate(context, fn, row, group, ctes, evalArg, out result);

    private bool TryEvalGeneralScalarFunctionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => context.TryEvaluate(fn, row, group, ctes, AstQueryJsonObjectFunctionEvaluator.TryEvalJsonObjectFunction, evalArg, Eval, ParseIntervalValue, out result);

    private bool TryEvalCastStringAndDateTail(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => CastStringAndDateTailEvaluator.TryEvaluate(context, fn, row, group, ctes, evalArg, out result);

    private bool TryEvalCastConversionFamily(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<int, object?> evalArg,
        out object? result)
        => CastConversionFamilyEvaluator.TryEvaluate(fn, context, evalArg, out result);

    private bool TryEvalUserDefinedScalarFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
        => _context.TryEvalUserDefinedScalarFunction(
            fn,
            row,
            group,
            ctes,
            _localParameterScopes,
            Eval,
            out result);

    private bool TryResolveLocalFunctionValue(string name, out object? value)
        => _context.TryResolveLocalFunctionValue(name, _localParameterScopes, out value);

    private AstQueryCastConversionFamilyEvaluator CastConversionFamilyEvaluator
        => _castConversionFamilyEvaluator ??= new AstQueryCastConversionFamilyEvaluator(
            tryEvalJsonAccessShimFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonAccessShimFunction,
            tryEvalJsonExtractionFunction: AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction,
            tryEvalSqlServerJsonModifyFunction: AstQuerySqlServerUtilityFunctionEvaluator.TryEvalSqlServerJsonModifyFunction,
            tryEvalOpenJsonFunction: AstQuerySqlServerUtilityFunctionEvaluator.TryEvalOpenJsonFunction,
            tryEvalJsonUnquoteFunction: AstQueryJsonUnquoteFunctionEvaluator.TryEvalJsonUnquoteFunction,
            tryEvalToNumberFunction: AstQueryToNumberFunctionEvaluator.TryEvalToNumberFunction);

    private AstQueryCastStringAndDateTailEvaluator CastStringAndDateTailEvaluator
        => _castStringAndDateTailEvaluator ??= new AstQueryCastStringAndDateTailEvaluator(
            tryEvalCastConversionFamily: TryEvalCastConversionFamily,
            tryEvalCastConcatAndStringTail: AstQueryCastStringAndDateTailEvaluator.TryEvalCastConcatAndStringTail,
            tryEvalCastDateTail: (fn, row, group, ctes, context, evalArg, out result) =>
            {
                Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> getTemporalUnit =
                    (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
                        => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

                return AstQueryCastStringAndDateTailEvaluator.TryEvalCastDateTail(
                    fn,
                    row,
                    group,
                    ctes,
                    context,
                    evalArg,
                    Eval,
                    getTemporalUnit,
                    out result);
            });

    private AstQuerySqlServerDatabaseFunctionEvaluator SqlServerDatabaseFunctionEvaluator
        => _sqlServerDatabaseFunctionEvaluator ??= new AstQuerySqlServerDatabaseFunctionEvaluator(
            resolveDatabaseProperty: _context.TryResolveSqlServerDatabaseProperty,
            resolveDatabasePrincipalId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerDatabasePrincipalId,
            resolveColumnProperty: _context.TryResolveSqlServerColumnProperty,
            resolveColumnLength: _context.TryResolveSqlServerColumnLength,
            resolveColumnName: _context.TryResolveSqlServerColumnName,
            resolveObjectId: _context.TryResolveSqlServerObjectId,
            resolveObjectProperty: _context.TryResolveSqlServerObjectProperty,
            resolveObjectName: _context.TryResolveSqlServerObjectName,
            resolveObjectSchemaName: _context.TryResolveSqlServerObjectSchemaName,
            resolveTypeProperty: AstQuerySqlServerResolutionHelper.TryResolveSqlServerTypeProperty,
            getDatabaseName: () => Cnn.Database);

    private AstQuerySqlServerIdentityFunctionEvaluator SqlServerIdentityFunctionEvaluator
        => _sqlServerIdentityFunctionEvaluator ??= new AstQuerySqlServerIdentityFunctionEvaluator(
            getDialect: () => context.Dialect,
            getLastInsertId: Cnn.GetLastInsertId,
            resolveSystemTypeId: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeId,
            resolveSystemTypeName: AstQuerySqlServerResolutionHelper.TryResolveSqlServerSystemTypeName);

    private AstQuerySqlServerUtilityFunctionEvaluator SqlServerUtilityFunctionEvaluator
        => _sqlServerUtilityFunctionEvaluator ??= new AstQuerySqlServerUtilityFunctionEvaluator(
            getDialect: () => context.Dialect,
            tryConvertNumericToDecimal: TryConvertNumericToDecimal,
            tryCoerceDateTime: TryCoerceDateTime,
            tryParseOffset: SqlTemporalFunctionEvaluator.TryParseOffset,
            tryParseCachedDateTimeOffset: TryParseCachedDateTimeOffset);

    private AstQuerySqlServerSessionFunctionEvaluator SqlServerSessionFunctionEvaluator
        => _sqlServerSessionFunctionEvaluator ??= new AstQuerySqlServerSessionFunctionEvaluator(
            getDialect: () => context.Dialect,
            getContextInfo: Cnn.GetContextInfo,
            hasActiveTransaction: () => Cnn.HasActiveTransaction || _context.HasActiveTransaction,
            tryResolveSqlServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerRoleMembership,
            tryResolveSqlServerServerRoleMembership: AstQuerySqlServerResolutionHelper.TryResolveSqlServerServerRoleMembership);

    private AstQuerySqlServerCompatibilityFunctionEvaluator SqlServerCompatibilityFunctionEvaluator
        => _sqlServerCompatibilityFunctionEvaluator ??= new AstQuerySqlServerCompatibilityFunctionEvaluator(
            SqlServerSessionFunctionEvaluator,
            SqlServerDatabaseFunctionEvaluator,
            SqlServerIdentityFunctionEvaluator,
            SqlServerUtilityFunctionEvaluator,
            Eval,
            CreateTemporalUnitResolver(),
            AstQueryExecutionRuntimeHelper.ResolveTemporalUnit);

    private Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, TemporalUnit> CreateTemporalUnitResolver()
        => (SqlExpr expr, EvalRow evalRow, EvalGroup? evalGroup, IDictionary<string, Source> evalCtes)
            => AstQueryExecutionRuntimeHelper.GetTemporalUnit(expr, evalRow, evalGroup, evalCtes, Eval);

}
