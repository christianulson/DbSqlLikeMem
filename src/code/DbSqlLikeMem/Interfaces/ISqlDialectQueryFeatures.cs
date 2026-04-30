namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes query, pagination, DML, and window-function capabilities for a SQL dialect.
/// PT: Expõe capacidades de query, paginacao, DML e funcoes de janela para um dialeto SQL.
/// </summary>
internal interface ISqlDialectQueryFeatures
{
    bool SupportsLimitOffset { get; }
    bool SupportsFetchFirst { get; }
    bool SupportsTop { get; }
    bool SupportsOffsetFetch { get; }
    bool RequiresOrderByForOffsetFetch { get; }
    bool SupportsOrderByNullsModifier { get; }
    bool SupportsOnDuplicateKeyUpdate { get; }
    bool SupportsOnConflictClause { get; }
    bool SupportsInsertReturning { get; }
    bool SupportsUpdateReturning { get; }
    bool SupportsDeleteReturning { get; }
    bool SupportsDeleteReturningWithJoin { get; }
    bool SupportsAggregateFunctionsInReturningClause { get; }
    bool SupportsDeleteWithoutFrom { get; }
    bool SupportsDeleteTargetAlias { get; }
    bool SupportsUpdateJoinFromSubquerySyntax { get; }
    bool SupportsUpdateFromJoinSubquerySyntax { get; }
    bool SupportsDeleteTargetFromJoinSubquerySyntax { get; }
    bool SupportsDeleteUsingSubquerySyntax { get; }
    int GetInsertUpsertAffectedRowCount(int insertedCount, int updatedCount);
    bool SupportsWithCte { get; }
    bool SupportsWithRecursive { get; }
    bool SupportsWithMaterializedHint { get; }
    bool SupportsNullSafeEq { get; }
    bool SupportsJsonArrowOperators { get; }
    bool SupportsJsonValueReturningClause { get; }
    bool SupportsMatchAgainstPredicate { get; }
    bool SupportsForJsonClause { get; }
    bool SupportsPivotClause { get; }
    bool SupportsUnpivotClause { get; }
    bool PivotAvgReturnsDecimalForIntegralInputs { get; }
    bool SupportsApplyClause { get; }
    bool SupportsStringSplitFunction { get; }
    bool SupportsStringSplitOrdinalArgument { get; }
    bool SupportsTryCastFunction { get; }
    bool SupportsTryConvertFunction { get; }
    bool SupportsParseFunction { get; }
    bool SupportsTryParseFunction { get; }
    bool SupportsEomonthFunction { get; }
    bool SupportsGetUtcDateFunction { get; }
    bool SupportsWindowFunctions { get; }
    bool SupportsWindowFrameClause { get; }
    bool SupportsWindowFrameRowsClause { get; }
    bool SupportsWindowFrameRangeClause { get; }
    bool SupportsWindowFrameGroupsClause { get; }
    bool SupportsLikeEscapeClause { get; }
    bool SupportsIlikeOperator { get; }
    char? LikeDefaultEscapeCharacter { get; }
    bool LikeEscapeExpressionMustBeSingleCharacter { get; }
    bool IsRowNumberWindowFunction(string functionName);
    bool SupportsWindowFunction(string functionName);
    bool RequiresOrderByInWindowFunction(string functionName);
    bool TryGetWindowFunctionArgumentArity(string functionName, out int minArgs, out int maxArgs);
    bool SupportsWithinGroupForStringAggregates { get; }
    bool SupportsWithinGroupStringAggregateFunction(string functionName);
    bool SupportsStringAggregateFunction(string functionName);
    bool SupportsAggregateOrderByForStringAggregates { get; }
    bool SupportsAggregateOrderByStringAggregateFunction(string functionName);
    bool SupportsAggregateSeparatorKeywordForStringAggregates { get; }
    bool SupportsAggregateSeparatorKeywordStringAggregateFunction(string functionName);
    bool SupportsApproximateAggregateFunction(string functionName);
    bool SupportsApproximateScalarFunction(string functionName);
    DbType InferWindowFunctionDbType(WindowFunctionExpr windowFunctionExpr, Func<SqlExpr, DbType> inferArgDbType);
}
