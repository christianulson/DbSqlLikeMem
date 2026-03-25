namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines escape rules and behavior for a SQL dialect.
/// PT: Define regras de escape e comportamento de um dialeto SQL.
/// </summary>
internal interface ISqlDialect
{
    /// <summary>
    /// EN: Gets or sets Version.
    /// PT: Obtém ou define Version.
    /// </summary>
    int Version { get; }
    string Name { get; }

    /// <summary>
    /// EN: Gets the scalar function registry supported by this dialect.
    /// PT: Obtém o registry de funcoes escalares suportadas por este dialeto.
    /// </summary>
    IDictionaryProcess<DbScalarFunctionDef> ScalarFunctions { get; }

    /// <summary>
    /// EN: Gets the table-valued function registry supported by this dialect.
    /// PT: Obtém o registry de funções de tabela suportadas por este dialeto.
    /// </summary>
    IDictionaryProcess<DbTableFunctionDef> TableFunctions { get; }

    /// <summary>
    /// EN: Gets the stored procedure registry supported by this dialect.
    /// PT: Obtém o registry de procedimentos armazenados suportados por este dialeto.
    /// </summary>
    IDictionaryProcess<ProcedureDef> Procedures { get; }

    /// <summary>
    /// EN: Gets the window function registry supported by this dialect.
    /// PT: Obtém o registry de funções de janela suportadas por este dialeto.
    /// </summary>
    IDictionaryProcess<DbWindowFunctionDef> WindowFunctions { get; }

    // Identifier quoting
    bool AllowsBacktickIdentifiers { get; }
    bool AllowsDoubleQuoteIdentifiers { get; }
    bool AllowsBracketIdentifiers { get; }
    SqlIdentifierEscapeStyle IdentifierEscapeStyle { get; }

    // Quote pairs (dialect-aware scanning helpers)
    IReadOnlyList<SqlQuotePair> IdentifierQuotes { get; }
    IReadOnlyList<SqlQuotePair> StringQuotes { get; }
    bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair);
    bool TryGetStringQuote(char begin, out SqlQuotePair pair);

    // String quoting
    bool IsStringQuote(char ch);
    SqlStringEscapeStyle StringEscapeStyle { get; }
    bool SupportsDollarQuotedStrings { get; }

    // Parameters
    bool IsParameterPrefix(char ch);

    // Keywords
    bool IsKeyword(string text);

    // Operators (must be ordered by length desc to support greedy match)
    IReadOnlyList<string> Operators { get; }

    // Comments
    bool SupportsHashLineComment { get; }

    // Capabilities
    bool SupportsLimitOffset { get; }
    bool SupportsFetchFirst { get; }
    bool SupportsTop { get; }
    bool SupportsOnDuplicateKeyUpdate { get; }
    bool SupportsOnConflictClause { get; }
    bool SupportsReturning { get; }
    bool SupportsInsertReturning { get; }
    bool SupportsUpdateReturning { get; }
    bool SupportsDeleteReturning { get; }
    bool SupportsDeleteReturningWithJoin { get; }
    bool SupportsAggregateFunctionsInReturningClause { get; }
    bool SupportsMerge { get; }
    bool SupportsTriggers { get; }
    bool SupportsSequenceDdl { get; }
    bool SupportsFunctionDdl { get; }
    bool SupportsCreateOrReplaceFunctionDdl { get; }
    bool SupportsAlterTableAddColumn { get; }
    bool SupportsNextValueForSequenceExpression { get; }
    bool SupportsPreviousValueForSequenceExpression { get; }
    bool SupportsSequenceDotValueExpression(string suffix);
    bool SupportsDoubleAtIdentifierSyntax { get; }
    bool SupportsSqlCalcFoundRowsModifier { get; }

    // Pagination
    bool SupportsOffsetFetch { get; }
    bool RequiresOrderByForOffsetFetch { get; }
    bool SupportsOrderByNullsModifier { get; }

    // DML variations
    bool SupportsDeleteWithoutFrom { get; }
    bool SupportsDeleteTargetAlias { get; }
    bool SupportsUpdateJoinFromSubquerySyntax { get; }
    bool SupportsUpdateFromJoinSubquerySyntax { get; }
    bool SupportsDeleteTargetFromJoinSubquerySyntax { get; }
    bool SupportsDeleteUsingSubquerySyntax { get; }
    int GetInsertUpsertAffectedRowCount(int insertedCount, int updatedCount);


    // CTE (WITH ...)
    bool SupportsWithCte { get; }
    bool SupportsWithRecursive { get; }
    bool SupportsWithMaterializedHint { get; }
    // Features
    bool SupportsNullSafeEq { get; }
    bool SupportsJsonArrowOperators { get; }
    bool SupportsJsonValueReturningClause { get; }

    // Parser-only compatibility toggles (keep runtime rules separated)
    bool AllowsParserCrossDialectQuotedIdentifiers { get; }
    bool AllowsParserCrossDialectJsonOperators { get; }
    bool AllowsParserInsertSelectUpsertSuffix { get; }
    bool AllowsParserDeleteWithoutFromCompatibility { get; }
    bool AllowsParserLimitOffsetCompatibility { get; }

    // Table hints
    bool SupportsSqlServerTableHints { get; }
    bool SupportsSqlServerQueryHints { get; }
    bool SupportsMySqlIndexHints { get; }
    bool SupportsSqlServerMetadataFunction(string functionName);
    bool SupportsSqlServerMetadataIdentifier(string identifier);
    bool SupportsSqlServerScalarFunction(string functionName);
    bool SupportsSqlServerDateFunction(string functionName);
    bool SupportsSqlServerAggregateFunction(string functionName);

    // Temporary table naming
    bool AllowsHashIdentifiers { get; }
    TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName);

    // Operator mapping
    bool TryMapBinaryOperator(string token, out SqlBinaryOp op);

    // Comparison semantics
    StringComparison TextComparison { get; }
    bool SupportsImplicitNumericStringComparison { get; }
    bool LikeIsCaseInsensitive { get; }
    bool SupportsIfFunction { get; }
    bool SupportsIifFunction { get; }
    IReadOnlyCollection<string> NullSubstituteFunctionNames { get; }
    IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames { get; }
    IReadOnlyCollection<string> TemporalFunctionIdentifierNames { get; }
    IReadOnlyCollection<string> TemporalFunctionCallNames { get; }
    bool ConcatReturnsNullOnNullInput { get; }
    // Dialect-specific runtime semantics
    bool RegexInvalidPatternEvaluatesToFalse { get; }
    bool RegexIsCaseInsensitive { get; }
    bool AreUnionColumnTypesCompatible(DbType first, DbType second);
    bool IsIntegerCastTypeName(string typeName);
    bool SupportsWindowFunctions { get; }
    bool SupportsWindowFrameClause { get; }
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
    /// <summary>
    /// EN: Indicates whether an approximate aggregate helper is supported by the current dialect/version.
    /// PT: Indica se um helper de agregacao aproximada e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsApproximateAggregateFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an approximate scalar/helper function is supported by the current dialect/version.
    /// PT: Indica se uma funcao escalar/helper aproximada e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsApproximateScalarFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle-specific conversion helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle especifico de conversao e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleSpecificConversionFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle SCN helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de SCN e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleScnFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle analytics/modeling helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de analytics/modelagem e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleAnalyticsFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle clustering/data mining helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de clustering/data mining e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleClusterFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle container identifier helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de identificador de container e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleContainerFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle rowid helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de rowid e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleRowIdFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle user environment helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de ambiente do usuario e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleUserEnvFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle validation helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de validacao e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleValidationFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle JSON transform helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de JSON transform e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleJsonTransformFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle collation helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de collation e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleCollationFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle NLS helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de NLS e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleNlsFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle hash helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de hash e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleHashFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle SYS helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle SYS e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleSysFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle time/date helper is supported by the current dialect/version.
    /// PT: Indica se um helper Oracle de tempo/data e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleTimeFunction(string functionName);
    DbType InferWindowFunctionDbType(WindowFunctionExpr windowFunctionExpr, Func<SqlExpr, DbType> inferArgDbType);
}
