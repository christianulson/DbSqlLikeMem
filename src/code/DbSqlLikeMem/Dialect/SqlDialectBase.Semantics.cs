namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    /// <summary>
    /// EN: String comparison mode used by textual operators (=, &lt;&gt;, ORDER BY fallback, etc.).
    /// PT: Modo de comparação textual usado por operadores textuais (=, &lt;&gt;, ORDER BY fallback, etc.).
    /// </summary>
    public virtual StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Enables controlled implicit cast between numeric and numeric-string values in comparisons.
    /// PT: Habilita cast implícito controlado entre números e strings numéricas em comparações.
    /// </summary>
    public virtual bool SupportsImplicitNumericStringComparison => true;

    public virtual bool LikeIsCaseInsensitive => true;
    public virtual bool SupportsIfFunction => true;
    public virtual bool SupportsIifFunction => true;
    public virtual bool SupportsWindowFunctions => true;
    public virtual bool SupportsWindowFrameClause => false;
    public virtual bool SupportsWindowFrameRowsClause => SupportsWindowFrameClause;
    public virtual bool SupportsWindowFrameRangeClause => SupportsWindowFrameClause;
    public virtual bool SupportsWindowFrameGroupsClause => SupportsWindowFrameClause;
    public virtual bool SupportsForJsonClause => false;
    public virtual bool SupportsPivotClause => false;
    public virtual bool SupportsUnpivotClause => false;
    public virtual bool PivotAvgReturnsDecimalForIntegralInputs => false;
    public virtual IReadOnlyCollection<string> NullSubstituteFunctionNames
        => ["IFNULL", "ISNULL", "NVL"];

    public virtual IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames
    {
        get
        {
            var registryNames = new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in Functions)
            {
                if (item.Value.TemporalKind is SqlTemporalFunctionKind temporalKind)
                    registryNames[item.Key] = temporalKind;
            }

            return registryNames;
        }
    }

    public virtual IReadOnlyCollection<string> TemporalFunctionIdentifierNames
    {
        get
        {
            var registryNames = new List<string>();
            foreach (var item in Functions)
            {
                if (item.Value.TemporalKind is not null && item.Value.AllowsIdentifier)
                    registryNames.Add(item.Key);
            }

            return registryNames;
        }
    }

    public virtual IReadOnlyCollection<string> TemporalFunctionCallNames
    {
        get
        {
            var registryNames = new List<string>();
            foreach (var item in Functions)
            {
                if (item.Value.TemporalKind is not null && item.Value.AllowsCall)
                    registryNames.Add(item.Key);
            }

            return registryNames;
        }
    }

    /// <summary>
    /// EN: Indicates whether string concatenation with the plus operator returns null when any operand is null.
    /// PT: Indica se a concatenacao de strings com o operador mais retorna null quando qualquer operando e null.
    /// </summary>
    public virtual bool PlusStringConcatReturnsNullOnNullInput => true;

    /// <summary>
    /// EN: Indicates whether CONCAT() returns null when any argument is null.
    /// PT: Indica se CONCAT() retorna null quando qualquer argumento e null.
    /// </summary>
    public virtual bool ConcatFunctionReturnsNullOnNullInput => true;

    /// <summary>
    /// EN: Indicates whether the pipe operator (||) is treated as string concatenation.
    /// PT: Indica se o operador pipe (||) e tratado como concatenacao de strings.
    /// </summary>
    public virtual bool SupportsPipeConcatOperator => false;

    /// <summary>
    /// EN: Legacy combined concat null behavior kept for compatibility with older call sites.
    /// PT: Comportamento legado combinado de concat null mantido por compatibilidade com call sites antigos.
    /// </summary>
    public virtual bool ConcatReturnsNullOnNullInput => PlusStringConcatReturnsNullOnNullInput && ConcatFunctionReturnsNullOnNullInput;

    public virtual bool RegexInvalidPatternEvaluatesToFalse => false;

    public virtual bool RegexIsCaseInsensitive => false;

    public virtual bool AreUnionColumnTypesCompatible(DbType first, DbType second)
    {
        if (first == second)
            return true;

        if (first == DbType.Object || second == DbType.Object)
            return true;

        static bool IsNumeric(DbType t)
            => t is DbType.Byte or DbType.SByte
            or DbType.Int16 or DbType.UInt16
            or DbType.Int32 or DbType.UInt32
            or DbType.Int64 or DbType.UInt64
            or DbType.Decimal or DbType.Double
            or DbType.Single or DbType.VarNumeric;

        static bool IsText(DbType t)
            => t is DbType.AnsiString or DbType.String
            or DbType.AnsiStringFixedLength or DbType.StringFixedLength;

        if (IsNumeric(first) && IsNumeric(second))
            return true;

        if (IsText(first) && IsText(second))
            return true;

        return false;
    }

    public virtual bool IsIntegerCastTypeName(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            return false;

        return typeName.Equals("SIGNED", StringComparison.OrdinalIgnoreCase)
            || typeName.Equals("UNSIGNED", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("INT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("INTEGER", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("BIGINT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("SMALLINT", StringComparison.OrdinalIgnoreCase)
            || typeName.StartsWith("TINYINT", StringComparison.OrdinalIgnoreCase)
            || typeName.Equals("BIT", StringComparison.OrdinalIgnoreCase);
    }

    public virtual bool SupportsLikeEscapeClause => true;

    public virtual bool SupportsIlikeOperator => false;

    public virtual char? LikeDefaultEscapeCharacter => null;

    public virtual bool LikeEscapeExpressionMustBeSingleCharacter => true;

    public virtual bool SupportsWithinGroupForStringAggregates => false;

    public virtual bool SupportsWithinGroupStringAggregateFunction(string functionName)
    {
        if (!SupportsWithinGroupForStringAggregates || string.IsNullOrWhiteSpace(functionName))
            return false;

        return SupportsRegisteredScalarCall(functionName);
    }

    public virtual bool SupportsStringAggregateFunction(string functionName)
    {
        if (string.IsNullOrWhiteSpace(functionName))
            return false;

        return TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.IsStringAggregate;
    }

    public virtual bool SupportsAggregateOrderByForStringAggregates => false;

    public virtual bool SupportsAggregateOrderByStringAggregateFunction(string functionName)
    {
        if (!SupportsAggregateOrderByForStringAggregates || string.IsNullOrWhiteSpace(functionName))
            return false;

        return TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.IsStringAggregate;
    }

    public virtual bool SupportsAggregateSeparatorKeywordForStringAggregates => false;

    public virtual bool SupportsAggregateSeparatorKeywordStringAggregateFunction(string functionName)
    {
        if (!SupportsAggregateSeparatorKeywordForStringAggregates || string.IsNullOrWhiteSpace(functionName))
            return false;

        return TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.IsStringAggregate;
    }

    public virtual bool SupportsMatchAgainstPredicate => false;
    public virtual bool SupportsApplyClause => false;
    public virtual bool SupportsStringSplitFunction
        => TryGetTableFunctionDefinition(SqlConst.STRING_SPLIT, out _);

    public virtual bool SupportsStringSplitOrdinalArgument
        => TryGetTableFunctionDefinition(SqlConst.STRING_SPLIT, out var definition)
            && definition!.Signatures.Any(s => s.MaxArguments >= 3);
    public virtual bool SupportsTryCastFunction
        => SupportsRegisteredScalarCall("TRY_CAST");

    public virtual bool SupportsTryConvertFunction
        => SupportsRegisteredScalarCall("TRY_CONVERT");
    public virtual bool SupportsParseFunction
        => SupportsRegisteredScalarCall("PARSE");

    public virtual bool SupportsTryParseFunction
        => SupportsRegisteredScalarCall("TRY_PARSE");
    public virtual bool SupportsEomonthFunction
        => SupportsRegisteredScalarCall("EOMONTH");

    public virtual bool SupportsGetUtcDateFunction
        => SupportsRegisteredScalarCall("GETUTCDATE");
    public virtual bool SupportsApproximateAggregateFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsApproximateScalarFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleSpecificConversionFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleScnFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleAnalyticsFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleClusterFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleContainerFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleRowIdFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleUserEnvFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleValidationFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleJsonTransformFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleCollationFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleNlsFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleHashFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleSysFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsOracleReservedIdentifier(string identifier)
        => false;

    public virtual bool SupportsOracleTimeFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);
    public virtual bool IsRowNumberWindowFunction(string functionName)
        => functionName.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase);

    private bool SupportsRegisteredScalarCall(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.AllowsCall;

    /// <summary>
    /// EN: Indicates whether a specific window function name is supported by the current dialect/version.
    /// PT: Indica se um nome específico de função de janela é suportado pelo dialeto/versão atual.
    /// </summary>
    public virtual bool SupportsWindowFunction(string functionName)
    {
        if (!SupportsWindowFunctions || string.IsNullOrWhiteSpace(functionName))
            return false;

        return Functions.IsWindowFunction(functionName);
    }

    /// <summary>
    /// EN: Indicates whether a specific window function requires ORDER BY inside OVER clause.
    /// PT: Indica se uma função de janela específica exige ORDER BY dentro da cláusula OVER.
    /// </summary>
    public virtual bool RequiresOrderByInWindowFunction(string functionName)
    {
        if (!SupportsWindowFunction(functionName))
            return false;

        return TryGetWindowFunctionDefinition(functionName, out var definition)
            && definition!.Signatures.Any(s => s.RequiresOrderBy);
    }


    /// <summary>
    /// EN: Gets accepted argument arity range for a supported window function.
    /// PT: Obtém o intervalo de aridade aceito para uma função de janela suportada.
    /// </summary>
    public virtual bool TryGetWindowFunctionArgumentArity(string functionName, out int minArgs, out int maxArgs)
    {
        minArgs = 0;
        maxArgs = 0;

        if (!SupportsWindowFunction(functionName))
            return false;

        if (!TryGetWindowFunctionDefinition(functionName, out var definition))
            return false;

        minArgs = definition!.Signatures.Count == 0 ? 0 : definition.Signatures.Min(s => s.MinArguments);
        maxArgs = definition.Signatures.Count == 0 ? 0 : definition.Signatures.Max(s => s.MaxArguments);
        return true;
    }

    public virtual DbType InferWindowFunctionDbType(
        WindowFunctionExpr windowFunctionExpr,
        Func<SqlExpr, DbType> inferArgDbType)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(windowFunctionExpr, nameof(windowFunctionExpr));
        ArgumentNullExceptionCompatible.ThrowIfNull(inferArgDbType, nameof(inferArgDbType));

        if (IsRowNumberWindowFunction(windowFunctionExpr.Name)
            || windowFunctionExpr.Name.Equals("RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("DENSE_RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("NTILE", StringComparison.OrdinalIgnoreCase))
            return DbType.Int64;

        if (windowFunctionExpr.Name.Equals("PERCENT_RANK", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("CUME_DIST", StringComparison.OrdinalIgnoreCase))
            return DbType.Double;

        if (windowFunctionExpr.Name.Equals("LAG", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("LEAD", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("FIRST_VALUE", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("LAST_VALUE", StringComparison.OrdinalIgnoreCase)
            || windowFunctionExpr.Name.Equals("NTH_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            if (windowFunctionExpr.Args.Count > 0)
                return inferArgDbType(windowFunctionExpr.Args[0]);
        }

        return DbType.Object;
    }
}
