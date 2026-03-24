using DbSqlLikeMem.Dialect;

namespace DbSqlLikeMem;

/// <summary>
/// EN: String escaping styles supported by the parser.
/// PT: Estilos de escape de string suportados pelo parser.
/// </summary>
internal enum SqlStringEscapeStyle { backslash, doubled_quote }

/// <summary>
/// EN: Identifier escaping styles supported by the parser.
/// PT: Estilos de escape de identificador suportados pelo parser.
/// </summary>
internal enum SqlIdentifierEscapeStyle { double_quote, backtick, bracket }

internal readonly record struct SqlQuotePair(char Begin, char End);

internal enum SqlTemporalFunctionKind
{
    Date,
    Time,
    DateTime,
    DateTimeOffset
}

internal abstract class SqlDialectBase : ISqlDialect
{
    private readonly HashSet<string> _keywords;
    private readonly Dictionary<string, SqlBinaryOp> _binOps;

    /// <summary>
    /// EN: Implements SqlDialectBase.
    /// PT: Implementa SqlDialectBase.
    /// </summary>
    protected SqlDialectBase(
        string name,
        int version,
        IEnumerable<string> keywords,
        IEnumerable<KeyValuePair<string, SqlBinaryOp>> binOps,
        IEnumerable<string> operators)
    {
        Name = name;
        Version = version;
        _keywords = new HashSet<string>(keywords, StringComparer.OrdinalIgnoreCase);
        _binOps = new Dictionary<string, SqlBinaryOp>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in binOps)
            _binOps[kv.Key] = kv.Value;

        Operators = [.. operators
            .Concat(["*", "/", "+", "-"])
            .Concat(binOps.Select(kv => kv.Key))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(s => s.Length)
            .ThenBy(s => s, StringComparer.Ordinal)];
    }

    /// <summary>
    /// EN: Gets or sets Name.
    /// PT: Obtém ou define Name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// EN: Gets or sets Version.
    /// PT: Obtém ou define Version.
    /// </summary>
    public int Version { get; }

    /// <summary>
    /// EN: Gets the scalar function registry supported by this dialect.
    /// PT: Obtém o registry de funcoes escalares suportadas por este dialeto.
    /// </summary>
    public IDictionaryProcess<DbScalarFunctionDef> ScalarFunctions { get; } = new DictionaryProcess<DbScalarFunctionDef>();

    /// <summary>
    /// EN: Gets the table-valued function registry supported by this dialect.
    /// PT: Obtém o registry de funções de tabela suportadas por este dialeto.
    /// </summary>
    public IDictionaryProcess<DbTableFunctionDef> TableFunctions { get; } = new DictionaryProcess<DbTableFunctionDef>();

    /// <summary>
    /// EN: Gets the stored procedure registry supported by this dialect.
    /// PT: Obtém o registry de procedimentos armazenados suportados por este dialeto.
    /// </summary>
    public IDictionaryProcess<ProcedureDef> Procedures { get; } = new DictionaryProcess<ProcedureDef>();

    /// <summary>
    /// EN: Gets the window function registry supported by this dialect.
    /// PT: Obtém o registry de funções de janela suportadas por este dialeto.
    /// </summary>
    public IDictionaryProcess<DbWindowFunctionDef> WindowFunctions { get; } = new DictionaryProcess<DbWindowFunctionDef>();

    /// <summary>
    /// EN: Gets or sets AllowsBacktickIdentifiers.
    /// PT: Obtém ou define AllowsBacktickIdentifiers.
    /// </summary>
    public virtual bool AllowsBacktickIdentifiers => false;

    /// <summary>
    /// EN: Gets or sets AllowsDoubleQuoteIdentifiers.
    /// PT: Obtém ou define AllowsDoubleQuoteIdentifiers.
    /// </summary>
    public virtual bool AllowsDoubleQuoteIdentifiers => true;

    /// <summary>
    /// EN: Gets or sets AllowsBracketIdentifiers.
    /// PT: Obtém ou define AllowsBracketIdentifiers.
    /// </summary>
    public virtual bool AllowsBracketIdentifiers => false;

    /// <summary>
    /// EN: Gets or sets IdentifierEscapeStyle.
    /// PT: Obtém ou define IdentifierEscapeStyle.
    /// </summary>
    public virtual SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Gets the identifier quote pairs supported by the dialect.
    /// PT: Obtem os pares de aspas de identificador suportados pelo dialeto.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> IdentifierQuotes
    {
        get
        {
            // Double quote can be either string or identifier depending on dialect.
            bool dqIsString = IsStringQuote('"');
            var list = new List<SqlQuotePair>(3);
            if (AllowsBacktickIdentifiers) list.Add(new SqlQuotePair('`', '`'));
            if (AllowsBracketIdentifiers) list.Add(new SqlQuotePair('[', ']'));
            if (AllowsDoubleQuoteIdentifiers && !dqIsString) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// EN: Gets the string quote pairs supported by the dialect.
    /// PT: Obtem os pares de aspas de string suportados pelo dialeto.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> StringQuotes
    {
        get
        {
            var list = new List<SqlQuotePair>(2);
            if (IsStringQuote('\'')) list.Add(new SqlQuotePair('\'', '\''));
            if (IsStringQuote('"')) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// EN: Implements TryGetIdentifierQuote.
    /// PT: Implementa TryGetIdentifierQuote.
    /// </summary>
    public virtual bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in IdentifierQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    /// <summary>
    /// EN: Implements TryGetStringQuote.
    /// PT: Implementa TryGetStringQuote.
    /// </summary>
    public virtual bool TryGetStringQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in StringQuotes)
        {
            if (p.Begin == begin) { pair = p; return true; }
        }
        pair = default;
        return false;
    }

    /// <summary>
    /// EN: Implements IsStringQuote.
    /// PT: Implementa IsStringQuote.
    /// </summary>
    public virtual bool IsStringQuote(char ch) => ch == '\'';

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

    /// <summary>
    /// EN: Controls LIKE case sensitivity in the mock when no explicit collation is available.
    /// PT: Controla sensibilidade de maiúsculas/minúsculas no LIKE do simulado quando não há collation explícita.
    /// </summary>
    public virtual bool LikeIsCaseInsensitive => true;
    public virtual bool SupportsIfFunction => true;
    public virtual bool SupportsIifFunction => true;
    public virtual bool SupportsWindowFunctions => true;
    public virtual bool SupportsWindowFrameClause => false;
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
            foreach (var item in ScalarFunctions)
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
            foreach (var item in ScalarFunctions)
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
            foreach (var item in ScalarFunctions)
            {
                if (item.Value.TemporalKind is not null && item.Value.AllowsCall)
                    registryNames.Add(item.Key);
            }

            return registryNames;
        }
    }
    public virtual bool ConcatReturnsNullOnNullInput => true;

    public virtual bool RegexInvalidPatternEvaluatesToFalse => false;

    public virtual bool RegexIsCaseInsensitive => false;

    public virtual bool AreUnionColumnTypesCompatible(DbType first, DbType second)
    {
        if (first == second)
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

        return SupportsRegisteredScalarCall(functionName);
    }

    public virtual bool SupportsAggregateOrderByForStringAggregates => false;

    public virtual bool SupportsAggregateOrderByStringAggregateFunction(string functionName)
    {
        if (!SupportsAggregateOrderByForStringAggregates || string.IsNullOrWhiteSpace(functionName))
            return false;

        return SupportsRegisteredScalarCall(functionName);
    }

    public virtual bool SupportsAggregateSeparatorKeywordForStringAggregates => false;

    public virtual bool SupportsAggregateSeparatorKeywordStringAggregateFunction(string functionName)
    {
        if (!SupportsAggregateSeparatorKeywordForStringAggregates || string.IsNullOrWhiteSpace(functionName))
            return false;

        return SupportsRegisteredScalarCall(functionName);
    }

    public virtual bool SupportsMatchAgainstPredicate => false;
    public virtual bool SupportsApplyClause => false;
    public virtual bool SupportsStringSplitFunction
        => TableFunctions.ContainsKey(SqlConst.STRING_SPLIT);

    public virtual bool SupportsStringSplitOrdinalArgument
        => TableFunctions.TryGetValue(SqlConst.STRING_SPLIT, out var definition)
            && definition.MaxArguments >= 3;
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

    public virtual bool SupportsOracleTimeFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);
    public virtual bool IsRowNumberWindowFunction(string functionName)
        => functionName.Equals("ROW_NUMBER", StringComparison.OrdinalIgnoreCase);

    private bool SupportsRegisteredScalarCall(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && this.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.AllowsCall;

    private bool SupportsRegisteredScalarIdentifier(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && this.TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.AllowsIdentifier;

    /// <summary>
    /// EN: Indicates whether a specific window function name is supported by the current dialect/version.
    /// PT: Indica se um nome específico de função de janela é suportado pelo dialeto/versão atual.
    /// </summary>
    public virtual bool SupportsWindowFunction(string functionName)
    {
        if (!SupportsWindowFunctions || string.IsNullOrWhiteSpace(functionName))
            return false;

        return WindowFunctions.ContainsKey(functionName);
    }

    /// <summary>
    /// EN: Indicates whether a specific window function requires ORDER BY inside OVER clause.
    /// PT: Indica se uma função de janela específica exige ORDER BY dentro da cláusula OVER.
    /// </summary>
    public virtual bool RequiresOrderByInWindowFunction(string functionName)
    {
        if (!SupportsWindowFunction(functionName))
            return false;

        return WindowFunctions.TryGetValue(functionName, out var definition)
            && definition.RequiresOrderBy;
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

        if (!WindowFunctions.TryGetValue(functionName, out var definition))
            return false;

        minArgs = definition.MinArguments;
        maxArgs = definition.MaxArguments;
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

    /// <summary>
    /// EN: Gets or sets StringEscapeStyle.
    /// PT: Obtém ou define StringEscapeStyle.
    /// </summary>
    public virtual SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// EN: Gets or sets SupportsDollarQuotedStrings.
    /// PT: Obtém ou define SupportsDollarQuotedStrings.
    /// </summary>
    public virtual bool SupportsDollarQuotedStrings => false;

    /// <summary>
    /// EN: Implements IsParameterPrefix.
    /// PT: Implementa IsParameterPrefix.
    /// </summary>
    public virtual bool IsParameterPrefix(char ch) => ch is '@' or ':' or '?';

    /// <summary>
    /// EN: Implements IsKeyword.
    /// PT: Implementa IsKeyword.
    /// </summary>
    public virtual bool IsKeyword(string text)
        => SqlKeywords.IsKeyword(text) || _keywords.Contains(text);

    /// <summary>
    /// EN: Gets or sets Operators.
    /// PT: Obtém ou define Operators.
    /// </summary>
    public IReadOnlyList<string> Operators { get; }

    /// <summary>
    /// EN: Gets or sets SupportsHashLineComment.
    /// PT: Obtém ou define SupportsHashLineComment.
    /// </summary>
    public virtual bool SupportsHashLineComment => false;

    /// <summary>
    /// EN: Gets or sets SupportsLimitOffset.
    /// PT: Obtém ou define SupportsLimitOffset.
    /// </summary>
    public virtual bool SupportsLimitOffset => false;

    /// <summary>
    /// EN: Gets or sets SupportsFetchFirst.
    /// PT: Obtém ou define SupportsFetchFirst.
    /// </summary>
    public virtual bool SupportsFetchFirst => false;
    /// <summary>
    /// EN: Gets or sets SupportsTop.
    /// PT: Obtém ou define SupportsTop.
    /// </summary>
    public virtual bool SupportsTop => false;

    /// <summary>
    /// EN: Gets or sets SupportsOnDuplicateKeyUpdate.
    /// PT: Obtém ou define SupportsOnDuplicateKeyUpdate.
    /// </summary>
    public virtual bool SupportsOnDuplicateKeyUpdate => false;

    /// <summary>
    /// EN: Gets or sets SupportsOnConflictClause.
    /// PT: Obtém ou define SupportsOnConflictClause.
    /// </summary>
    public virtual bool SupportsOnConflictClause => false;

    /// <summary>
    /// EN: Gets or sets SupportsReturning.
    /// PT: Obtém ou define SupportsReturning.
    /// </summary>
    public virtual bool SupportsReturning => false;

    /// <summary>
    /// EN: Gets whether INSERT statements support RETURNING in this dialect.
    /// PT: Obtém se instruções INSERT suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsInsertReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether UPDATE statements support RETURNING in this dialect.
    /// PT: Obtém se instruções UPDATE suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsUpdateReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether DELETE statements support RETURNING in this dialect.
    /// PT: Obtém se instruções DELETE suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsDeleteReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether RETURNING is allowed for joined or multi-table DELETE statements.
    /// PT: Obtém se RETURNING é permitido em instruções DELETE com join ou multi-tabela.
    /// </summary>
    public virtual bool SupportsDeleteReturningWithJoin => true;

    /// <summary>
    /// EN: Gets whether RETURNING clauses accept aggregate functions in this dialect.
    /// PT: Obtém se cláusulas RETURNING aceitam funções de agregação neste dialeto.
    /// </summary>
    public virtual bool SupportsAggregateFunctionsInReturningClause => true;

    /// <summary>
    /// EN: Gets or sets SupportsMerge.
    /// PT: Obtém ou define SupportsMerge.
    /// </summary>
    public virtual bool SupportsMerge => false;

    public virtual bool SupportsTriggers => true;

    public virtual bool SupportsSequenceDdl => false;

    public virtual bool SupportsFunctionDdl => false;

    public virtual bool SupportsCreateOrReplaceFunctionDdl => false;

    public virtual bool SupportsAlterTableAddColumn => false;

    public virtual bool SupportsNextValueForSequenceExpression
        => SupportsRegisteredScalarCall("NEXT_VALUE_FOR");

    public virtual bool SupportsPreviousValueForSequenceExpression
        => SupportsRegisteredScalarCall("PREVIOUS_VALUE_FOR");

    public virtual bool SupportsSequenceDotValueExpression(string suffix)
        => this.TryGetScalarFunctionDefinition(suffix, out var definition)
            && definition is not null
            && definition.AllowsCall;

    /// <summary>
    /// EN: Gets whether a sequence-style scalar call is supported in this dialect.
    /// PT: Obtém se uma chamada escalar no estilo sequence é suportada neste dialeto.
    /// </summary>
    public virtual bool SupportsSequenceFunctionCall(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    public virtual bool SupportsDoubleAtIdentifierSyntax => false;

    public virtual bool SupportsSqlCalcFoundRowsModifier => false;

    /// <summary>
    /// EN: Gets whether a LAST/FOUND ROWS helper function is supported in this dialect.
    /// PT: Obtém se uma função auxiliar LAST/FOUND ROWS é suportada neste dialeto.
    /// </summary>
    public virtual bool SupportsLastFoundRowsFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    /// <summary>
    /// EN: Gets whether a LAST/FOUND ROWS identifier token is supported in this dialect.
    /// PT: Obtém se um token identificador LAST/FOUND ROWS é suportado neste dialeto.
    /// </summary>
    public virtual bool SupportsLastFoundRowsIdentifier(string identifier)
        => SupportsRegisteredScalarIdentifier(identifier);

    /// <summary>
    /// EN: Gets or sets SupportsOffsetFetch.
    /// PT: Obtém ou define SupportsOffsetFetch.
    /// </summary>
    public virtual bool SupportsOffsetFetch => false;

    /// <summary>
    /// EN: Gets or sets RequiresOrderByForOffsetFetch.
    /// PT: Obtém ou define RequiresOrderByForOffsetFetch.
    /// </summary>
    public virtual bool RequiresOrderByForOffsetFetch => false;

    public virtual bool SupportsOrderByNullsModifier => false;

    /// <summary>
    /// EN: Gets or sets SupportsDeleteWithoutFrom.
    /// PT: Obtém ou define SupportsDeleteWithoutFrom.
    /// </summary>
    public virtual bool SupportsDeleteWithoutFrom => false;

    /// <summary>
    /// EN: Gets or sets SupportsDeleteTargetAlias.
    /// PT: Obtém ou define SupportsDeleteTargetAlias.
    /// </summary>
    public virtual bool SupportsDeleteTargetAlias => true;

    /// <summary>
    /// EN: Gets whether MySQL-style UPDATE target JOIN (subquery) syntax is supported.
    /// PT: Obtém se a sintaxe UPDATE alvo JOIN (subquery) no estilo MySQL é suportada.
    /// </summary>
    public virtual bool SupportsUpdateJoinFromSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether SQL Server/PostgreSQL-style UPDATE ... FROM ... JOIN (subquery) syntax is supported.
    /// PT: Obtém se a sintaxe UPDATE ... FROM ... JOIN (subquery) no estilo SQL Server/PostgreSQL é suportada.
    /// </summary>
    public virtual bool SupportsUpdateFromJoinSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether SQL Server/MySQL-style DELETE target FROM ... JOIN (subquery) syntax is supported.
    /// PT: Obtém se a sintaxe DELETE alvo FROM ... JOIN (subquery) no estilo SQL Server/MySQL é suportada.
    /// </summary>
    public virtual bool SupportsDeleteTargetFromJoinSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether PostgreSQL-style DELETE FROM ... USING (subquery) syntax is supported.
    /// PT: Obtém se a sintaxe DELETE FROM ... USING (subquery) no estilo PostgreSQL é suportada.
    /// </summary>
    public virtual bool SupportsDeleteUsingSubquerySyntax => false;

    /// <summary>
    /// EN: Calculates the affected-row count reported by INSERT/UPSERT operations for this dialect.
    /// PT: Calcula a contagem de linhas afetadas reportada por operacoes INSERT/UPSERT para este dialeto.
    /// </summary>
    public virtual int GetInsertUpsertAffectedRowCount(int insertedCount, int updatedCount)
        => insertedCount + updatedCount;

    /// <summary>
    /// EN: Gets or sets SupportsWithCte.
    /// PT: Obtém ou define SupportsWithCte.
    /// </summary>
    public virtual bool SupportsWithCte => false;

    /// <summary>
    /// EN: Gets or sets SupportsWithRecursive.
    /// PT: Obtém ou define SupportsWithRecursive.
    /// </summary>
    public virtual bool SupportsWithRecursive => true;

    /// <summary>
    /// EN: Gets or sets SupportsWithMaterializedHint.
    /// PT: Obtém ou define SupportsWithMaterializedHint.
    /// </summary>
    public virtual bool SupportsWithMaterializedHint => false;

    /// <summary>
    /// EN: Gets or sets SupportsNullSafeEq.
    /// PT: Obtém ou define SupportsNullSafeEq.
    /// </summary>
    public virtual bool SupportsNullSafeEq => false;

    /// <summary>
    /// EN: Gets or sets SupportsJsonArrowOperators.
    /// PT: Obtém ou define SupportsJsonArrowOperators.
    /// </summary>
    public virtual bool SupportsJsonArrowOperators => false;
    /// <summary>
    /// EN: Gets whether JSON_EXTRACT is supported as a scalar helper in this dialect.
    /// PT: Obtém se JSON_EXTRACT é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonExtractFunction
        => SupportsRegisteredScalarCall("JSON_EXTRACT");

    /// <summary>
    /// EN: Gets whether JSON_VALUE is supported as a scalar helper in this dialect.
    /// PT: Obtém se JSON_VALUE é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonValueFunction
        => SupportsRegisteredScalarCall("JSON_VALUE");

    /// <summary>
    /// EN: Gets whether JSON_QUERY is supported as a scalar helper in this dialect.
    /// PT: Obtém se JSON_QUERY é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonQueryFunction
        => SupportsRegisteredScalarCall("JSON_QUERY");

    /// <summary>
    /// EN: Gets whether OPENJSON is supported as a table function in this dialect.
    /// PT: Obtém se OPENJSON é suportada como função de tabela neste dialeto.
    /// </summary>
    public virtual bool SupportsOpenJsonFunction
        => TableFunctions.ContainsKey(SqlConst.OPENJSON);

    /// <summary>
    /// EN: Gets whether JSON_TABLE is supported as a table function in this dialect.
    /// PT: Obtém se JSON_TABLE é suportada como função de tabela neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonTableFunction
        => TableFunctions.ContainsKey(SqlConst.JSON_TABLE);

    public virtual bool SupportsJsonValueReturningClause => false;

    /// <summary>
    /// EN: Gets or sets AllowsParserCrossDialectQuotedIdentifiers.
    /// PT: Obtém ou define AllowsParserCrossDialectQuotedIdentifiers.
    /// </summary>
    public virtual bool AllowsParserCrossDialectQuotedIdentifiers => false;

    /// <summary>
    /// EN: Gets or sets AllowsParserCrossDialectJsonOperators.
    /// PT: Obtém ou define AllowsParserCrossDialectJsonOperators.
    /// </summary>
    public virtual bool AllowsParserCrossDialectJsonOperators => false;

    /// <summary>
    /// EN: Gets or sets AllowsParserInsertSelectUpsertSuffix.
    /// PT: Obtém ou define AllowsParserInsertSelectUpsertSuffix.
    /// </summary>
    public virtual bool AllowsParserInsertSelectUpsertSuffix => false;

    /// <summary>
    /// EN: Gets or sets AllowsParserDeleteWithoutFromCompatibility.
    /// PT: Obtém ou define AllowsParserDeleteWithoutFromCompatibility.
    /// </summary>
    public virtual bool AllowsParserDeleteWithoutFromCompatibility => false;

    /// <summary>
    /// EN: Gets or sets AllowsParserLimitOffsetCompatibility.
    /// PT: Obtém ou define AllowsParserLimitOffsetCompatibility.
    /// </summary>
    public virtual bool AllowsParserLimitOffsetCompatibility => false;

    /// <summary>
    /// EN: Gets or sets SupportsSqlServerTableHints.
    /// PT: Obtém ou define SupportsSqlServerTableHints.
    /// </summary>
    public virtual bool SupportsSqlServerTableHints => false;

    /// <summary>
    /// EN: Gets or sets SupportsSqlServerQueryHints.
    /// PT: Obtém ou define SupportsSqlServerQueryHints.
    /// </summary>
    public virtual bool SupportsSqlServerQueryHints => false;

    /// <summary>
    /// EN: Gets or sets SupportsMySqlIndexHints.
    /// PT: Obtém ou define SupportsMySqlIndexHints.
    /// </summary>
    public virtual bool SupportsMySqlIndexHints => false;

    /// <summary>
    /// EN: Gets or sets AllowsHashIdentifiers.
    /// PT: Obtém ou define AllowsHashIdentifiers.
    /// </summary>
    public virtual bool AllowsHashIdentifiers => false;

    /// <summary>
    /// EN: Implements GetTemporaryTableScope.
    /// PT: Implementa GetTemporaryTableScope.
    /// </summary>
    public virtual TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        _ = tableName;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// EN: Implements TryMapBinaryOperator.
    /// PT: Implementa TryMapBinaryOperator.
    /// </summary>
    public bool TryMapBinaryOperator(string token, out SqlBinaryOp op)
        => _binOps.TryGetValue(token, out op);
}
