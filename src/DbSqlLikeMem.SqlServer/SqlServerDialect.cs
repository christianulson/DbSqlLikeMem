namespace DbSqlLikeMem.SqlServer;

internal sealed class SqlServerDialect : SqlDialectBase
{
    internal const string DialectName = "sqlserver";
    internal SqlServerDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: [],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>("AND", SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>("OR", SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("=", SqlBinaryOp.Eq),
            new KeyValuePair<string, SqlBinaryOp>("<>", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>("!=", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>(">", SqlBinaryOp.Greater),
            new KeyValuePair<string, SqlBinaryOp>(">=", SqlBinaryOp.GreaterOrEqual),
            new KeyValuePair<string, SqlBinaryOp>("<", SqlBinaryOp.Less),
            new KeyValuePair<string, SqlBinaryOp>("<=", SqlBinaryOp.LessOrEqual),
        ],
        operators:
        [
            ">=", "<=", "<>", "!="
        ])
    { }


    internal const int WithCteMinVersion = 2005;
    internal const int MergeMinVersion = 2008;
    internal const int OffsetFetchMinVersion = 2012;
    internal const int ApproxCountDistinctMinVersion = 2019;
    internal const int JsonFunctionsMinVersion = 2016;
    internal const int CompressionFunctionsMinVersion = 2016;
    internal const int SessionContextMinVersion = 2016;
    internal const int StringEscapeMinVersion = 2016;
    internal const int DateDiffBigMinVersion = 2016;
    internal const int SequenceMinVersion = 2012;
    internal const int StringAggMinVersion = 2017;
    internal const int TranslateMinVersion = 2017;
    internal const int StringSplitOrdinalMinVersion = 2022;
    internal const int HighPrecisionTemporalFunctionsMinVersion = 2008;
    internal const int DateTimeOffsetFunctionsMinVersion = 2008;
    internal const int EomonthMinVersion = 2012;
    internal const int FormatMinVersion = 2012;
    internal const int FromPartsMinVersion = 2012;
    internal const int ParseMinVersion = 2012;
    internal const int TryCastMinVersion = 2012;
    internal const int TryConvertMinVersion = 2012;
    internal const int WindowFunctionsMinVersion = 2005;

    /// <summary>
    /// EN: Gets or sets allows bracket identifiers.
    /// PT: Obtém ou define allows bracket identifiers.
    /// </summary>
    public override bool AllowsBracketIdentifiers => true;

    /// <summary>
    /// EN: Gets or sets identifier escape style.
    /// PT: Obtém ou define identifier escape style.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.bracket;

    /// <summary>
    /// EN: Determines whether the character is treated as a string quote delimiter.
    /// PT: Determina se o caractere é tratado como delimitador de string.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch == '\'';
    /// <summary>
    /// EN: Gets or sets string escape style.
    /// PT: Obtém ou define string escape style.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;
    /// <summary>
    /// EN: Gets or sets text comparison.
    /// PT: Obtém ou define text comparison.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Gets whether top is supported.
    /// PT: Obtém se há suporte a top.
    /// </summary>
    public override bool SupportsTop => true;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured SQL Server version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do SQL Server.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;

    public override bool SupportsWithinGroupForStringAggregates => Version >= StringAggMinVersion;

    public override bool SupportsStringAggregateFunction(string functionName)
        => Version >= StringAggMinVersion
            && functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsWithinGroupStringAggregateFunction(string functionName)
        => Version >= StringAggMinVersion
            && functionName.Equals("STRING_AGG", StringComparison.OrdinalIgnoreCase);

    public override bool SupportsFunctionDdl => true;

    // OFFSET ... FETCH entrou no SQL Server 2012.
    /// <summary>
    /// EN: Gets whether offset fetch is supported.
    /// PT: Obtém se há suporte a offset fetch.
    /// </summary>
    public override bool SupportsOffsetFetch => Version >= OffsetFetchMinVersion;
    /// <summary>
    /// EN: Gets or sets requires order by for offset fetch.
    /// PT: Obtém ou define requires order by for offset fetch.
    /// </summary>
    public override bool RequiresOrderByForOffsetFetch => true;

    /// <summary>
    /// EN: Gets whether delete without from is supported.
    /// PT: Obtém se há suporte a delete without from.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => true; // DELETE [FROM] t
    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => true; // DELETE alias FROM t alias JOIN ...
    public override bool SupportsUpdateFromJoinSubquerySyntax => true;
    public override bool SupportsDeleteTargetFromJoinSubquerySyntax => true;
    /// <summary>
    /// EN: Gets whether with cte is supported.
    /// PT: Obtém se há suporte a with cte.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    // SQL Server supports CTE but not the "WITH RECURSIVE" keyword form.
    /// <summary>
    /// EN: Gets whether with recursive is supported.
    /// PT: Obtém se há suporte a with recursive.
    /// </summary>
    public override bool SupportsWithRecursive => false;
    /// <summary>
    /// EN: Gets whether json value function is supported.
    /// PT: Obtém se há suporte a função json_value.
    /// </summary>
    public override bool SupportsJsonQueryFunction => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether json value function is supported.
    /// PT: Obtém se há suporte a função json_value.
    /// </summary>
    public override bool SupportsJsonValueFunction => Version >= JsonFunctionsMinVersion;
    /// <inheritdoc />
    public override bool SupportsForJsonClause => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether open json function is supported.
    /// PT: Obtém se há suporte a função openjson.
    /// </summary>
    public override bool SupportsOpenJsonFunction => Version >= JsonFunctionsMinVersion;
    /// <summary>
    /// EN: Gets whether merge is supported.
    /// PT: Obtém se há suporte a merge.
    /// </summary>
    public override bool SupportsMerge => Version >= MergeMinVersion;
    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsSequenceDdl => Version >= SequenceMinVersion;
    public override bool SupportsNextValueForSequenceExpression => Version >= SequenceMinVersion;
    public override bool SupportsPreviousValueForSequenceExpression => false;
    public override bool SupportsDoubleAtIdentifierSyntax => true;
    public override bool SupportsLastFoundRowsFunction(string functionName)
        => functionName.Equals("ROWCOUNT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROWCOUNT_BIG", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsLastFoundRowsIdentifier(string identifier)
        => identifier.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Gets whether pivot clause is supported.
    /// PT: Obtém se há suporte a pivot clause.
    /// </summary>
    public override bool SupportsPivotClause => true;
    /// <inheritdoc />
    public override bool SupportsUnpivotClause => true;
    public override bool SupportsApplyClause => Version >= WithCteMinVersion;
    public override bool SupportsStringSplitFunction => Version >= JsonFunctionsMinVersion;
    public override bool SupportsStringSplitOrdinalArgument => Version >= StringSplitOrdinalMinVersion;
    public override bool SupportsTryCastFunction => Version >= TryCastMinVersion;
    public override bool SupportsTryConvertFunction => Version >= TryConvertMinVersion;
    public override bool SupportsParseFunction => Version >= ParseMinVersion;
    public override bool SupportsTryParseFunction => Version >= ParseMinVersion;
    public override bool SupportsEomonthFunction => Version >= EomonthMinVersion;
    public override bool SupportsGetUtcDateFunction => true;
    public override bool SupportsSqlServerMetadataFunction(string functionName)
        => functionName.Equals("APP_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("APPLOCK_MODE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("APPLOCK_TEST", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ASSEMBLYPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CERTENCODED", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CERTPRIVATEKEY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURRENT_REQUEST_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURRENT_TRANSACTION_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CONTEXT_INFO", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATABASE_PRINCIPAL_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATABASEPROPERTYEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CONNECTIONPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("COLUMNPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("COL_LENGTH", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("COL_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CURSOR_STATUS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DB_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DB_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILE_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILE_IDEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILE_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILEGROUP_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILEGROUP_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILEGROUPPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FILEPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FULLTEXTCATALOGPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FULLTEXTSERVICEPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("GET_FILESTREAM_TRANSACTION_CONTEXT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("HAS_PERMS_BY_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("INDEX_COL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("INDEXKEY_PROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("INDEXPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("MIN_ACTIVE_ROWVERSION", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECT_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECT_DEFINITION", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECTPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECTPROPERTYEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECT_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("OBJECT_SCHEMA_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ORIGINAL_DB_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ORIGINAL_LOGIN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PWDCOMPARE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PWDENCRYPT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_LINE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_MESSAGE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_NUMBER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_PROCEDURE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_SEVERITY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ERROR_STATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("GETANSINULL", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("HOST_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("HOST_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("IS_MEMBER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("IS_ROLEMEMBER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("IS_SRVROLEMEMBER", StringComparison.OrdinalIgnoreCase)
            || (functionName.Equals("SESSION_CONTEXT", StringComparison.OrdinalIgnoreCase)
                && Version >= SessionContextMinVersion)
            || functionName.Equals("SCHEMA_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SCHEMA_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SCOPE_IDENTITY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SERVERPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SESSION_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_SID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUSER_SNAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STATS_DATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TYPE_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TYPE_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TYPEPROPERTY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("USER_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("USER_NAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("XACT_STATE", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsSqlServerMetadataIdentifier(string identifier)
        => identifier.Equals("@@DATEFIRST", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("@@IDENTITY", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("@@MAX_PRECISION", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("@@TEXTSIZE", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("CURRENT_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase)
            || identifier.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsSqlServerDateFunction(string functionName)
        => functionName.Equals("DATEDIFF", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATENAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATEPART", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DAY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("MONTH", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("YEAR", StringComparison.OrdinalIgnoreCase);
    /// <inheritdoc />
    public override bool SupportsApproximateAggregateFunction(string functionName)
        => Version >= ApproxCountDistinctMinVersion
            && functionName.Equals("APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase);
    /// <inheritdoc />
    public override bool SupportsSqlServerAggregateFunction(string functionName)
        => functionName.Equals("CHECKSUM_AGG", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsSqlServerScalarFunction(string functionName)
        => functionName.Equals("ABS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ACOS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ASCII", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ASIN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ATAN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ATN2", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("BINARY_CHECKSUM", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CEILING", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CHARINDEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CHECKSUM", StringComparison.OrdinalIgnoreCase)
            || (Version >= CompressionFunctionsMinVersion
                && functionName.Equals("COMPRESS", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("COS", StringComparison.OrdinalIgnoreCase)
            || (Version >= CompressionFunctionsMinVersion
                && functionName.Equals("DECOMPRESS", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("COT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DEGREES", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DIFFERENCE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("EXP", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("FLOOR", StringComparison.OrdinalIgnoreCase)
            || (Version >= FormatMinVersion
                && functionName.Equals("FORMAT", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("FORMATMESSAGE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATALENGTH", StringComparison.OrdinalIgnoreCase)
            || (Version >= DateDiffBigMinVersion
                && functionName.Equals("DATEDIFF_BIG", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("GROUPING", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("GROUPING_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ISDATE", StringComparison.OrdinalIgnoreCase)
            || (Version >= JsonFunctionsMinVersion
                && functionName.Equals("ISJSON", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("ISNUMERIC", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CHAR", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CONCAT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("CONCAT_WS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LEFT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOG", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOG10", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LOWER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NCHAR", StringComparison.OrdinalIgnoreCase)
            || (Version >= JsonFunctionsMinVersion
                && functionName.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("NEWID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("NEWSEQUENTIALID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PATINDEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PI", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("POWER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RADIANS", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RAND", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("REPLACE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RIGHT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ROUND", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SIGN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SIN", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SQUARE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STR", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || (Version >= DateTimeOffsetFunctionsMinVersion
                && functionName.Equals("SWITCHOFFSET", StringComparison.OrdinalIgnoreCase))
            || (Version >= StringEscapeMinVersion
                && functionName.Equals("STRING_ESCAPE", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("TAN", StringComparison.OrdinalIgnoreCase)
            || (Version >= DateTimeOffsetFunctionsMinVersion
                && functionName.Equals("TODATETIMEOFFSET", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("TRIM", StringComparison.OrdinalIgnoreCase)
            || (Version >= TranslateMinVersion
                && functionName.Equals("TRANSLATE", StringComparison.OrdinalIgnoreCase))
            || functionName.Equals("UPPER", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("LTRIM", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PARSENAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("QUOTENAME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("REPLICATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("REVERSE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("RTRIM", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SOUNDEX", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SPACE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("SQRT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("STUFF", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("UNICODE", StringComparison.OrdinalIgnoreCase);
    public override bool SupportsSqlServerFromPartsFunction(string functionName)
        => Version >= FromPartsMinVersion
            && (functionName.Equals("DATEFROMPARTS", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATETIME2FROMPARTS", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATETIMEOFFSETFROMPARTS", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TIMEFROMPARTS", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("SMALLDATETIMEFROMPARTS", StringComparison.OrdinalIgnoreCase));
    /// <summary>
    /// EN: Gets whether sql server table hints is supported.
    /// PT: Obtém se há suporte a sql server table hints.
    /// </summary>
    public override bool SupportsSqlServerTableHints => true;
    /// <summary>
    /// EN: Gets whether sql server query hints is supported.
    /// PT: Obtém se há suporte a sql server consulta hints.
    /// </summary>
    public override bool SupportsSqlServerQueryHints => true;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
        public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["ISNULL"];
    public override IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames
    {
        get
        {
            var names = new Dictionary<string, SqlTemporalFunctionKind>(StringComparer.OrdinalIgnoreCase)
            {
                ["CURRENT_TIMESTAMP"] = SqlTemporalFunctionKind.DateTime,
                ["GETDATE"] = SqlTemporalFunctionKind.DateTime,
                ["GETUTCDATE"] = SqlTemporalFunctionKind.DateTime,
                ["SYSTEMDATE"] = SqlTemporalFunctionKind.DateTime,
            };

            if (Version >= HighPrecisionTemporalFunctionsMinVersion)
            {
                names["SYSDATETIME"] = SqlTemporalFunctionKind.DateTime;
                names["SYSDATETIMEOFFSET"] = SqlTemporalFunctionKind.DateTimeOffset;
                names["SYSUTCDATETIME"] = SqlTemporalFunctionKind.DateTime;
            }

            return names;
        }
    }

    public override IReadOnlyCollection<string> TemporalFunctionIdentifierNames
        => ["CURRENT_TIMESTAMP"];

    public override IReadOnlyCollection<string> TemporalFunctionCallNames
    {
        get
        {
            var names = new List<string> { "GETDATE", "GETUTCDATE" };
            if (Version >= HighPrecisionTemporalFunctionsMinVersion)
            {
                names.Add("SYSDATETIME");
                names.Add("SYSDATETIMEOFFSET");
                names.Add("SYSUTCDATETIME");
            }

            return names;
        }
    }

    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;

    /// <summary>
    /// EN: Gets or sets allows hash identifiers.
    /// PT: Obtém ou define allows hash identifiers.
    /// </summary>
    public override bool AllowsHashIdentifiers => true;

    /// <summary>
    /// EN: Gets temporary table scope.
    /// PT: Obtém temporary table scope.
    /// </summary>
    public override TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        if (string.IsNullOrWhiteSpace(tableName)) return TemporaryTableScope.None;
        if (tableName.StartsWith("##", StringComparison.Ordinal))
            return TemporaryTableScope.Global;
        if (tableName.StartsWith("#", StringComparison.Ordinal))
            return TemporaryTableScope.Connection;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// EN: Represents Supports Date Add Function.
    /// PT: Representa suporte Date Add Function.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase);
}
