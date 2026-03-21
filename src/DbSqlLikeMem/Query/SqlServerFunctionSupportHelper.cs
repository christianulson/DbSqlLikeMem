namespace DbSqlLikeMem;

internal static class SqlServerFunctionSupportHelper
{
    private static readonly HashSet<string> _metadataFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "APP_NAME", "APPLOCK_MODE", "APPLOCK_TEST", "ASSEMBLYPROPERTY", "CERTENCODED", "CERTPRIVATEKEY",
        "CURSOR_STATUS", "DB_ID", "CURRENT_REQUEST_ID", "CURRENT_TRANSACTION_ID", "CONTEXT_INFO",
        "DATABASE_PRINCIPAL_ID", "DATABASEPROPERTYEX", "CONNECTIONPROPERTY", "COLUMNPROPERTY", "DB_NAME",
        "COL_LENGTH", "COL_NAME", "OBJECT_ID", "FILE_ID", "FILE_IDEX", "FILE_NAME", "FILEGROUP_ID",
        "FILEGROUP_NAME", "FILEGROUPPROPERTY", "FILEPROPERTY", "FULLTEXTCATALOGPROPERTY",
        "FULLTEXTSERVICEPROPERTY", "GET_FILESTREAM_TRANSACTION_CONTEXT", "HAS_PERMS_BY_NAME", "INDEX_COL",
        "INDEXKEY_PROPERTY", "INDEXPROPERTY", "MIN_ACTIVE_ROWVERSION", "OBJECT_DEFINITION", "OBJECTPROPERTY",
        "OBJECTPROPERTYEX", "OBJECT_NAME", "OBJECT_SCHEMA_NAME", "IS_MEMBER", "IS_ROLEMEMBER",
        "IS_SRVROLEMEMBER", "ORIGINAL_DB_NAME", "ORIGINAL_LOGIN", "PWDCOMPARE", "PWDENCRYPT", "SCHEMA_ID",
        "SCHEMA_NAME", "SESSION_CONTEXT", "SERVERPROPERTY", "SESSION_ID", "SUSER_ID", "SUSER_NAME",
        "SUSER_SID", "SUSER_SNAME", "STATS_DATE", "TYPE_ID", "TYPE_NAME", "TYPEPROPERTY", "USER_ID",
        "USER_NAME", "XACT_STATE"
    };

    private static readonly HashSet<string> _dateFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "DATEDIFF", "DATENAME", "DATEPART", "DAY", "MONTH", SqlConst.YEAR
    };

    private static readonly HashSet<string> _scalarFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "ABS", "ACOS", "ASCII", "ASIN", "ATAN", "ATN2", "BINARY_CHECKSUM", "CEILING", "CHARINDEX",
        "CHECKSUM", "COS", "COMPRESS", "DECOMPRESS", "COT", "DEGREES", "DIFFERENCE", "EXP", "FLOOR",
        "FORMAT", "FORMATMESSAGE", "DATALENGTH", "DATEDIFF_BIG", "GROUPING", "GROUPING_ID", "ISDATE",
        "ISJSON", "ISNUMERIC", "CHAR", SqlConst.CONCAT, SqlConst.CONCAT_WS, "LEN", SqlConst.LEFT, "LOG", "LOG10", "LOWER",
        "PI", "POWER", "RADIANS", "RAND", "NCHAR", "JSON_MODIFY", "NEWID", "NEWSEQUENTIALID", SqlConst.REPLACE,
        SqlConst.RIGHT, "ROUND", "SIGN", "SIN", "SQUARE", "STR", "SUBSTRING", "TAN", "STRING_ESCAPE",
        "TRANSLATE", "TRIM", "UPPER", "LTRIM", "PARSENAME", "PATINDEX", "QUOTENAME", "REPLICATE",
        "REVERSE", "RTRIM", "SOUNDEX", "SPACE", "SQRT", "STUFF", "UNICODE"
    };

    public static void EnsureSupport(FunctionCallExpr fn, ISqlDialect dialect)
    {
        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return;

        if (_metadataFunctions.Contains(fn.Name)
            && !dialect.SupportsSqlServerMetadataFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (_dateFunctions.Contains(fn.Name)
            && !dialect.SupportsSqlServerDateFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        if (_scalarFunctions.Contains(fn.Name)
            && !dialect.SupportsSqlServerScalarFunction(fn.Name))
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }
    }
}
