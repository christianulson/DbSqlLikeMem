namespace DbSqlLikeMem.SqlServer.TestTools;

/// <summary>
/// EN: Provides SQL Server-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT-br: Fornece trechos SQL especificos de SQL Server usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public class SqlServerProviderSqlDialect : ProviderSqlDialect
{
    private static readonly string[] SqlServerMetadataFunctionNames =
    [
        "APP_NAME",
        "CONNECTIONPROPERTY",
        "DATABASE_PRINCIPAL_ID",
        "DATABASEPROPERTYEX",
        "COLUMNPROPERTY",
        "COL_LENGTH",
        "COL_NAME",
        "DB_ID",
        "DB_NAME",
        "OBJECT_ID",
        "OBJECTPROPERTY",
        "OBJECTPROPERTYEX",
        "OBJECT_NAME",
        "OBJECT_SCHEMA_NAME",
        "ORIGINAL_DB_NAME",
        "ORIGINAL_LOGIN",
        "SERVERPROPERTY",
        "SCHEMA_ID",
        "SCHEMA_NAME",
        "SCOPE_IDENTITY",
        "CURRENT_REQUEST_ID",
        "SESSION_ID",
        "TYPE_ID",
        "TYPE_NAME",
        "TYPEPROPERTY",
        "SUSER_ID",
        "SUSER_NAME",
        "SUSER_SID",
        "SUSER_SNAME",
        "USER_ID",
        "USER_NAME",
        "XACT_STATE",
        "GETANSINULL",
        "HOST_ID",
        "HOST_NAME",
        "IS_MEMBER",
        "IS_ROLEMEMBER",
        "IS_SRVROLEMEMBER",
        "SESSION_CONTEXT",
        "CURRENT_TRANSACTION_ID",
        "CONTEXT_INFO",
        "ERROR_LINE",
        "ERROR_MESSAGE",
        "ERROR_NUMBER",
        "ERROR_PROCEDURE",
        "ERROR_SEVERITY",
        "ERROR_STATE"
    ];

    private static readonly string[] SqlServerMetadataIdentifierNames =
    [
        "CURRENT_USER",
        "SESSION_USER",
        "SYSTEM_USER",
        "@@DATEFIRST",
        "@@IDENTITY",
        "@@MAX_PRECISION",
        "@@ROWCOUNT",
        "@@TEXTSIZE"
    ];

    private static readonly string[] SqlServerScalarFunctionNames =
    [
        "ABS",
        "ACOS",
        "ASIN",
        "ATAN",
        "ATN2",
        "CEILING",
        "COS",
        "COT",
        "DEGREES",
        "EXP",
        "FLOOR",
        "FORMAT",
        "LOG",
        "LOG10",
        "PI",
        "POWER",
        "RADIANS",
        "RAND",
        "ROUND",
        "SIGN",
        "SIN",
        "SQUARE",
        "TAN",
        "SQRT",
        "ASCII",
        "CHARINDEX",
        "CHECKSUM",
        "BINARY_CHECKSUM",
        "DATALENGTH",
        "DIFFERENCE",
        "GROUPING",
        "GROUPING_ID",
        "ISDATE",
        "ISJSON",
        "ISNUMERIC",
        "LEN",
        "PATINDEX",
        "UNICODE",
        "ROWCOUNT",
        "ROWCOUNT_BIG",
        "CHAR",
        "CONCAT",
        "CONCAT_WS",
        "FORMATMESSAGE",
        "LEFT",
        "LOWER",
        "NCHAR",
        "NEWID",
        "NEWSEQUENTIALID",
        "PARSENAME",
        "QUOTENAME",
        "REPLICATE",
        "REVERSE",
        "REPLACE",
        "RIGHT",
        "SOUNDEX",
        "SPACE",
        "STR",
        "STUFF",
        "SUBSTRING",
        "TRIM",
        "TRANSLATE",
        "UPPER",
        "LTRIM",
        "RTRIM",
        "IF",
        "IIF",
        "JSON_MODIFY",
        "COMPRESS",
        "DECOMPRESS",
        "STRING_ESCAPE",
        "TODATETIMEOFFSET",
        "SWITCHOFFSET",
        "DATETRUNC",
        "DATEADD",
        "DATEDIFF",
        "DATENAME",
        "DATEPART",
        "DAY",
        "MONTH",
        "YEAR",
        "DATEDIFF_BIG",
        "CAST",
        "PARSE",
        "TRY_PARSE",
        "TRY_CAST",
        "TRY_CONVERT",
        "DATEFROMPARTS",
        "DATETIMEFROMPARTS",
        "DATETIME2FROMPARTS",
        "DATETIMEOFFSETFROMPARTS",
        "TIMEFROMPARTS",
        "SMALLDATETIMEFROMPARTS"
    ];

    private static readonly string[] SqlServerDateFunctionNames =
    [
        "CURRENT_TIMESTAMP",
        "GETDATE",
        "GETUTCDATE",
        "SYSTEMDATE",
        "SYSDATETIME",
        "SYSUTCDATETIME",
        "SYSDATETIMEOFFSET",
        "EOMONTH",
        "DATEADD",
        "DATEDIFF",
        "DATENAME",
        "DATEPART",
        "DAY",
        "MONTH",
        "YEAR",
        "DATEDIFF_BIG",
        "TODATETIMEOFFSET",
        "SWITCHOFFSET",
        "DATETRUNC"
    ];

    private static readonly string[] SqlServerAggregateFunctionNames =
    [
        "CHECKSUM_AGG",
        "STRING_AGG",
        "APPROX_COUNT_DISTINCT",
        "MEDIAN",
        "PERCENTILE",
        "PERCENTILE_CONT",
        "PERCENTILE_DISC"
    ];

    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.SqlServer;

    /// <inheritdoc />
    public override string DisplayName => "SQL Server";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsReleaseSavepoints => false;

    /// <inheritdoc />
    public override bool SupportsMathFunctions => true;

    /// <inheritdoc />
    public override bool SupportsMathLogBaseFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathPiFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathRandFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathCotFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathTranscendentalFunctions => true;

    /// <inheritdoc />
    public override string MathNaturalLogExpression(string expression) =>
        $"LOG({expression})";

    /// <inheritdoc />
    public override string MathAtan2Expression(string yExpression, string xExpression) =>
        $"ATN2({yExpression}, {xExpression})";

    /// <inheritdoc />
    public override bool SupportsUpdateDeleteJoinRuntime => true;

    /// <inheritdoc />
    public override string UpdateJoinDerivedSelectSql =>
        @"
UPDATE u
SET u.total = s.total
FROM users u
JOIN (SELECT userid, SUM(amount) AS total FROM orders GROUP BY userid) s ON s.userid = u.id
WHERE u.tenantid = 10";

    /// <inheritdoc />
    public override string DeleteJoinDerivedSelectSql =>
        "DELETE u FROM users u JOIN (SELECT id FROM users WHERE tenantid = 10) s ON s.id = u.id";

    /// <inheritdoc />
    public override bool SupportsGroupByOrdinal => false;

    /// <inheritdoc />
    public override bool SupportsNthValueWindowFunction => false;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareDefinitionAcrossConnections => true;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareRowsAcrossConnections => true;

    /// <inheritdoc />
    public override string TemporaryUsersTableName(FidelityTestContext context) =>
        context.TbUsersFullName.StartsWith("#", StringComparison.Ordinal) ? context.TbUsersFullName : $"#{context.TbUsersFullName}";

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
CREATE TABLE {TemporaryUsersTableName(context)} (
    Id INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    TenantId INT NOT NULL
)";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE IF EXISTS {TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(150) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    Age SMALLINT NULL,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL,
    BirthDate DATE NULL,
    ProfileJson NVARCHAR(MAX) NULL,
    FixedCode CHAR(4) NULL,
    BigCount BIGINT NULL,
    PrecisionValue DECIMAL(18,4) NULL,
    DoubleValue FLOAT NULL,
    GuidValue UNIQUEIDENTIFIER NULL,
    BinaryValue VARBINARY(16) NULL,
    TimeValue TIME NULL,
    DateTimeOffsetValue DATETIMEOFFSET NULL,
    CONSTRAINT CK_{context.TbUsersFullName}_ProfileJson CHECK (ProfileJson IS NULL OR ISJSON(ProfileJson) = 1)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INT NOT NULL,
    Note NVARCHAR(100) NOT NULL,
    OrderNumber NVARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BIT NOT NULL DEFAULT 0,
    OrderedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    DeliveredAt DATETIME2 NULL,
    ExtraJson NVARCHAR(MAX) NULL,
    CONSTRAINT CK_{context.TbOrdersFullName}_ExtraJson CHECK (ExtraJson IS NULL OR ISJSON(ExtraJson) = 1),
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
);
CREATE INDEX IX_{context.TbOrdersFullName}_{context.TbUsersFullName}Id ON {context.TbOrdersFullName} ({context.TbUsers}Id);
CREATE UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber ON {context.TbOrdersFullName} (OrderNumber)";
    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, SYSUTCDATETIME())";

    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', 1, 0.00, SYSUTCDATETIME())"))}";

    /// <inheritdoc />
    public override string InsertOrder(
        FidelityTestContext context,
        int id,
        int userId,
        string note,
        string orderNumber,
        decimal amount,
        int quantity,
        bool isPaid,
        string orderedAtLiteral) =>
        $"INSERT INTO {context.TbOrdersFullName} (Id, {context.TbUsers}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(FidelityTestContext context, int id) =>
        $"SELECT Name FROM {context.TbUsersFullName} WHERE Id = {id}";
    /// <inheritdoc />
    public override string CountJoinForUser(FidelityTestContext context, int userId) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} u INNER JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string UpdateUserNameById(FidelityTestContext context, int id, string newName) =>
        $"UPDATE {context.TbUsersFullName} SET Name = '{newName}' WHERE Id = {id}";

    /// <inheritdoc />
    public override string DeleteUserById(FidelityTestContext context, int id) =>
        $"DELETE FROM {context.TbUsersFullName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName}";

    /// <inheritdoc />
    public override string DateScalar() =>
        "SELECT CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string DecimalTextExpression(string expression, int scale = 2) =>
        $"CONVERT(VARCHAR(50), CAST({expression} AS DECIMAL(38, {scale})))";

    /// <inheritdoc />
    public override string MathSquareExpression(string expression) =>
        $"SQUARE({expression})";

    /// <inheritdoc />
    public override string StringAggregate(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) => $@"
MERGE INTO {context.TbUsersFullName} AS target
USING (SELECT {id} AS Id, '{newName}' AS Name) AS source
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name);";

    /// <inheritdoc />
    public override string CreateSequence(FidelityTestContext context) =>
        $"CREATE SEQUENCE {context.Seq} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(FidelityTestContext context) =>
        $"SELECT NEXT VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(FidelityTestContext context) =>
        $"NEXT VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string CurrentSequenceValue(FidelityTestContext context) =>
        $"SELECT CONVERT(BIGINT, current_value) FROM sys.sequences WHERE name = N'{context.Seq}'";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

    /// <inheritdoc />
    public override string DropSequence(FidelityTestContext context) =>
        $"DROP SEQUENCE IF EXISTS {context.Seq}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Savepoint(string savepointName) => $"SAVE TRANSACTION {savepointName}";

    /// <inheritdoc />
    public override string RollbackToSavepoint(string savepointName) => $"ROLLBACK TRANSACTION {savepointName}";

    /// <summary>
    /// EN: Returns the SQL Server no-op command used when release-savepoint handling is exercised.
    /// PT-br: Retorna o comando sem efeito do SQL Server usado quando o tratamento de release-savepoint eh exercitado.
    /// </summary>
    public override string ReleaseSavepoint(string savepointName) => "SELECT 1";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override bool SupportsJsonQueryFunction => true;

    /// <inheritdoc />
    public override bool SupportsOpenJsonFunction => true;

    /// <inheritdoc />
    public override bool SupportsForJsonClause => true;

    /// <inheritdoc />
    public override bool SupportsApplyClause => true;

    /// <inheritdoc />
    public override bool SupportsStringSplitFunction => true;

    /// <inheritdoc />
    public override bool SupportsStringSplitOrdinalArgument => true;

    /// <inheritdoc />
    public override bool SupportsSqlServerMetadataFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && SqlServerMetadataFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerMetadataIdentifier(string identifier)
        => !string.IsNullOrWhiteSpace(identifier)
            && SqlServerMetadataIdentifierNames.Contains(identifier, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerScalarFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && SqlServerScalarFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerDateFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && SqlServerDateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsSqlServerAggregateFunction(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && SqlServerAggregateFunctionNames.Contains(functionName, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override bool SupportsApproximateAggregateFunction(string functionName)
        => string.Equals(functionName, "APPROX_COUNT_DISTINCT", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT STRING_AGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {context.TbUsersFullName}";
    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.user.name')";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSON_VALUE({jsonColumn}, '$.profile.name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"LEN({expression})";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT DATEADD(day, 1, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "DATEADD(day, 1, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT TOP (1) Name FROM {context.TbUsersFullName} ORDER BY Name, CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    x.Note AS Note
FROM {context.TbUsersFullName} u
CROSS APPLY (SELECT TOP (1) o.Note FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id ORDER BY o.Id DESC) x
ORDER BY u.Id
""";

    /// <inheritdoc />
    public override string OuterApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    x.Note AS Note
FROM {context.TbUsersFullName} u
OUTER APPLY (SELECT TOP (1) o.Note FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id ORDER BY o.Id DESC) x
ORDER BY u.Id
""";
}
