namespace DbSqlLikeMem.SqlServer.TestTools;

/// <summary>
/// EN: Provides SQL Server-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de SQL Server usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public class SqlServerProviderSqlDialect : ProviderSqlDialect
{
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
    public override bool SupportsUpdateDeleteJoinRuntime => true;

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
        $"SELECT CURRENT VALUE FOR {context.Seq}";

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

    /// <inheritdoc />
    public override string ReleaseSavepoint(string savepointName) => "SELECT 1";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

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
