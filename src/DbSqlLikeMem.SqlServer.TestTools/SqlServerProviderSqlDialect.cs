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
    public override string TemporaryUsersTableName(string tableName) =>
        tableName.StartsWith("#", StringComparison.Ordinal) ? tableName : $"#{tableName}";

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(string tableName) =>
        $@"
CREATE TABLE {TemporaryUsersTableName(tableName)} (
    Id INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL
)
";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(string tableName) =>
        $"DROP TABLE IF EXISTS {TemporaryUsersTableName(tableName)}";

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INT NOT NULL PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(150) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    Age SMALLINT NULL,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL,
    ProfileJson NVARCHAR(MAX) NULL,
    CONSTRAINT CK_{tableName}_{uId}_ProfileJson CHECK (ProfileJson IS NULL OR ISJSON(ProfileJson) = 1)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INT NOT NULL PRIMARY KEY,
    {usersTableName}Id INT NOT NULL,
    Note NVARCHAR(100) NOT NULL,
    OrderNumber NVARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BIT NOT NULL DEFAULT 0,
    OrderedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    DeliveredAt DATETIME2 NULL,
    ExtraJson NVARCHAR(MAX) NULL,
    CONSTRAINT CK_{tableName}_{uId}_ExtraJson CHECK (ExtraJson IS NULL OR ISJSON(ExtraJson) = 1),
    CONSTRAINT FK_{tableName}_{uId}_{usersTableName} FOREIGN KEY ({usersTableName}Id) REFERENCES {usersTableName}(Id)
);
CREATE INDEX IX_{tableName}_{uId}_{usersTableName}Id ON {tableName}_{uId} ({usersTableName}Id);
    CREATE UNIQUE INDEX UX_{tableName}_{uId}_OrderNumber ON {tableName}_{uId} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, SYSUTCDATETIME())";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', 1, 0.00, SYSUTCDATETIME())"))}";

    /// <inheritdoc />
    public override string InsertOrder(
        string tableName,
        string usersTableName,
        int id,
        int userId,
        string note,
        string orderNumber,
        decimal amount,
        int quantity,
        bool isPaid,
        string orderedAtLiteral) =>
        $"INSERT INTO {tableName} (Id, {usersTableName}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(string tableName, int id) =>
        $"SELECT Name FROM {tableName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountJoinForUser(string usersTable, string ordersTable, int userId) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id WHERE u.Id = {userId}";

    /// <inheritdoc />
    public override string UpdateUserNameById(string tableName, int id, string newName) =>
        $"UPDATE {tableName} SET Name = '{newName}' WHERE Id = {id}";

    /// <inheritdoc />
    public override string DeleteUserById(string tableName, int id) =>
        $"DELETE FROM {tableName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName}";

    /// <inheritdoc />
    public override string DateScalar() =>
        "SELECT CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string StringAggregate(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} AS target
USING (SELECT {id} AS Id, '{newName}' AS Name) AS source
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name);";

    /// <inheritdoc />
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(string sequenceName) =>
        $"SELECT NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(string sequenceName) =>
        $"NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string DropTable(string tableName, string uId) =>
        $"DROP TABLE IF EXISTS {tableName}_{uId}";

    /// <inheritdoc />
    public override string DropSequence(string sequenceName) =>
        $"DROP SEQUENCE IF EXISTS {sequenceName}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

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
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT STRING_AGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

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
        "SELECT DATEADD(day, 1, CAST('2024-01-01T00:00:00' AS datetime2))";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "DATEADD(day, 1, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT TOP (1) Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name";

    /// <inheritdoc />
    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u CROSS APPLY (SELECT TOP (1) o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC) x";

    /// <inheritdoc />
    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u OUTER APPLY (SELECT TOP (1) o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC) x";
}
