namespace DbSqlLikeMem.MariaDb.TestTools;

/// <summary>
/// EN: Provides MariaDB-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de MariaDB usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class MariaDbProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.MariaDb;

    /// <inheritdoc />
    public override string DisplayName => "MariaDB";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override bool SupportsInsertReturning => true;

    /// <inheritdoc />
    protected override void ConfigureParameter(DbParameter parameter, DbType dbType)
    {
        if (dbType is DbType.DateTime or DbType.DateTime2)
        {
            parameter.DbType = DbType.String;
            return;
        }

        base.ConfigureParameter(parameter, dbType);
    }

    /// <summary>
    /// EN: Normalizes MariaDB temporal parameter values to an invariant SQL timestamp text.
    /// PT: Normaliza valores temporais de parametros MariaDB para um texto de timestamp SQL invariavel.
    /// </summary>
    /// <param name="dbType">EN: Parameter database type. PT: Tipo de banco do parametro.</param>
    /// <param name="value">EN: Parameter value. PT: Valor do parametro.</param>
    /// <returns>EN: Normalized parameter value. PT: Valor do parametro normalizado.</returns>
    protected override object? NormalizeParameterValue(DbType dbType, object? value)
        => value is null
            ? DBNull.Value
            : (dbType, value) switch
            {
                (DbType.DateTime, DateTime dateTime) => dateTime.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture),
                (DbType.DateTime2, DateTime dateTime) => dateTime.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture),
                _ => value
            };

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150) NULL,
    IsActive BOOLEAN NOT NULL DEFAULT TRUE,
    Age SMALLINT UNSIGNED NULL,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    BirthDate DATE NULL,
    ProfileJson JSON NULL,
    FixedCode CHAR(4) NULL,
    BigCount BIGINT NULL,
    PrecisionValue DECIMAL(18,4) NULL,
    DoubleValue DOUBLE NULL,
    GuidValue CHAR(36) NULL,
    BinaryValue VARBINARY(16) NULL,
    TimeValue TIME NULL,
    DateTimeOffsetValue VARCHAR(40) NULL
)";

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) =>
        $@"
CREATE TEMPORARY TABLE {TemporaryUsersTableName(context)} AS
SELECT CAST(NULL AS SIGNED) AS Id
     , CAST(NULL AS CHAR(100)) AS Name
     , CAST(NULL AS SIGNED) AS TenantId
WHERE 1 = 0";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INT NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BOOLEAN NOT NULL DEFAULT FALSE,
    OrderedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DeliveredAt DATETIME NULL,
    ExtraJson JSON NULL,
    INDEX IX_{context.TbOrdersFullName}_{context.TbUsers}Id ({context.TbUsers}Id),
    UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber (OrderNumber),
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
)";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', TRUE, 0.00, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string InsertUserReturning(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', TRUE, 0.00, CURRENT_TIMESTAMP) RETURNING Id, Name";

    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', TRUE, 0.00, CURRENT_TIMESTAMP)"))}";

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
        $"INSERT INTO {context.TbOrdersFullName} (Id, {context.TbUsers}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "TRUE" : "FALSE")}, {orderedAtLiteral})";

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
        $"SELECT GROUP_CONCAT(Name SEPARATOR ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name) VALUES ({id}, '{newName}') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";

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
        $"SELECT PREVIOUS VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TEMPORARY TABLE IF EXISTS {TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string DropSequence(FidelityTestContext context) =>
        $"DROP SEQUENCE IF EXISTS {context.Seq}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_UNQUOTE(JSON_EXTRACT('{jsonLiteral}', '$.name'))";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR '{separator}') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_UNQUOTE(JSON_EXTRACT('{jsonLiteral}', '$.user.name'))";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSON_UNQUOTE(JSON_EXTRACT({jsonColumn}, '$.profile.name'))";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"CHAR_LENGTH({expression})";

    /// <inheritdoc />
    public override string IntCastExpression(string expression) =>
        $"CAST({expression} AS SIGNED)";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT DATE_ADD(NOW(), INTERVAL 1 DAY)";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "NOW()";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "DATE_ADD(NOW(), INTERVAL 1 DAY)";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE NOW() IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT Name FROM {context.TbUsersFullName} ORDER BY Name, NOW() LIMIT 1";

    /// <inheritdoc />
    public override string PagedNameProjection(string tableName, int offset, int fetch) =>
        $"SELECT Name FROM {tableName} ORDER BY Name LIMIT {fetch} OFFSET {offset}";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    (
        SELECT o.Note
        FROM {context.TbOrdersFullName} o
        WHERE o.{context.TbUsers}Id = u.Id
        ORDER BY o.Id DESC
        LIMIT 1
    ) AS Note
FROM {context.TbUsersFullName} u
WHERE EXISTS (SELECT 1 FROM {context.TbOrdersFullName} o WHERE o.{context.TbUsers}Id = u.Id)
ORDER BY u.Id
""";

    /// <inheritdoc />
    public override string OuterApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    (
        SELECT o.Note
        FROM {context.TbOrdersFullName} o
        WHERE o.{context.TbUsers}Id = u.Id
        ORDER BY o.Id DESC
        LIMIT 1
    ) AS Note
FROM {context.TbUsersFullName} u
ORDER BY u.Id
""";
}
