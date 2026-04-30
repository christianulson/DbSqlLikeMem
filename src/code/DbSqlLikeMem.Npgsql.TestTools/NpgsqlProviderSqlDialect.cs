using System.Text.Json;

namespace DbSqlLikeMem.Npgsql.TestTools;

/// <summary>
/// EN: Provides PostgreSQL-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de PostgreSQL usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class NpgsqlProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Npgsql;

    /// <inheritdoc />
    public override string DisplayName => "PostgreSQL / Npgsql";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsMerge => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsDistinctOnProjection => true;

    /// <inheritdoc />
    public override bool SupportsUpdateDeleteJoinRuntime => true;

    /// <inheritdoc />
    public override bool SupportsWithMaterializedHint => true;

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
        "DELETE FROM users u USING (SELECT id FROM users WHERE tenantid = 10) s WHERE s.id = u.id";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override bool SupportsJsonEachFunction => true;

    /// <inheritdoc />
    public override string JsonEachFunction(string jsonColumn) =>
        $"SELECT key, value FROM json_each({jsonColumn})";

    /// <inheritdoc />
    public override object? NormalizeJsonTableValue(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        if (value is JsonElement jsonElement)
        {
            return jsonElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined
                ? null
                : jsonElement.GetRawText();
        }

        if (value is JsonDocument jsonDocument)
        {
            return NormalizeJsonTableValue(jsonDocument.RootElement);
        }

        return value;
    }

    /// <inheritdoc />
    public override string JsonParameter(string name) =>
        $"CAST({Parameter(name)} AS JSONB)";

    /// <inheritdoc />
    protected override object? NormalizeParameterValue(DbType dbType, object? value)
    {
        if (dbType == DbType.DateTime
            && value is DateTime dateTime
            && dateTime.Kind == DateTimeKind.Unspecified)
        {
            value = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
        }

        if (dbType == DbType.DateTimeOffset
            && value is DateTimeOffset dateTimeOffset
            && dateTimeOffset.Offset != TimeSpan.Zero)
        {
            value = dateTimeOffset.ToUniversalTime();
        }

        return value;
    }

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
CREATE TEMP TABLE {TemporaryUsersTableName(context)} (
    Id INT,
    Name VARCHAR(100),
    TenantId INT
)";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE IF EXISTS {TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150) NULL,
    IsActive BOOLEAN NOT NULL DEFAULT TRUE,
    Age SMALLINT NULL,
    Balance NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    BirthDate DATE NULL,
    ProfileJson JSONB NULL,
    FixedCode CHAR(4) NULL,
    BigCount BIGINT NULL,
    PrecisionValue NUMERIC(18,4) NULL,
    DoubleValue DOUBLE PRECISION NULL,
    GuidValue UUID NULL,
    BinaryValue BYTEA NULL,
    TimeValue TIME NULL,
    DateTimeOffsetValue TIMESTAMPTZ NULL
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INT NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INT NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BOOLEAN NOT NULL DEFAULT FALSE,
    OrderedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson JSONB NULL,
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
);
CREATE INDEX IX_{context.TbOrdersFullName}_{context.TbUsers}Id ON {context.TbOrdersFullName} ({context.TbUsers}Id);
CREATE UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber ON {context.TbOrdersFullName} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', TRUE, 0.00, CURRENT_TIMESTAMP)";

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
        $"SELECT STRING_AGG(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string DistinctOnProjection(FidelityTestContext context) =>
        $"""
SELECT DISTINCT ON (u.Id)
    u.Id AS UserId,
    u.Name AS UserName,
    o.Note
FROM {context.TbUsersFullName} u
LEFT JOIN {context.TbOrdersFullName} o ON o.{context.TbUsers}Id = u.Id
ORDER BY u.Id, o.Id DESC
""";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";

    /// <inheritdoc />
    public override string Merge(FidelityTestContext context, int id, string newName) =>
        $"""
MERGE INTO {context.TbUsersFullName} AS target
USING (SELECT {id} AS Id, '{newName}' AS Name) AS src
ON target.Id = src.Id
WHEN MATCHED THEN
    UPDATE SET Name = src.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (src.Id, src.Name);
""";

    /// <inheritdoc />
    public override string CreateSequence(FidelityTestContext context) =>
        $"CREATE SEQUENCE {context.Seq} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(FidelityTestContext context) =>
        $"SELECT nextval('{context.Seq}')";
    /// <inheritdoc />
    public override string NextSequenceValueExpression(FidelityTestContext context) =>
        $"nextval('{context.Seq}')";

    /// <inheritdoc />
    public override string CurrentSequenceValue(FidelityTestContext context) =>
        "SELECT lastval()";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

    /// <inheritdoc />
    public override string DropSequence(FidelityTestContext context) =>
        $"DROP SEQUENCE IF EXISTS {context.Seq}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSONB_EXTRACT_PATH_TEXT(CAST('{jsonLiteral}' AS jsonb), 'name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT STRING_AGG(Name, '{separator}' ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSONB_EXTRACT_PATH_TEXT(CAST('{jsonLiteral}' AS jsonb), 'user', 'name')";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSONB_EXTRACT_PATH_TEXT({jsonColumn}, 'profile', 'name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"CHAR_LENGTH({expression})";

    /// <inheritdoc />
    public override string StringCastExpression(string expression, int length = 10)
    {
        _ = length;
        return $"CONCAT({expression}, '')";
    }

    /// <inheritdoc />
    public override string DecimalTextExpression(string expression, int scale = 2)
    {
        var fractionalPattern = scale > 0 ? "." + new string('0', scale) : string.Empty;
        return $"TO_CHAR({expression}, 'FM9999999990{fractionalPattern}')";
    }

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT (CURRENT_TIMESTAMP + INTERVAL '1 day')";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "(CURRENT_TIMESTAMP + INTERVAL '1 day')";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT Name FROM {context.TbUsersFullName} ORDER BY CURRENT_TIMESTAMP, Name LIMIT 1";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    x.Note AS Note
FROM {context.TbUsersFullName} u
JOIN LATERAL (
    SELECT o.Note
    FROM {context.TbOrdersFullName} o
    WHERE o.{context.TbUsers}Id = u.Id
    ORDER BY o.Id DESC
    LIMIT 1
) x ON TRUE
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
LEFT JOIN LATERAL (
    SELECT o.Note
    FROM {context.TbOrdersFullName} o
    WHERE o.{context.TbUsers}Id = u.Id
    ORDER BY o.Id DESC
    LIMIT 1
) x ON TRUE
ORDER BY u.Id
""";
}
