using System.Data;
using System.Data.Common;

namespace DbSqlLikeMem.Firebird.TestTools;

/// <summary>
/// EN: Provides Firebird-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de Firebird usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class FirebirdProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Firebird;

    /// <inheritdoc />
    public override string DisplayName => "Firebird";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => false;

    /// <inheritdoc />
    public override bool SupportsReleaseSavepoints => true;

    /// <inheritdoc />
    public override bool SupportsDateTimeOffsetInputOutputParameters => false;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareDefinitionAcrossConnections => true;

    /// <inheritdoc />
    public override bool GlobalTemporaryTablesShareRowsAcrossConnections => false;

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
CREATE GLOBAL TEMPORARY TABLE {TemporaryUsersTableName(context)} (
    Id INTEGER,
    Name VARCHAR(100),
    TenantId INTEGER
) ON COMMIT PRESERVE ROWS";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(FidelityTestContext context) =>
        $"DROP TABLE {TemporaryUsersTableName(context)}";

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INTEGER NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150),
    IsActive SMALLINT DEFAULT 1 NOT NULL,
    Age SMALLINT,
    Balance DECIMAL(12,2) DEFAULT 0.00 NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP,
    BirthDate DATE,
    ProfileJson BLOB SUB_TYPE TEXT,
    FixedCode CHAR(4),
    BigCount BIGINT,
    PrecisionValue DECIMAL(18,4),
    DoubleValue DOUBLE PRECISION,
    GuidValue VARCHAR(36),
    BinaryValue VARBINARY(16),
    TimeValue TIME,
    DateTimeOffsetValue VARCHAR(40)
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INTEGER NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INTEGER NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) DEFAULT 0.00 NOT NULL,
    Quantity INTEGER DEFAULT 1 NOT NULL,
    IsPaid SMALLINT DEFAULT 0 NOT NULL,
    OrderedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    DeliveredAt TIMESTAMP,
    ExtraJson BLOB SUB_TYPE TEXT,
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
);
CREATE INDEX IX_{context.TbOrdersFullName}_{context.TbUsers}Id ON {context.TbOrdersFullName} ({context.TbUsers}Id);
CREATE UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber ON {context.TbOrdersFullName} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    protected override void ConfigureParameter(DbParameter parameter, DbType dbType)
    {
        if (dbType == DbType.Currency || dbType == DbType.DateTimeOffset || dbType == DbType.Guid)
        {
            return;
        }

        parameter.DbType = dbType;
    }

    /// <inheritdoc />
    protected override object? NormalizeParameterValue(DbType dbType, object? value) =>
        NormalizeFirebirdParameterValue(dbType, value);

    /// <inheritdoc />
    public override string InsertUserReturning(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, CURRENT_TIMESTAMP) RETURNING Id";

    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        BuildInsertUsers(context.TbUsersFullName, values);
    private static string BuildInsertUsers(string tableName, (int id, string name)[] values)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("INSERT INTO ");
        sb.Append(tableName);
        sb.AppendLine(" (Id, Name, IsActive, Balance, CreatedAt)");
        sb.AppendLine("SELECT counter, 'User-' || counter, 1, 0.00, CURRENT_TIMESTAMP");
        sb.AppendLine("FROM (");

        for (var i = 0; i < values.Length; i++)
        {
            if (i == 0)
            {
                sb.Append("    SELECT ");
            }
            else
            {
                sb.Append("    UNION ALL SELECT ");
            }

            sb.Append(values[i].id);
            sb.Append(" AS counter FROM RDB$DATABASE");
            sb.AppendLine();
        }

        sb.Append(')');
        sb.AppendLine(" q");
        return sb.ToString();
    }

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
    public override string SelectScalarSubquery(FidelityTestContext context) =>
        $"""
SELECT (
    SELECT COUNT(*)
    FROM {context.TbOrdersFullName} o
    WHERE o.{context.TbUsers}Id = 1
)
FROM RDB$DATABASE
""";

    /// <inheritdoc />
    public override string SelectParameterProjection(string projectionList) =>
        $@"
SELECT
    {projectionList}
FROM RDB$DATABASE";

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
        "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string StringAggregate(FidelityTestContext context) =>
        $"SELECT LIST(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) => $@"
MERGE INTO {context.TbUsersFullName} target
USING (SELECT {id} AS Id, '{newName}' AS Name FROM RDB$DATABASE) source
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(FidelityTestContext context) =>
        $"CREATE SEQUENCE {context.Seq} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(FidelityTestContext context) =>
        $"SELECT NEXT VALUE FOR {context.Seq} FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(FidelityTestContext context) =>
        $"NEXT VALUE FOR {context.Seq}";

    /// <inheritdoc />
    public override string SelectNextSequenceValue(FidelityTestContext context) =>
        $"SELECT NEXT VALUE FOR {context.Seq} AS SeqValue FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string CurrentSequenceValue(FidelityTestContext context) =>
        $"SELECT GEN_ID({context.Seq}, 0) FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE {tableName}";

    /// <inheritdoc />
    public override string DropSequence(FidelityTestContext context) =>
        $"DROP SEQUENCE {context.Seq}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT LIST(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT LIST(Name, ',') FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName} ORDER BY Name) q";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT LIST(Name, '{separator}') FROM {context.TbUsersFullName}";
    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT LIST(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string StringPrefixExpression(string expression, int length) =>
        $"SUBSTRING({expression} FROM 1 FOR {length})";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"CHAR_LENGTH({expression})";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT DATEADD(1 DAY TO CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "DATEADD(1 DAY TO CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TempTbFullName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT FIRST 1 Name FROM {context.TempTbFullName} ORDER BY Name";

    /// <inheritdoc />
    public override string CrossApplyProjection(FidelityTestContext context) =>
        $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    (
        SELECT FIRST 1 o.Note
        FROM {context.TbOrdersFullName} o
        WHERE o.{context.TbUsers}Id = u.Id
        ORDER BY o.Id DESC
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
        SELECT FIRST 1 o.Note
        FROM {context.TbOrdersFullName} o
        WHERE o.{context.TbUsers}Id = u.Id
        ORDER BY o.Id DESC
    ) AS Note
FROM {context.TbUsersFullName} u
ORDER BY u.Id
""";
}
