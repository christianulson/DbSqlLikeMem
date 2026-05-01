using System.Text;

namespace DbSqlLikeMem.Sqlite.TestTools;

/// <summary>
/// EN: Provides SQLite-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT-br: Fornece trechos SQL especificos de SQLite usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class SqliteProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Sqlite;

    /// <inheritdoc />
    public override string DisplayName => "SQLite";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsMathFunctions => true;

    /// <inheritdoc />
    public override bool SupportsMathLog2Function => true;

    /// <inheritdoc />
    public override bool SupportsMathLogBaseFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathPiFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathTruncFunction => true;

    /// <inheritdoc />
    public override bool SupportsMathTruncScaleFunction => false;

    /// <inheritdoc />
    public override bool SupportsMathTranscendentalFunctions => true;

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(FidelityTestContext context) => $@"
CREATE TEMPORARY TABLE {TemporaryUsersTableName(context)} AS
SELECT CAST(NULL AS INTEGER) AS Id
     , CAST(NULL AS TEXT) AS Name
     , CAST(NULL AS INTEGER) AS TenantId
WHERE 1 = 0
";

    /// <inheritdoc />
    public override string CreateUsersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbUsersFullName} (
    Id INTEGER NOT NULL PRIMARY KEY,
    Name TEXT NOT NULL,
    Email TEXT NULL,
    IsActive INTEGER NOT NULL DEFAULT 1,
    Age INTEGER NULL,
    Balance NUMERIC NOT NULL DEFAULT 0.00,
    CreatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TEXT NULL,
    BirthDate DATE NULL,
    ProfileJson TEXT NULL,
    FixedCode CHAR(4) NULL,
    BigCount BIGINT NULL,
    PrecisionValue NUMERIC(18,4) NULL,
    DoubleValue REAL NULL,
    GuidValue TEXT NULL,
    BinaryValue BLOB NULL,
    TimeValue TEXT NULL,
    DateTimeOffsetValue TEXT NULL
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(FidelityTestContext context) =>
        $@"
CREATE TABLE {context.TbOrdersFullName} (
    Id INTEGER NOT NULL PRIMARY KEY,
    {context.TbUsers}Id INTEGER NOT NULL,
    Note TEXT NOT NULL,
    OrderNumber TEXT NOT NULL,
    Amount NUMERIC NOT NULL DEFAULT 0.00,
    Quantity INTEGER NOT NULL DEFAULT 1,
    IsPaid INTEGER NOT NULL DEFAULT 0,
    OrderedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DeliveredAt TEXT NULL,
    ExtraJson TEXT NULL,
    CONSTRAINT FK_{context.TbOrdersFullName}_{context.TbUsers}Id_{context.TbUsersFullName} FOREIGN KEY ({context.TbUsers}Id) REFERENCES {context.TbUsersFullName}(Id)
);
CREATE INDEX IX_{context.TbOrdersFullName}_{context.TbUsers}Id ON {context.TbOrdersFullName} ({context.TbUsers}Id);
CREATE UNIQUE INDEX UX_{context.TbOrdersFullName}_OrderNumber ON {context.TbOrdersFullName} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(FidelityTestContext context, int id, string name) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string InsertUsers(FidelityTestContext context, params (int id, string name)[] values) =>
        BuildInsertUsers(context.TbUsersFullName, values);
    private static string BuildInsertUsers(string tableName, (int id, string name)[] values)
    {
        var sb = new StringBuilder(64 + values.Length * 16);
        sb.Append("INSERT INTO ");
        sb.Append(tableName);
        sb.Append(" (Id, Name, IsActive, Balance, CreatedAt) VALUES ");

        for (var i = 0; i < values.Length; i++)
        {
            if (i > 0)
                sb.Append(',');

            sb.Append('(');
            sb.Append(values[i].id);
            sb.Append(", '");
            sb.Append(values[i].name);
            sb.Append("', 1, 0.00, CURRENT_TIMESTAMP)");
        }

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
        $"INSERT INTO {context.TbOrdersFullName} (Id, {context.TbUsers}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "1" : "0")}, {orderedAtLiteral})";

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
        $"SELECT GROUP_CONCAT(Name, ',') FROM {context.TbUsersFullName}";

    /// <inheritdoc />
    public override string Upsert(FidelityTestContext context, int id, string newName) =>
        $"INSERT INTO {context.TbUsersFullName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT(Id) DO UPDATE SET Name = excluded.Name";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {context.TbUsersFullName} ORDER BY Name)";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override bool SupportsJsonEachFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonTreeFunction => true;

    /// <inheritdoc />
    public override bool SupportsJsonTableFunctions => true;

    /// <inheritdoc />
    public override bool SupportsWithMaterializedHint => true;

    /// <inheritdoc />
    public override string JsonEachFunction(string jsonColumn) =>
        $"SELECT key, value FROM json_each({jsonColumn})";

    /// <inheritdoc />
    public override string JsonTreeFunction(string jsonColumn) =>
        $"SELECT key, value, type, id, parent, path FROM json_tree({jsonColumn})";

    /// <inheritdoc />
    public override bool SupportsOuterApplyProjection => false;

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT DISTINCT Name FROM {context.TbUsersFullName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(FidelityTestContext context, string separator) =>
        $"SELECT GROUP_CONCAT(Name, '{separator}') FROM (SELECT Name FROM {context.TbUsersFullName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(FidelityTestContext context) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {context.TbUsersFullName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.user.name')";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"json_extract({jsonColumn}, '$.profile.name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"LENGTH({expression})";

    /// <inheritdoc />
    public override string StringCastExpression(string expression, int length = 10) =>
        $"PRINTF('%s', {expression})";

    /// <inheritdoc />
    public override string DecimalTextExpression(string expression, int scale = 2) =>
        $"PRINTF('%.{scale}f', {expression})";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT datetime(CURRENT_TIMESTAMP, '+1 day')";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT_TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "datetime(CURRENT_TIMESTAMP, '+1 day')";

    /// <inheritdoc />
    public override string StringPrefixExpression(string expression, int length) =>
        $"SUBSTR({expression}, 1, {length})";

    /// <inheritdoc />
    public override string TemporalNowWhere(FidelityTestContext context) =>
        $"SELECT COUNT(*) FROM {context.TbUsersFullName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(FidelityTestContext context) =>
        $"SELECT Name FROM {context.TbUsersFullName} ORDER BY Name, CURRENT_TIMESTAMP LIMIT 1";

    /// <inheritdoc />
    public override string PagedNameProjection(string tableName, int offset, int fetch) =>
        $"SELECT Name FROM {tableName} ORDER BY Name LIMIT {fetch} OFFSET {offset}";
}
