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
    public override string TemporaryUsersTableName(string tableName) => tableName;

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(string tableName) =>
        $@"
CREATE GLOBAL TEMPORARY TABLE {TemporaryUsersTableName(tableName)} (
    Id INTEGER,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(string tableName) =>
        $"DROP TABLE {TemporaryUsersTableName(tableName)}";

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INTEGER NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150),
    IsActive SMALLINT DEFAULT 1 NOT NULL,
    Age SMALLINT,
    Balance DECIMAL(12,2) DEFAULT 0.00 NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    UpdatedAt TIMESTAMP,
    ProfileJson BLOB SUB_TYPE TEXT
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INTEGER NOT NULL PRIMARY KEY,
    {usersTableName}Id INTEGER NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) DEFAULT 0.00 NOT NULL,
    Quantity INTEGER DEFAULT 1 NOT NULL,
    IsPaid SMALLINT DEFAULT 0 NOT NULL,
    OrderedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    DeliveredAt TIMESTAMP,
    ExtraJson BLOB SUB_TYPE TEXT,
    CONSTRAINT FK_{tableName}_{uId}_{usersTableName} FOREIGN KEY ({usersTableName}Id) REFERENCES {usersTableName}(Id)
);
CREATE INDEX IX_{tableName}_{uId}_{usersTableName}Id ON {tableName}_{uId} ({usersTableName}Id);
CREATE UNIQUE INDEX UX_{tableName}_{uId}_OrderNumber ON {tableName}_{uId} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, CURRENT_TIMESTAMP)";

    /// <inheritdoc />
    public override string InsertUserReturning(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', 1, 0.00, CURRENT_TIMESTAMP) RETURNING Id";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        BuildInsertUsers(tableName, values);

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
    public override string SelectScalarSubquery(string usersTable, string ordersTable) =>
        $"""
SELECT (
    SELECT COUNT(*)
    FROM {ordersTable} o
    WHERE o.{usersTable}Id = 1
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
        "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string StringAggregate(string tableName) =>
        $"SELECT LIST(Name, ',') FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} target
USING (SELECT {id} AS Id, '{newName}' AS Name FROM RDB$DATABASE) source
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(string sequenceName) =>
        $"SELECT NEXT VALUE FOR {sequenceName} FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(string sequenceName) =>
        $"NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string SelectNextSequenceValue(string sequenceName) =>
        $"SELECT NEXT VALUE FOR {sequenceName} AS SeqValue FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string CurrentSequenceValue(string sequenceName) =>
        $"SELECT GEN_ID({sequenceName}, 0) FROM RDB$DATABASE";

    /// <inheritdoc />
    public override string DropTable(string tableName, string uId) =>
        $"DROP TABLE {tableName}_{uId}";

    /// <inheritdoc />
    public override string DropSequence(string sequenceName) =>
        $"DROP SEQUENCE {sequenceName}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT LIST(Name, ',') FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LIST(Name, ',') FROM (SELECT DISTINCT Name FROM {tableName} ORDER BY Name) q";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LIST(Name, '{separator}') FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LIST(Name, ',') FROM {tableName}";

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
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT FIRST 1 Name FROM {tableName} ORDER BY Name";

    /// <inheritdoc />
    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE EXISTS (SELECT 1 FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id)";

    /// <inheritdoc />
    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u";
}
