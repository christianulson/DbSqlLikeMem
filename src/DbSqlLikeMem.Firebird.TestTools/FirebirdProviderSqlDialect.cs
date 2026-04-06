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
    IsActive SMALLINT NOT NULL DEFAULT 1,
    Age SMALLINT,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INTEGER NOT NULL DEFAULT 1,
    IsPaid SMALLINT NOT NULL DEFAULT 0,
    OrderedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', 1, 0.00, CURRENT_TIMESTAMP)"))}";

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
        $"SELECT LIST(Name, ',') FROM {tableName} ORDER BY Name";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LIST(Name, ',') FROM (SELECT DISTINCT Name FROM {tableName}) q";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LIST(Name, '{separator}') FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LIST(Name, ',') FROM {tableName}";

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

