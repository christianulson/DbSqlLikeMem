namespace DbSqlLikeMem.Db2.TestTools;

/// <summary>
/// EN: Provides DB2-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de DB2 usados pelos helpers compartilhados de benchmark e fidelidade.
/// </summary>
public sealed class Db2ProviderSqlDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Db2;

    /// <inheritdoc />
    public override string DisplayName => "DB2";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override bool SupportsSequence => true;

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INT NOT NULL PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(150) NULL,
    IsActive BOOLEAN NOT NULL DEFAULT TRUE,
    Age SMALLINT NULL,
    Balance DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    UpdatedAt TIMESTAMP NULL,
    ProfileJson CLOB(2000) NULL
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INT NOT NULL PRIMARY KEY,
    {usersTableName}Id INT NOT NULL,
    Note VARCHAR(100) NOT NULL,
    OrderNumber VARCHAR(40) NOT NULL,
    Amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    Quantity INT NOT NULL DEFAULT 1,
    IsPaid BOOLEAN NOT NULL DEFAULT FALSE,
    OrderedAt TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
    DeliveredAt TIMESTAMP NULL,
    ExtraJson CLOB(2000) NULL,
    CONSTRAINT FK_{tableName}_{uId}_{usersTableName} FOREIGN KEY ({usersTableName}Id) REFERENCES {usersTableName}(Id)
);
CREATE INDEX IX_{tableName}_{uId}_{usersTableName}Id ON {tableName}_{uId} ({usersTableName}Id);
CREATE UNIQUE INDEX UX_{tableName}_{uId}_OrderNumber ON {tableName}_{uId} (OrderNumber)";

    /// <inheritdoc />
    public override string TemporaryUsersTableName(string tableName) =>
        tableName;

    /// <inheritdoc />
    public override string CreateTemporaryUsersTable(string tableName) =>
        $@"
DECLARE GLOBAL TEMPORARY TABLE SESSION.{TemporaryUsersTableName(tableName)} (
    Id INT,
    Name VARCHAR(100)
) ON COMMIT PRESERVE ROWS NOT LOGGED";

    /// <inheritdoc />
    public override string DropTemporaryUsersTable(string tableName) =>
        $"DROP TEMPORARY TABLE {TemporaryUsersTableName(tableName)}";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES ({id}, '{name}', TRUE, 0.00, CURRENT TIMESTAMP)";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $"INSERT INTO {tableName} (Id, Name, IsActive, Balance, CreatedAt) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}', TRUE, 0.00, CURRENT TIMESTAMP)"))}";

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
        $"INSERT INTO {tableName} (Id, {usersTableName}Id, Note, OrderNumber, Amount, Quantity, IsPaid, OrderedAt) VALUES ({id}, {userId}, '{note}', '{orderNumber}', {amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)}, {quantity}, {(isPaid ? "TRUE" : "FALSE")}, {orderedAtLiteral})";

    /// <inheritdoc />
    public override string SelectUserNameById(string tableName, int id) =>
        $"SELECT Name FROM {tableName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string SelectParameterProjection(string projectionList) =>
        $"SELECT {projectionList} FROM SYSIBM.SYSDUMMY1";

    /// <inheritdoc />
    public override string SelectScalarSubquery(string usersTable, string ordersTable) =>
        $"SELECT (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = 1) FROM SYSIBM.SYSDUMMY1";

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
        "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1";

    /// <inheritdoc />
    public override string StringAggregate(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) =>
        $@"
MERGE INTO {tableName} target
USING (VALUES ({id}, '{newName}')) source (Id, Name)
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <inheritdoc />
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(string sequenceName) =>
        $"VALUES NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string NextSequenceValueExpression(string sequenceName) =>
        $"NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string CurrentSequenceValue(string sequenceName) =>
        $"VALUES PREVIOUS VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string Savepoint(string savepointName) =>
        $"SAVEPOINT {savepointName} ON ROLLBACK RETAIN CURSORS";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.user.name')";

    /// <inheritdoc />
    public override string JsonProfileNameExpression(string jsonColumn) =>
        $"JSON_VALUE({jsonColumn}, 'strict $.profile.name')";

    /// <inheritdoc />
    public override string StringLengthExpression(string expression) =>
        $"LENGTH({expression})";

    /// <inheritdoc />
    public override string StringCastExpression(string expression, int length = 10) =>
        $"TO_CHAR({expression})";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "VALUES TIMESTAMPADD(16, 1, CURRENT TIMESTAMP)";

    /// <inheritdoc />
    public override string TemporalCurrentTimestampExpression() => "CURRENT TIMESTAMP";

    /// <inheritdoc />
    public override string TemporalDateAddExpression() =>
        "TIMESTAMPADD(16, 1, CURRENT TIMESTAMP)";

    /// <inheritdoc />
    public override string StringPrefixExpression(string expression, int length) =>
        $"SUBSTR({expression}, 1, {length})";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY Name FETCH FIRST 1 ROW ONLY";

    /// <inheritdoc />
    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";

    /// <inheritdoc />
    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u LEFT JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";
}
