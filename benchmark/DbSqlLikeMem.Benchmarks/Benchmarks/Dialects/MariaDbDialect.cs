namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// EN: Provides MariaDB benchmark SQL snippets built on the shared MySQL-family shape.
/// PT-br: Fornece trechos SQL de benchmark para MariaDB com base na mesma forma compartilhada da familia MySQL.
/// </summary>
public sealed class MariaDbDialect : ProviderSqlDialect
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
    public override string CreateUsersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, Name VARCHAR(100) NOT NULL)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, UserId INT NOT NULL, Note VARCHAR(100) NOT NULL)";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}')";

    /// <inheritdoc />
    public override string InsertUserReturning(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}') RETURNING Id, Name";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}')"))}";

    /// <inheritdoc />
    public override string InsertOrder(string tableName, int id, int userId, string note) =>
        $"INSERT INTO {tableName} (Id, UserId, Note) VALUES ({id}, {userId}, '{note}')";

    /// <inheritdoc />
    public override string SelectUserNameById(string tableName, int id) =>
        $"SELECT Name FROM {tableName} WHERE Id = {id}";

    /// <inheritdoc />
    public override string CountJoinForUser(string usersTable, string ordersTable, int userId) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o ON o.UserId = u.Id WHERE u.Id = {userId}";

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
        $"SELECT GROUP_CONCAT(Name SEPARATOR ',') FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{newName}') ON DUPLICATE KEY UPDATE Name = VALUES(Name)";

    /// <inheritdoc />
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <inheritdoc />
    public override string NextSequenceValue(string sequenceName) =>
        $"SELECT NEXT VALUE FOR {sequenceName}";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

    /// <inheritdoc />
    public override string DropSequence(string sequenceName) =>
        $"DROP SEQUENCE IF EXISTS {sequenceName}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM {tableName}";

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT JSON_UNQUOTE(JSON_EXTRACT('{jsonLiteral}', '$.name'))";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM (SELECT DISTINCT Name FROM {tableName}) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR '{separator}') FROM {tableName}";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT GROUP_CONCAT(Name ORDER BY Name SEPARATOR ',') FROM {tableName}";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_UNQUOTE(JSON_EXTRACT('{jsonLiteral}', '$.user.name'))";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL 1 DAY)";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name LIMIT 1";

    /// <inheritdoc />
    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC LIMIT 1) x ON TRUE";

    /// <inheritdoc />
    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u LEFT JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC LIMIT 1) x ON TRUE";
}
