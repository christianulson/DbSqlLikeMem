using System.Text;

namespace DbSqlLikeMem.Sqlite.TestTools;

/// <summary>
/// EN: Provides SQLite-specific SQL snippets used by the shared benchmark and fidelity helpers.
/// PT: Fornece trechos SQL especificos de SQLite usados pelos helpers compartilhados de benchmark e fidelidade.
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
    public override string CreateUsersTable(string tableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INTEGER NOT NULL PRIMARY KEY,
    Name TEXT NOT NULL,
    Email TEXT NULL,
    IsActive INTEGER NOT NULL DEFAULT 1,
    Age INTEGER NULL,
    Balance NUMERIC NOT NULL DEFAULT 0.00,
    CreatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TEXT NULL,
    ProfileJson TEXT NULL
)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName, string usersTableName, string uId) =>
        $@"
CREATE TABLE {tableName}_{uId} (
    Id INTEGER NOT NULL PRIMARY KEY,
    {usersTableName}Id INTEGER NOT NULL,
    Note TEXT NOT NULL,
    OrderNumber TEXT NOT NULL,
    Amount NUMERIC NOT NULL DEFAULT 0.00,
    Quantity INTEGER NOT NULL DEFAULT 1,
    IsPaid INTEGER NOT NULL DEFAULT 0,
    OrderedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DeliveredAt TEXT NULL,
    ExtraJson TEXT NULL,
    CONSTRAINT FK_{tableName}_{uId}_{usersTableName} FOREIGN KEY ({usersTableName}Id) REFERENCES {usersTableName}_{uId}(Id)
);
CREATE INDEX IX_{tableName}_{uId}_{usersTableName}Id ON {tableName}_{uId} ({usersTableName}Id);
CREATE UNIQUE INDEX UX_{tableName}_{uId}_OrderNumber ON {tableName}_{uId} (OrderNumber)";

    /// <inheritdoc />
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}')";

    /// <inheritdoc />
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        BuildInsertUsers(tableName, values);

    private static string BuildInsertUsers(string tableName, (int id, string name)[] values)
    {
        var sb = new StringBuilder(64 + values.Length * 16);
        sb.Append("INSERT INTO ");
        sb.Append(tableName);
        sb.Append(" (Id, Name) VALUES ");

        for (var i = 0; i < values.Length; i++)
        {
            if (i > 0)
                sb.Append(',');

            sb.Append('(');
            sb.Append(values[i].id);
            sb.Append(", '");
            sb.Append(values[i].name);
            sb.Append("')");
        }

        return sb.ToString();
    }

    /// <inheritdoc />
    public override string InsertOrder(string tableName, string usersTableName, int id, int userId, string note) =>
        $"INSERT INTO {tableName} (Id, {usersTableName}Id, Note) VALUES ({id}, {userId}, '{note}')";

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
        $"SELECT GROUP_CONCAT(Name, ',') FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT(Id) DO UPDATE SET Name = excluded.Name";

    /// <inheritdoc />
    public override string DropTable(string tableName, string uId) =>
        $"DROP TABLE IF EXISTS {tableName}_{uId}";

    /// <inheritdoc />
    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {tableName} ORDER BY Name)";

    /// <inheritdoc />
    public override bool SupportsJsonScalarRead => true;

    /// <inheritdoc />
    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.name')";

    /// <inheritdoc />
    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT DISTINCT Name FROM {tableName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT GROUP_CONCAT(Name, '{separator}') FROM (SELECT Name FROM {tableName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {tableName} ORDER BY Name) t";

    /// <inheritdoc />
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.user.name')";

    /// <inheritdoc />
    public override string TemporalDateAdd() =>
        "SELECT datetime('2024-01-01 00:00:00', '+1 day')";

    /// <inheritdoc />
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";

    /// <inheritdoc />
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name LIMIT 1";
}
