using System.Text;

namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// EN: Provides benchmark SQL statements using SQLite syntax.
/// PT: Fornece comandos SQL de benchmark usando sintaxe do SQLite.
/// </summary>
public sealed class SqliteDialect : ProviderSqlDialect
{
    /// <inheritdoc />
    public override ProviderId Provider => ProviderId.Sqlite;

    /// <inheritdoc />
    public override string DisplayName => "SQLite";

    /// <inheritdoc />
    public override bool SupportsUpsert => true;

    /// <inheritdoc />
    public override string CreateUsersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INTEGER NOT NULL PRIMARY KEY, Name TEXT NOT NULL)";

    /// <inheritdoc />
    public override string CreateOrdersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INTEGER NOT NULL PRIMARY KEY, UserId INTEGER NOT NULL, Note TEXT NOT NULL)";

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
        $"SELECT GROUP_CONCAT(Name, ',') FROM {tableName}";

    /// <inheritdoc />
    public override string Upsert(string tableName, int id, string newName) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT(Id) DO UPDATE SET Name = excluded.Name";

    /// <inheritdoc />
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";

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
