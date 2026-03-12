namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class SqliteDialect : ProviderSqlDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.Sqlite;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "SQLite";

    /// <summary>
    /// 
    /// </summary>
    public override bool SupportsUpsert => true;

    /// <summary>
    /// 
    /// </summary>
    public override string CreateUsersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INTEGER NOT NULL PRIMARY KEY, Name TEXT NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateOrdersTable(string tableName) =>
        $"CREATE TABLE {tableName} (Id INTEGER NOT NULL PRIMARY KEY, UserId INTEGER NOT NULL, Note TEXT NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string InsertUser(string tableName, int id, string name) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}')";

    /// <summary>
    /// 
    /// </summary>
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES {(string.Join(",", values.Select(_ => $"({_.id}, '{_.name}')")))}";

    /// <summary>
    /// 
    /// </summary>
    public override string InsertOrder(string tableName, int id, int userId, string note) =>
        $"INSERT INTO {tableName} (Id, UserId, Note) VALUES ({id}, {userId}, '{note}')";

    /// <summary>
    /// 
    /// </summary>
    public override string SelectUserNameById(string tableName, int id) =>
        $"SELECT Name FROM {tableName} WHERE Id = {id}";

    /// <summary>
    /// 
    /// </summary>
    public override string CountJoinForUser(string usersTable, string ordersTable, int userId) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o ON o.UserId = u.Id WHERE u.Id = {userId}";

    /// <summary>
    /// 
    /// </summary>
    public override string UpdateUserNameById(string tableName, int id, string newName) =>
        $"UPDATE {tableName} SET Name = '{newName}' WHERE Id = {id}";

    /// <summary>
    /// 
    /// </summary>
    public override string DeleteUserById(string tableName, int id) =>
        $"DELETE FROM {tableName} WHERE Id = {id}";

    /// <summary>
    /// 
    /// </summary>
    public override string CountRows(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string DateScalar() =>
        "SELECT CURRENT_TIMESTAMP";

    /// <summary>
    /// 
    /// </summary>
    public override string StringAggregate(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string Upsert(string tableName, int id, string newName) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT(Id) DO UPDATE SET Name = excluded.Name";

    /// <summary>
    /// 
    /// </summary>
    public override string DropTable(string tableName) =>
        $"DROP TABLE IF EXISTS {tableName}";


    public override string StringAggregateOrdered(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {tableName} ORDER BY Name)";

    public override bool SupportsJsonScalarRead => true;

    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.name')";


    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT DISTINCT Name FROM {tableName} ORDER BY Name) t";
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT GROUP_CONCAT(Name, '{separator}') FROM (SELECT Name FROM {tableName} ORDER BY Name) t";
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT GROUP_CONCAT(Name, ',') FROM (SELECT Name FROM {tableName} ORDER BY Name) t";
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT json_extract('{jsonLiteral}', '$.user.name')";
    public override string TemporalDateAdd() =>
        "SELECT datetime('2024-01-01 00:00:00', '+1 day')";
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name LIMIT 1";

}
