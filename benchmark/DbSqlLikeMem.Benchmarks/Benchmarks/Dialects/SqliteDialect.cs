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
}
