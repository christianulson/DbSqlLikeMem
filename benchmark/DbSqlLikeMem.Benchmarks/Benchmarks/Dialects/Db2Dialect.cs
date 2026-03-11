namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class Db2Dialect : ProviderSqlDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.Db2;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "DB2";

    /// <summary>
    /// 
    /// </summary>
    public override bool SupportsUpsert => true;

    /// <summary>
    /// 
    /// </summary>
    public override bool SupportsSequence => true;

    /// <summary>
    /// 
    /// </summary>
    public override string CreateUsersTable(string tableName) =>
            $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, Name VARCHAR(100) NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateOrdersTable(string tableName) =>
            $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, UserId INT NOT NULL, Note VARCHAR(100) NOT NULL)";

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
            "VALUES CURRENT TIMESTAMP";

    /// <summary>
    /// 
    /// </summary>
    public override string StringAggregate(string tableName) =>
            $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} AS target
USING (VALUES ({id}, '{newName}')) AS source (Id, Name)
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name)";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateSequence(string sequenceName) =>
            $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <summary>
    /// 
    /// </summary>
    public override string NextSequenceValue(string sequenceName) =>
            $"VALUES NEXT VALUE FOR {sequenceName}";
}
