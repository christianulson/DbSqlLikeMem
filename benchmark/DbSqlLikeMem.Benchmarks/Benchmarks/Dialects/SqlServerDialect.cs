namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public class SqlServerDialect : ProviderSqlDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.SqlServer;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "SQL Server";

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
            $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, Name NVARCHAR(100) NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateOrdersTable(string tableName) =>
            $"CREATE TABLE {tableName} (Id INT NOT NULL PRIMARY KEY, UserId INT NOT NULL, Note NVARCHAR(100) NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string InsertUser(string tableName, int id, string name) =>
            $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{name}')";

    /// <summary>
    /// 
    /// </summary>
    public override string InsertUsers(string tableName, params (int id, string name)[] values) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES {string.Join(",", values.Select(_ => $"({_.id}, '{_.name}')"))}";

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
            $"SELECT STRING_AGG(Name, ',') FROM {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} AS target
USING (SELECT {id} AS Id, '{newName}' AS Name) AS source
ON target.Id = source.Id
WHEN MATCHED THEN UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN INSERT (Id, Name) VALUES (source.Id, source.Name);";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateSequence(string sequenceName) =>
            $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <summary>
    /// 
    /// </summary>
    public override string NextSequenceValue(string sequenceName) =>
            $"SELECT NEXT VALUE FOR {sequenceName}";

    /// <summary>
    /// 
    /// </summary>
    public override string DropTable(string tableName) =>
            $"DROP TABLE IF EXISTS {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string DropSequence(string sequenceName) =>
            $"DROP SEQUENCE IF EXISTS {sequenceName}";


    public override string StringAggregateOrdered(string tableName) =>
            $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    public override string Savepoint(string savepointName) => $"SAVEPOINT {savepointName}";

    public override string RollbackToSavepoint(string savepointName) => $"ROLLBACK TO SAVEPOINT {savepointName}";

    public override string ReleaseSavepoint(string savepointName) => "SELECT 1";

    public override bool SupportsJsonScalarRead => true;

    public override string JsonScalarRead(string jsonLiteral) =>
            $"SELECT JSON_VALUE('{jsonLiteral}', '$.name')";

    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT STRING_AGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT STRING_AGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.user.name')";
    public override string TemporalDateAdd() =>
        "SELECT DATEADD(day, 1, CAST('2024-01-01T00:00:00' AS datetime2))";
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT TOP (1) Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name";

    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u CROSS APPLY (SELECT TOP (1) o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC) x";

    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u OUTER APPLY (SELECT TOP (1) o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC) x";

}
