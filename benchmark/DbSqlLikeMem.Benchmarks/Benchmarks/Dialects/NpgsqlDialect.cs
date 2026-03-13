namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class NpgsqlDialect : ProviderSqlDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.Npgsql;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "PostgreSQL / Npgsql";

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
    public override string Upsert(string tableName, int id, string newName) =>
        $"INSERT INTO {tableName} (Id, Name) VALUES ({id}, '{newName}') ON CONFLICT (Id) DO UPDATE SET Name = EXCLUDED.Name";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateSequence(string sequenceName) =>
        $"CREATE SEQUENCE {sequenceName} START WITH 10 INCREMENT BY 1";

    /// <summary>
    /// 
    /// </summary>
    public override string NextSequenceValue(string sequenceName) =>
        $"SELECT nextval('{sequenceName}')";

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
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM {tableName}";

    public override bool SupportsJsonScalarRead => true;

    public override string JsonScalarRead(string jsonLiteral) =>
        $"SELECT CAST('{jsonLiteral}' AS json) ->> 'name'";


    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT STRING_AGG(Name, '{separator}' ORDER BY Name) FROM {tableName}";
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT STRING_AGG(Name, ',' ORDER BY Name) FROM {tableName}";
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT ('{jsonLiteral}'::json -> 'user' ->> 'name')";
    public override string TemporalDateAdd() =>
        "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 day'";
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name LIMIT 1";



    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC LIMIT 1) x ON TRUE";

    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u LEFT JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC LIMIT 1) x ON TRUE";

}
