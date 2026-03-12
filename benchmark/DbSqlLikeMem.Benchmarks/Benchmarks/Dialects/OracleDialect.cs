namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class OracleDialect : ProviderSqlDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.Oracle;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "Oracle";

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
            $"CREATE TABLE {tableName} (Id NUMBER(10) PRIMARY KEY, Name VARCHAR2(100) NOT NULL)";

    /// <summary>
    /// 
    /// </summary>
    public override string CreateOrdersTable(string tableName) =>
            $"CREATE TABLE {tableName} (Id NUMBER(10) PRIMARY KEY, UserId NUMBER(10) NOT NULL, Note VARCHAR2(100) NOT NULL)";

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
            "SELECT CURRENT_TIMESTAMP FROM DUAL";

    /// <summary>
    /// 
    /// </summary>
    public override string StringAggregate(string tableName) =>
            $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public override string Upsert(string tableName, int id, string newName) => $@"
MERGE INTO {tableName} target
USING (SELECT {id} Id, '{newName}' Name FROM DUAL) source
ON (target.Id = source.Id)
WHEN MATCHED THEN UPDATE SET target.Name = source.Name
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
            $"SELECT {sequenceName}.NEXTVAL FROM DUAL";


    public override string StringAggregateOrdered(string tableName) =>
            $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    public override bool SupportsJsonScalarRead => true;

    public override string JsonScalarRead(string jsonLiteral) =>
            $"SELECT JSON_VALUE('{jsonLiteral}', '$.name') FROM DUAL";


    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string JsonPathRead(string jsonLiteral) =>
        $"SELECT JSON_VALUE('{jsonLiteral}', '$.user.name') FROM DUAL";
    public override string TemporalDateAdd() =>
        "SELECT TO_TIMESTAMP('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' DAY FROM DUAL";
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT_TIMESTAMP IS NOT NULL";
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM (SELECT Name FROM {tableName} ORDER BY CURRENT_TIMESTAMP, Name) WHERE ROWNUM = 1";

}
