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
    /// EN: Returns the DB2 scalar-subquery benchmark SQL with the required dummy table reference.
    /// PT: Retorna a SQL do benchmark de subconsulta escalar para DB2 com a referencia obrigatoria de tabela dummy.
    /// </summary>
    public override string SelectScalarSubquery(string usersTable, string ordersTable) =>
        $"SELECT (SELECT COUNT(*) FROM {ordersTable} o WHERE o.UserId = 1) FROM SYSIBM.SYSDUMMY1";

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
MERGE INTO {tableName} target
USING (VALUES ({id}, '{newName}')) source (Id, Name)
ON target.Id = source.Id
WHEN MATCHED THEN
    UPDATE SET Name = source.Name
WHEN NOT MATCHED THEN
    INSERT (Id, Name) VALUES (source.Id, source.Name)";

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

    public override string Savepoint(string savepointName) => $"SAVEPOINT {savepointName} ON ROLLBACK RETAIN CURSORS";

    public override string StringAggregateOrdered(string tableName) =>
            $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";

    public override bool SupportsJsonScalarRead => true;

    public override string JsonScalarRead(string jsonLiteral) =>
            $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.name')";

    public override string StringAggregateDistinct(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM (SELECT DISTINCT Name FROM {tableName}) t";
    public override string StringAggregateCustomSeparator(string tableName, string separator) =>
        $"SELECT LISTAGG(Name, '{separator}') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string StringAggregateLargeGroup(string tableName) =>
        $"SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM {tableName}";
    public override string JsonPathRead(string jsonLiteral) =>
        $"VALUES JSON_VALUE('{jsonLiteral}', 'strict $.user.name')";
    public override string TemporalDateAdd() =>
        "VALUES TIMESTAMP('2024-01-01-00.00.00') + 1 DAY";
    public override string TemporalNowWhere(string tableName) =>
        $"SELECT COUNT(*) FROM {tableName} WHERE CURRENT TIMESTAMP IS NOT NULL";
    public override string TemporalNowOrderBy(string tableName) =>
        $"SELECT Name FROM {tableName} ORDER BY CURRENT TIMESTAMP, Name FETCH FIRST 1 ROW ONLY";

    public override string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";

    public override string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u LEFT JOIN LATERAL (SELECT o.Note FROM {ordersTable} o WHERE o.UserId = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x ON 1 = 1";

}
