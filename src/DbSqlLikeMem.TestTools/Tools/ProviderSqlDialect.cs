namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides provider-specific SQL snippets used by the benchmark session workflows.
/// PT-br: Fornece trechos SQL espec�ficos do provedor usados pelos fluxos das sess�es de benchmark.
/// </summary>
public abstract class ProviderSqlDialect
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract ProviderId Provider { get; }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string DisplayName { get; }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual bool SupportsUpsert => false;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual bool SupportsSequence => false;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual bool SupportsStringAggregate => true;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual bool SupportsSavepoints => true;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual bool SupportsJsonScalarRead => false;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string CreateUsersTable(string tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string CreateOrdersTable(string tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string InsertUser(string tableName, int id, string name);

    /// <summary>
    /// EN: Returns the INSERT statement used by the returning benchmark when the provider supports returning rows.
    /// PT-br: Retorna a instrução INSERT usada pelo benchmark de returning quando o provider suporta linhas retornadas.
    /// </summary>
    public virtual string InsertUserReturning(string tableName, int id, string name) => InsertUser(tableName, id, name);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string InsertUsers(string tableName, params (int id, string name)[] values);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string InsertOrder(string tableName, int id, int userId, string note);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string SelectUserNameById(string tableName, int id);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string CountJoinForUser(string usersTable, string ordersTable, int userId);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string UpdateUserNameById(string tableName, int id, string newName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string DeleteUserById(string tableName, int id);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string CountRows(string tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string DateScalar();

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract string StringAggregate(string tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string StringAggregateOrdered(string tableName) => StringAggregate(tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string StringAggregateDistinct(string tableName) => StringAggregateOrdered(tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string StringAggregateCustomSeparator(string tableName, string separator) => StringAggregateOrdered(tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string StringAggregateLargeGroup(string tableName) => StringAggregateOrdered(tableName);

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string Upsert(string tableName, int id, string newName) =>
        throw new NotSupportedException($"{DisplayName} does not support the configured upsert benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string CreateSequence(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string NextSequenceValue(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string Savepoint(string savepointName) => $"SAVEPOINT {savepointName}";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string RollbackToSavepoint(string savepointName) => $"ROLLBACK TO SAVEPOINT {savepointName}";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string ReleaseSavepoint(string savepointName) => $"RELEASE SAVEPOINT {savepointName}";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string JsonScalarRead(string jsonLiteral) =>
        throw new NotSupportedException($"{DisplayName} does not support the JSON scalar benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string JsonPathRead(string jsonLiteral) =>
        throw new NotSupportedException($"{DisplayName} does not support the JSON path benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string TemporalCurrentTimestamp() => DateScalar();

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string TemporalDateAdd() => throw new NotSupportedException($"{DisplayName} does not support the temporal DATEADD benchmark.");

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string TemporalNowWhere(string tableName) => $"SELECT COUNT(*) FROM {tableName} WHERE 1 = 1";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string TemporalNowOrderBy(string tableName) => $"SELECT Name FROM {tableName} ORDER BY Name";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string CteSimple(string tableName) => $"WITH cte AS (SELECT Name FROM {tableName} WHERE Id = 1) SELECT COUNT(*) FROM cte";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string WindowRowNumber(string tableName) => $"SELECT MAX(rn) FROM (SELECT ROW_NUMBER() OVER (ORDER BY Name) AS rn FROM {tableName}) q";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string WindowLag(string tableName) => $"SELECT COUNT(*) FROM (SELECT LAG(Name) OVER (ORDER BY Id) AS PrevName FROM {tableName}) q";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string SelectExistsPredicate(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE EXISTS (SELECT 1 FROM {ordersTable} o WHERE o.UserId = u.Id)";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string SelectCorrelatedCount(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE (SELECT COUNT(*) FROM {ordersTable} o WHERE o.UserId = u.Id) > 0";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string GroupByHaving(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM (SELECT u.Id FROM {usersTable} u INNER JOIN {ordersTable} o ON o.UserId = u.Id GROUP BY u.Id HAVING COUNT(*) >= 2) q";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string UnionAllProjection(string tableName) =>
        $"SELECT COUNT(*) FROM (SELECT Name FROM {tableName} WHERE Id = 1 UNION ALL SELECT Name FROM {tableName} WHERE Id = 2) q";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string DistinctProjection(string tableName) =>
        $"SELECT COUNT(*) FROM (SELECT DISTINCT Name FROM {tableName}) q";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string MultiJoinAggregate(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u INNER JOIN {ordersTable} o1 ON o1.UserId = u.Id INNER JOIN {ordersTable} o2 ON o2.UserId = u.Id WHERE u.Id = 1";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string SelectScalarSubquery(string usersTable, string ordersTable) =>
        $"SELECT (SELECT COUNT(*) FROM {ordersTable} o WHERE o.UserId = 1)";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string SelectInSubquery(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} WHERE Id IN (SELECT UserId FROM {ordersTable})";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string CrossApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u WHERE EXISTS (SELECT 1 FROM {ordersTable} o WHERE o.UserId = u.Id)";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string OuterApplyProjection(string usersTable, string ordersTable) =>
        $"SELECT COUNT(*) FROM {usersTable} u";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string DropTable(string tableName) => $"DROP TABLE {tableName}";

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual string DropSequence(string sequenceName) => $"DROP SEQUENCE {sequenceName}";
}

