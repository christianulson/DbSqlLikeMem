namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
public abstract class ProviderSqlDialect
{
    /// <summary>
    /// 
    /// </summary>
    public abstract BenchmarkProviderId Provider { get; }

    /// <summary>
    /// 
    /// </summary>
    public abstract string DisplayName { get; }

    /// <summary>
    /// 
    /// </summary>
    public virtual bool SupportsUpsert => false;

    /// <summary>
    /// 
    /// </summary>
    public virtual bool SupportsSequence => false;

    /// <summary>
    /// 
    /// </summary>
    public virtual bool SupportsStringAggregate => true;

    /// <summary>
    /// 
    /// </summary>
    public abstract string CreateUsersTable(string tableName);

    /// <summary>
    /// 
    /// </summary>
    public abstract string CreateOrdersTable(string tableName);

    /// <summary>
    /// 
    /// </summary>
    public abstract string InsertUser(string tableName, int id, string name);

    /// <summary>
    /// 
    /// </summary>
    public abstract string InsertOrder(string tableName, int id, int userId, string note);

    /// <summary>
    /// 
    /// </summary>
    public abstract string SelectUserNameById(string tableName, int id);

    /// <summary>
    /// 
    /// </summary>
    public abstract string CountJoinForUser(string usersTable, string ordersTable, int userId);

    /// <summary>
    /// 
    /// </summary>
    public abstract string UpdateUserNameById(string tableName, int id, string newName);

    /// <summary>
    /// 
    /// </summary>
    public abstract string DeleteUserById(string tableName, int id);

    /// <summary>
    /// 
    /// </summary>
    public abstract string CountRows(string tableName);

    /// <summary>
    /// 
    /// </summary>
    public abstract string DateScalar();

    /// <summary>
    /// 
    /// </summary>
    public abstract string StringAggregate(string tableName);

    /// <summary>
    /// 
    /// </summary>
    public virtual string Upsert(string tableName, int id, string newName) =>
        throw new NotSupportedException($"{DisplayName} does not support the configured upsert benchmark.");

    /// <summary>
    /// 
    /// </summary>
    public virtual string CreateSequence(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// 
    /// </summary>
    public virtual string NextSequenceValue(string sequenceName) =>
        throw new NotSupportedException($"{DisplayName} does not support sequences in this benchmark.");

    /// <summary>
    /// 
    /// </summary>
    public virtual string DropTable(string tableName) => $"DROP TABLE {tableName}";

    /// <summary>
    /// 
    /// </summary>
    public virtual string DropSequence(string sequenceName) => $"DROP SEQUENCE {sequenceName}";
}
