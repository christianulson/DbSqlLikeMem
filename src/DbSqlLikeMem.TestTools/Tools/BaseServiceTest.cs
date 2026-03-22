namespace DbSqlLikeMem.TestTools;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public abstract class BaseServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect)
    where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public T Connection => connection;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public ProviderSqlDialect Dialect => dialect;

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual void CreateScenario(params object[] pars) {
        testScenario.CreateSenario(this, pars);
    }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public virtual void DropScenario(params object[] pars) {
        testScenario.DropScenario(this, pars);
    }

    /// <summary>
    /// EN: Executes a SQL command that does not return a result set.
    /// PT-br: Executa um comando SQL que não retorna um conjunto de resultados.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    internal int ExecuteNonQuery(
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
            command.Transaction = transaction;
        
        return command.ExecuteNonQuery();
    }

    /// <summary>
    /// EN: Executes a SQL command that does not return a result set.
    /// PT-br: Executa um comando SQL que não retorna um conjunto de resultados.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    internal async Task<int> ExecuteNonQueryAsync(
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
            command.Transaction = transaction;
        return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// EN: Executes a SQL command and returns its scalar result.
    /// PT-br: Executa um comando SQL e retorna o seu resultado escalar.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    /// <returns>EN: The scalar value returned by the SQL command. PT-br: O valor escalar retornado pelo comando SQL.</returns>
    internal object? ExecuteScalar(
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
            command.Transaction = transaction;
        return command.ExecuteScalar();
    }

    /// <summary>
    /// EN: Executes a SQL command and returns its scalar result.
    /// PT-br: Executa um comando SQL e retorna o seu resultado escalar.
    /// </summary>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    /// <param name="transaction">EN: The optional transaction associated with the command execution. PT-br: A transação opcional associada à execução do comando.</param>
    /// <returns>EN: The scalar value returned by the SQL command. PT-br: O valor escalar retornado pelo comando SQL.</returns>
    internal async Task<object?> ExecuteScalarAsync(
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
            command.Transaction = transaction;
        return await command.ExecuteScalarAsync().ConfigureAwait(false);
    }
}
