namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Describes shared connection and SQL execution helpers for scenario-based tests.
/// PT: Descreve helpers compartilhados de conexao e execucao SQL para testes baseados em cenarios.
/// </summary>
public abstract class BaseServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect)
    where T : DbConnection
{
    /// <summary>
    /// EN: Gets the connection used by the current scenario.
    /// PT: Obtem a conexao usada pelo cenario atual.
    /// </summary>
    public T Connection => connection;

    /// <summary>
    /// EN: Gets the dialect used to build provider-specific SQL.
    /// PT: Obtem o dialeto usado para montar SQL especifica do provedor.
    /// </summary>
    public ProviderSqlDialect Dialect => dialect;

    /// <summary>
    /// EN: Creates the scenario data using the configured scenario object.
    /// PT: Cria os dados do cenario usando o objeto de cenario configurado.
    /// </summary>
    public virtual void CreateScenario(params object[] pars) {
        testScenario.CreateScenario(this, pars);
    }

    /// <summary>
    /// EN: Removes the scenario data using the configured scenario object.
    /// PT: Remove os dados do cenario usando o objeto de cenario configurado.
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
