namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertUsersServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect,
    Func<T>? connectionFactory = null
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceWithReturnTest<int>
    where T : DbConnection
{
    /// <summary>
    /// EN: Inserts the requested number of user rows and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario e valida a contagem final.
    /// </summary>
    /// <param name="pars">EN: The users table name, scenario token and row count. PT: O nome da tabela de usuarios, o token do cenario e a contagem de linhas.</param>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public int RunTest(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var rowCount = (int)pars[2];
        var tableName = BuildScenarioTableName(users, uId);
        InsertSequentialRows(tableName, rowCount);

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != rowCount)
        {
            throw new InvalidOperationException($"Expected {rowCount} rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts the requested number of user rows in parallel and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario em paralelo e valida a contagem final.
    /// </summary>
    /// <param name="pars">EN: The users table name, scenario token and row count. PT: O nome da tabela de usuarios, o token do cenario e a contagem de linhas.</param>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public int RunParallelTest(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var rowCount = (int)pars[2];
        var tableName = BuildScenarioTableName(users, uId);
        var factory = connectionFactory ?? throw new InvalidOperationException($"Parallel insert workflows require a connection factory for {Dialect.DisplayName}.");

        var tasks = Enumerable.Range(1, rowCount)
            .Select(i => Task.Run(() =>
            {
                using var parallelConnection = factory();
                parallelConnection.Open();
                ExecuteNonQueryOnConnection(parallelConnection, Dialect.InsertUser(tableName, i, $"User-{i}"));
            }))
            .ToArray();

        Task.WhenAll(tasks).GetAwaiter().GetResult();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != rowCount)
        {
            throw new InvalidOperationException($"Expected {rowCount} rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts a single user row and returns the affected-row count.
    /// PT: Insere uma linha de usuario e retorna a contagem de linhas afetadas.
    /// </summary>
    /// <param name="pars">EN: The users table name and scenario token. PT: O nome da tabela de usuarios e o token do cenario.</param>
    /// <returns>EN: The affected-row count reported by the provider. PT: A contagem de linhas afetadas informada pelo provedor.</returns>
    public int RunRowCountAfterInsert(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = BuildScenarioTableName(users, uId);
        var affected = ExecuteNonQuery(Dialect.InsertUser(tableName, 1, "Alice"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected insert rowcount for {Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }

    private void InsertSequentialRows(string users, int rowCount)
    {
        for (var i = 1; i <= rowCount; i++)
        {
            var name = rowCount == 1 ? "Alice" : $"User-{i}";
            ExecuteNonQuery(Dialect.InsertUser(users, i, name));
        }
    }

    private static int ExecuteNonQueryOnConnection(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteNonQuery();
    }

    private static object? ExecuteScalarOnConnection(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteScalar();
    }

    private new object? ExecuteScalar(string sql, DbTransaction? transaction = null)
        => ExecuteScalarOnConnection(Connection, sql, transaction);

    private static string BuildScenarioTableName(string users, string uId)
        => $"{users}_{uId}";
}
