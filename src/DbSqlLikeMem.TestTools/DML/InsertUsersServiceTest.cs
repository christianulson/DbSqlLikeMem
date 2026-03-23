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
    /// <param name="pars">EN: The users table name, scenario token, row count, optional starting id, and optional expected total count. PT: O nome da tabela de usuarios, o token do cenario, a contagem de linhas, o id inicial opcional e a contagem total esperada opcional.</param>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public int RunTest(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var rowCount = (int)pars[2];
        var startId = pars.Length > 3 ? (int)pars[3] : 1;
        var expectedCount = pars.Length > 4 ? (int)pars[4] : rowCount;
        var tableName = BuildScenarioTableName(users, uId);
        InsertSequentialRows(tableName, rowCount, startId);

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != expectedCount)
        {
            throw new InvalidOperationException($"Expected {expectedCount} rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts the requested number of user rows in parallel and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario em paralelo e valida a contagem final.
    /// </summary>
    /// <param name="pars">EN: The users table name, scenario token, row count, optional starting id, and optional expected total count. PT: O nome da tabela de usuarios, o token do cenario, a contagem de linhas, o id inicial opcional e a contagem total esperada opcional.</param>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public int RunParallelTest(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var rowCount = (int)pars[2];
        var startId = pars.Length > 3 ? (int)pars[3] : 1;
        var expectedCount = pars.Length > 4 ? (int)pars[4] : rowCount;
        var tableName = BuildScenarioTableName(users, uId);
        var factory = connectionFactory ?? throw new InvalidOperationException($"Parallel insert workflows require a connection factory for {Dialect.DisplayName}.");

        var tasks = Enumerable.Range(0, rowCount)
            .Select(offset => Task.Run(() =>
            {
                var id = startId + offset;
                using var parallelConnection = factory();
                parallelConnection.Open();
                ExecuteNonQueryOnConnection(parallelConnection, Dialect.InsertUser(tableName, id, $"User-{id}"));
            }))
            .ToArray();

        Task.WhenAll(tasks).GetAwaiter().GetResult();

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(tableName)), CultureInfo.InvariantCulture);
        if (count != expectedCount)
        {
            throw new InvalidOperationException($"Expected {expectedCount} rows for {Dialect.DisplayName}, got {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Inserts a single user row and returns the affected-row count.
    /// PT: Insere uma linha de usuario e retorna a contagem de linhas afetadas.
    /// </summary>
    /// <param name="pars">EN: The users table name, scenario token, and optional insert id. PT: O nome da tabela de usuarios, o token do cenario e o id de insert opcional.</param>
    /// <returns>EN: The affected-row count reported by the provider. PT: A contagem de linhas afetadas informada pelo provedor.</returns>
    public int RunRowCountAfterInsert(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var id = pars.Length > 2 ? (int)pars[2] : 1;
        var tableName = BuildScenarioTableName(users, uId);
        var affected = ExecuteNonQuery(Dialect.InsertUser(tableName, id, "Alice"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected insert rowcount for {Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }

    private void InsertSequentialRows(string users, int rowCount, int startId)
    {
        for (var i = 0; i < rowCount; i++)
        {
            var id = startId + i;
            var name = rowCount == 1 ? "Alice" : $"User-{id}";
            ExecuteNonQuery(Dialect.InsertUser(users, id, name));
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
