namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a users table with optional seed rows for DML mutation workflows.
/// PT: Cria e remove uma tabela de usuarios com linhas iniciais opcionais para fluxos de mutacao DML.
/// </summary>
public sealed class UsersScenario<T>(
    ProviderSqlDialect dialect,
    params (int id, string name)[] seedRows
    ) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users table and seeds the configured rows.
    /// PT: Cria a tabela de usuarios e preenche as linhas configuradas.
    /// </summary>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";
        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));

        foreach (var (id, name) in seedRows)
        {
            service.ExecuteNonQuery(dialect.InsertUser(tableName, id, name));
        }
    }

    /// <summary>
    /// EN: Drops the users table created for the workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo.
    /// </summary>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];

        if (service.Dialect.Provider == ProviderId.Firebird
            && service.Connection.GetType().FullName == "FirebirdSql.Data.FirebirdClient.FbConnection")
        {
            service.Connection.Close();

            var cleanupConnection = (DbConnection?)Activator.CreateInstance(
                service.Connection.GetType(),
                service.Connection.ConnectionString);
            if (cleanupConnection is null)
                throw new InvalidOperationException("Unable to create a Firebird cleanup connection.");

            using (cleanupConnection)
            {
                cleanupConnection.Open();
                using var command = cleanupConnection.CreateCommand();
                command.CommandText = dialect.DropTable(users, uId);
                command.ExecuteNonQuery();
            }

            return;
        }

        service.ExecuteNonQuery(dialect.DropTable(users, uId));
    }
}
