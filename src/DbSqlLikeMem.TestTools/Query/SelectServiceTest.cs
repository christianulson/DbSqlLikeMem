namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Creates the users table and seed row required by the primary-key select scenario.
/// PT: Cria a tabela de usuarios e a linha inicial exigidas pelo cenario de selecao por chave primaria.
/// </summary>
public class SelectTableScenario<T>(
    ProviderSqlDialect dialect
    ) : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users table and inserts the seed row used by the select scenario.
    /// PT: Cria a tabela de usuarios e insere a linha base usada pelo cenario de select.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));
        service.ExecuteNonQuery(dialect.InsertUser(users, 1, "Alice"));
    }

    /// <summary>
    /// EN: Drops the users table created for the select scenario.
    /// PT: Remove a tabela de usuarios criada para o cenario de select.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        service.ExecuteNonQuery(dialect.DropTable((string)pars[0], (string)pars[1]));
    }
}

/// <summary>
/// EN: Executes the primary-key select command for the shared query scenario.
/// PT: Executa o comando de selecao por chave primaria para o cenario de consulta compartilhado.
/// </summary>
public class SelectByPKServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceWithReturnTest<string>
    where T : DbConnection
{
    /// <summary>
    /// EN: Reads the seeded row by primary key and validates the returned value.
    /// PT: Lê a linha inserida pela chave primaria e valida o valor retornado.
    /// </summary>
    /// <param name="pars"></param>
    public string RunTest(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)));
        if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected select result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        return value!;
    }
}
