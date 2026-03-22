namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class SelectTableScenario<T>(
    ProviderSqlDialect dialect
    ) : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        service.ExecuteNonQuery(dialect.CreateUsersTable(users));
        service.ExecuteNonQuery(dialect.InsertUser(users, 1, "Alice"));
    }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        service.ExecuteNonQuery(dialect.DropTable((string)pars[0]));
    }
}

/// <summary>
/// TODO: Add a summary for this class.
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
    /// TODO: Add a summary for this class.
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
