namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class CreateTableScenario<T> : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
    { }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public virtual void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[0]));
    }
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class CreateTableServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="pars"></param>
    public virtual void RunTest(params object[] pars)
    {
        var sql = Dialect.CreateUsersTable((string)pars[0]);
        ExecuteNonQuery(sql);
    }
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class CreateTableWithFKScenario<T>
     : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.CreateUsersTable((string)pars[0]));
    }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[1]));
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[0]));
    }
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class CreateTableWithFKServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="pars"></param>
    public void RunTest(params object[] pars)
    {
        ExecuteNonQuery(Dialect.CreateOrdersTable((string)pars[1]));
    }
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class DropTableScenario<T>
    : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.CreateUsersTable((string)pars[0]));
    }

    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {
        
    }
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class DropTableServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    /// <param name="pars"></param>
    public void RunTest(params object[] pars)
    {
        var sql = Dialect.DropTable((string)pars[0]);
        ExecuteNonQuery(sql);
    }
}
