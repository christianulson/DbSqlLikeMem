namespace DbSqlLikeMem.TestTools;

/// <summary>
/// TODO: Add a summary for this method.
/// </summary>
public interface ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// TODO: Add a summary for this method.
    /// </summary>
    /// <param name="service">TODO:</param>
    /// <param name="pars">TODO:</param>
    void CreateSenario(
        BaseServiceTest<T> service,
        params object[] pars);

    /// <summary>
    /// TODO: Add a summary for this method.
    /// </summary>
    /// <param name="service">TODO:</param>
    /// <param name="pars">TODO:</param>
    void DropScenario(
        BaseServiceTest<T> service, 
        params object[] pars);
}
