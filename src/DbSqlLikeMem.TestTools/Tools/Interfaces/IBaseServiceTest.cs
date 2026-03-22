
namespace DbSqlLikeMem.TestTools;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public interface IBaseServiceTest
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract void RunTest(params object[] pars);
}

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public interface IBaseServiceWithReturnTest<T2>
{
    /// <summary>
    /// TODO: Add a summary for this class.
    /// </summary>
    public abstract T2 RunTest(params object[] pars);
}
