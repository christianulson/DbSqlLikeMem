using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new TestTools.MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
