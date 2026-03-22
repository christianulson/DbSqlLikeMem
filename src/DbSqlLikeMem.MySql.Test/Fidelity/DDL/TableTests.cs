
using DbSqlLikeMem.TestTools.Tests.DDL;

namespace DbSqlLikeMem.MySql.Test.Fidelity.DDL;

/// <summary>
/// TODO: Add a summary for this class.
/// </summary>
public class TableTests(
    ITestOutputHelper helper
    ) : TableTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new TestTools.MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
}
