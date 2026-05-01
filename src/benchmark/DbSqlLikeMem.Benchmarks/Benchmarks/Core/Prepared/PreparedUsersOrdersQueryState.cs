using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedUsersOrdersQueryState(
        PreparedScenarioScope<UsersOrdersScenario, QueryServiceTest> scope) : IDisposable
    {
        public QueryServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
