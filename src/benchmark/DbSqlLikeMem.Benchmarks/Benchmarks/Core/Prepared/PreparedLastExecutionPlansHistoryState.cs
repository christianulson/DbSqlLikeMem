using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedLastExecutionPlansHistoryState(
        PreparedScenarioScope<UsersScenario, LastExecutionPlansHistoryServiceTest> scope) : IDisposable
    {
        public LastExecutionPlansHistoryServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
