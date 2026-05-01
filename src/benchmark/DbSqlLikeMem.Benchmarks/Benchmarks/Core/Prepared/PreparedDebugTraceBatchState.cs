using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedDebugTraceBatchState(
        PreparedScenarioScope<InsertUsersScenario, DebugTraceBatchServiceTest> scope) : IDisposable
    {
        public DebugTraceBatchServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
