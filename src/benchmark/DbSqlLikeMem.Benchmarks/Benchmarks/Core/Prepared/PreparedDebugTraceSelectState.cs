using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedDebugTraceSelectState(
        PreparedScenarioScope<UsersScenario, DebugTraceSelectServiceTest> scope) : IDisposable
    {
        public DebugTraceSelectServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
