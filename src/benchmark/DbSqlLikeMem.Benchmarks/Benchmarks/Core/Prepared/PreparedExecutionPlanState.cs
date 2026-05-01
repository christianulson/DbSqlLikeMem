using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedExecutionPlanState(
        PreparedScenarioScope<UsersScenario, ExecutionPlanSelectServiceTest> scope) : IDisposable
    {
        public ExecutionPlanSelectServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
