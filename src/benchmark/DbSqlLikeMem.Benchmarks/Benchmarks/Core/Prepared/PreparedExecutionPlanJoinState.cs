using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedExecutionPlanJoinState(
        PreparedScenarioScope<UsersOrdersScenario, ExecutionPlanJoinServiceTest> scope) : IDisposable
    {
        public ExecutionPlanJoinServiceTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
