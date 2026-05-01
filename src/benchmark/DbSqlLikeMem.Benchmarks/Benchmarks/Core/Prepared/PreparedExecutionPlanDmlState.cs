using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedExecutionPlanDmlState(
        PreparedScenarioScope<InsertUsersScenario, ExecutionPlanDmlServiceTest> scope) : IDisposable
    {
        private int _nextInsertId = 1;

        public object? RunExecutionPlanDml()
        {
            var value = scope.Service.RunTestAsync(_nextInsertId++).GetAwaiter().GetResult();
            return value;
        }

        public void Dispose()
            => scope.Dispose();
    }

}
