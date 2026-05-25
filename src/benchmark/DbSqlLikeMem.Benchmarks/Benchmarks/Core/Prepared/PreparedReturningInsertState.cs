using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedReturningInsertState(
        PreparedScenarioScope<InsertUsersScenario, BatchInsertReturningServiceTest> scope) : IDisposable
    {
        public BatchInsertReturningServiceTest Service => scope.Service;

        public object? RunReturningInsert()
        {
            try
            {
                var value = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(value);
                return value;
            }
            finally
            {
                try
                {
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }

                try
                {
                    scope.Scenario.CreateScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore restore failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

}
