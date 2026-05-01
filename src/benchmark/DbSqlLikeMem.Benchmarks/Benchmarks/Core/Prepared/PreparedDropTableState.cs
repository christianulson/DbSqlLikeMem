using DbSqlLikeMem.TestTools.DDL;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedDropTableState(
        PreparedScenarioScope<DropTableScenario, DropTableServiceTest> scope) : IDisposable
    {
        public void RunDropTable()
        {
            try
            {
                var result = scope.Service.RunTestAsync().GetAwaiter().GetResult();
                GC.KeepAlive(result);
            }
            finally
            {
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
