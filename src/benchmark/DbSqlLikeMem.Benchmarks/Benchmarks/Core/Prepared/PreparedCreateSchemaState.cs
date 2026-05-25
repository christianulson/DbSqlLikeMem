using DbSqlLikeMem.TestTools.DDL;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedCreateSchemaState(
        PreparedScenarioScope<CreateTableScenario, CreateTableServiceTest> scope) : IDisposable
    {
        public void RunCreateSchema()
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
                    scope.Scenario.DropScenarioAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore cleanup failures during benchmark teardown.
                }
            }
        }

        public void Dispose()
            => scope.Dispose();
    }

}
