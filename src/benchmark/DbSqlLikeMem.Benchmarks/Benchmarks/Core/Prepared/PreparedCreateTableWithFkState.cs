using System.Globalization;
using DbSqlLikeMem.TestTools.DDL;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedCreateTableWithFkState(
        PreparedScenarioScope<CreateTableWithFKScenario, CreateTableWithFKServiceTest> scope) : IDisposable
    {
        public void RunCreateTableWithFk()
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

        public int RunCreateTableWithFkInsert(int userId, int orderId)
        {
            try
            {
                var result = scope.Service.RunTestAsync(userId, orderId).GetAwaiter().GetResult();
                GC.KeepAlive(result);
                return Convert.ToInt32(result, CultureInfo.InvariantCulture);
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
