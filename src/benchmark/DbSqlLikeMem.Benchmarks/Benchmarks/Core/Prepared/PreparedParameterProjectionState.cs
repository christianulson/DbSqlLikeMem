using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedParameterProjectionState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public string? RunParameterProjection()
        {
            return runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (service, _) => Task.FromResult(service.RunParameterProjection())).GetAwaiter().GetResult();
        }

        public void Dispose()
            => runner.Dispose();
    }

}
