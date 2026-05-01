using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedSequenceExpressionFilterState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public long[] RunSequenceExpressionFilter()
            => runner.RunTestAsync<SequenceScenario, UsersScenario, SequenceExpressionFilterServiceTest, long[]>(
                (service, args) => service.RunSequenceExpressionFilterAsync(args)).GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
