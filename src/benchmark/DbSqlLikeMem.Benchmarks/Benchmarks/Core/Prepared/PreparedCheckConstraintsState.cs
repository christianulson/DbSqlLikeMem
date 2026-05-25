using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedCheckConstraintsState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public object? RunCheckConstraintsValidInsert()
            => runner.RunTestAsync<CheckConstraintsScenario, CheckConstraintsValidInsertServiceTest>(1, 10, 5).GetAwaiter().GetResult();

        public object? RunCheckConstraintsInvalidInsert()
            => runner.RunTestAsync<CheckConstraintsScenario, CheckConstraintsInvalidInsertServiceTest>(2, 10).GetAwaiter().GetResult();

        public object? RunCheckConstraintsInvalidUpdate()
            => runner.RunTestAsync<CheckConstraintsScenario, CheckConstraintsInvalidUpdateServiceTest>(3, 10, 5).GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
