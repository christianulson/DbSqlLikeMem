using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedSequenceUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public object? RunSequenceInsertRoundTrip()
            => runner.RunTestAsync<SequenceScenario, UsersScenario, DmlMutationSequenceInsertRoundTripServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceInsertExpression()
            => runner.RunTestAsync<SequenceScenario, UsersScenario, DmlMutationSequenceInsertExpressionServiceTest>().GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
