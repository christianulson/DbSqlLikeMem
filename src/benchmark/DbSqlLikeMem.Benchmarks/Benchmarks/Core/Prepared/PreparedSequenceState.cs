using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedSequenceState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public object? RunSequenceNextValue()
            => runner.RunTestAsync<SequenceScenario, DmlMutationSequenceServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceCurrentValue()
            => runner.RunTestAsync<SequenceScenario, DmlMutationSequenceCurrentValueServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceSelectProjection()
            => runner.RunTestAsync<SequenceScenario, DmlMutationSequenceSelectProjectionServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceCaseWhereMatrix()
            => runner.RunTestAsync<SequenceScenario, DmlMutationSequenceCaseWhereMatrixServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceTemporalMatrix()
            => runner.RunTestAsync<SequenceScenario, DmlMutationSequenceTemporalMatrixServiceTest>().GetAwaiter().GetResult();

        public object? RunSequenceJoinAggregate()
            => runner.RunTestAsync<SequenceScenario, UsersOrdersScenario, DmlMutationSequenceJoinAggregateServiceTest>().GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
