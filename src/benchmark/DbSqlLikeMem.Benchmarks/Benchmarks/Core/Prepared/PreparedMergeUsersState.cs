using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedMergeUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public object? RunMergeInsertThenUpdate()
            => runner.RunTestAsync<UsersScenario, DmlMutationMergeInsertThenUpdateServiceTest>().GetAwaiter().GetResult();

        public object? RunUpsertInsertThenUpdate()
            => runner.RunTestAsync<UsersScenario, DmlMutationUpsertInsertThenUpdateServiceTest>().GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
