using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedCrudUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public string RunUpdateByPk(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpdateByPkServiceTest>(userId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public int RunDeleteByPk(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationDeleteByPkServiceTest>(userId).GetAwaiter().GetResult();
            return ((List<List<object[]>>)result!).Count;
        }

        public int RunRowCountAfterUpdate()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationRowCountAfterUpdateServiceTest>(1).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunUpdateDeleteRoundTrip(int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpdateDeleteRoundTripServiceTest>(updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunTransactionalUpdateDeleteCommit(int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationTransactionalUpdateDeleteCommitServiceTest>(updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public string RunUpsert(int userId)
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationUpsertServiceTest>(userId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public void Dispose()
            => runner.Dispose();
    }

}
