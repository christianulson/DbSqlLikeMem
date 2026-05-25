using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedParameterTransactionUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunParameterTransactionCommit()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionCommitServiceTest>(
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterTransactionRollback()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionRollbackServiceTest>(
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified)).GetAwaiter().GetResult();
            var count2 = (int)(result!.GetType().GetProperty("count2")?.GetValue(result)
                ?? throw new InvalidOperationException("Parameter transaction rollback did not return a count2 value."));
            return count2;
        }

        public void Dispose()
            => runner.Dispose();
    }

}
