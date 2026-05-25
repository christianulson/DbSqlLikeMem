using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedTransactionUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunTransactionCommit()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunTransactionCommit()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunTransactionRollback()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunTransactionRollback()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRollbackToSavepoint()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunRollbackToSavepoint()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunNestedSavepointFlow()
        {
            var result = runner.RunTestAsync<UsersScenario, DmlMutationServiceTest>(
                (service, _) => Task.FromResult<object?>(service.RunNestedSavepointFlow()),
                Array.Empty<object>()).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

}
