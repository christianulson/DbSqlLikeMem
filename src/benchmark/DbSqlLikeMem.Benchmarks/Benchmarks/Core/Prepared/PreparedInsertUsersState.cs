using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedInsertUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunSequentialInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertUsersServiceTest>(rowCount, 1, rowCount).GetAwaiter().GetResult();
            return ((List<List<object[]>>)result!)[0].Count;
        }

        public int RunParallelInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertParallelUsersServiceTest>(rowCount, 1, rowCount).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRowCountAfterInsert()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertRowCountUsersServiceTest>(1).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterInsertSingle()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertParameterUsersServiceTest>(1, "User 1").GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public (string firstName, string lastName) RunInsertCustomStartId()
        {
            var result = runner.RunTestAsync<InsertUsersScenario, InsertCustomStartUsersServiceTest>(10).GetAwaiter().GetResult();
            return ((string firstName, string lastName))result!;
        }

        public object? RunInsertDefaultColumns()
            => runner.RunTestAsync<InsertUsersScenario, InsertDefaultsUsersServiceTest>(1, "Alice").GetAwaiter().GetResult();

        public object? RunInsertNullableColumns()
            => runner.RunTestAsync<InsertNullabilityScenario, InsertNullableColumnsServiceTest>(1, 10).GetAwaiter().GetResult();

        public object? RunInsertNotNullWithoutDefault()
            => runner.RunTestAsync<InsertNullabilityScenario, InsertNotNullWithoutDefaultServiceTest>(2).GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
