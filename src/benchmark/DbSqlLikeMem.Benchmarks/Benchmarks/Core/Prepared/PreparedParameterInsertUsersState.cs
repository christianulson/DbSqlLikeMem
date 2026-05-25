using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedParameterInsertUsersState(NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunParameterInsertRoundTrip()
        {
            var createdAt1 = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            var createdAt2 = new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified);
            var updatedAt1 = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Unspecified);
            var updatedAt2 = new DateTime(2024, 4, 5, 6, 7, 8, DateTimeKind.Unspecified);

            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterInsertRoundTripServiceTest>(
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                true,
                false,
                (short)31,
                (short)22,
                123.45m,
                67.89m,
                createdAt1,
                createdAt2,
                updatedAt1,
                updatedAt2,
                "{\"theme\":\"dark\"}",
                "{\"theme\":\"light\"}").GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunParameterInsertNullRoundTrip()
        {
            var createdAt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

            var result = runner.RunTestAsync<InsertUsersScenario, DmlMutationParameterInsertNullRoundTripServiceTest>(
                "Alice-v2",
                null!,
                true,
                (short)31,
                123.45m,
                createdAt,
                null!,
                null!).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

}
