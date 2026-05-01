using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedParameterMatrixState(
        NotFidelityTestService<DbConnection> runner,
        ProviderSqlDialect dialect) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public int RunParameterTypeMatrix()
        {
            var createdAt = _dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            return runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (service, args) => service.RunParameterTypeMatrixAsync(args),
                "Typed param",
                "Ansi param",
                "Fixed ANSI",
                "Fixed Text",
                (short)12,
                34,
                56L,
                true,
                78.90m,
                12.5d,
                TimeSpan.FromHours(1.5),
                new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                createdAt,
                Guid.Parse("11111111-2222-3333-4444-555555555555"),
                new byte[] { 1, 2, 3, 4 }).GetAwaiter().GetResult();
        }

        public int RunParameterDateCurrencyMatrix()
        {
            return runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (service, args) => service.RunParameterDateCurrencyMatrixAsync(args),
                new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified),
                123.45m).GetAwaiter().GetResult();
        }

        public void Dispose()
            => runner.Dispose();
    }

}
