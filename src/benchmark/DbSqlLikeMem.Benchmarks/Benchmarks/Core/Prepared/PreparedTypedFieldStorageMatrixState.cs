using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedTypedFieldStorageMatrixState(
        NotFidelityTestService<DbConnection> runner,
        ProviderSqlDialect dialect) : IDisposable
    {
        private readonly ProviderSqlDialect _dialect = dialect;

        public QueryResultSnapshot RunTypedFieldStorageMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTypedFieldStorageMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTypedFieldFunctionMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTypedFieldFunctionMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTypedFieldCalculationMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTypedFieldCalculationMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTemporalFieldMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTemporalFieldMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTemporalComparisonMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTemporalComparisonMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTemporalArithmeticMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTemporalArithmeticMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunJsonTypedFieldMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunJsonTypedFieldMatrixAsync()).GetAwaiter().GetResult();

        public int RunParameterRoundTripMatrix()
        {
            var createdAt = _dialect.Provider == ProviderId.Npgsql
                ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
                : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
            return runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (service, args) => service.RunParameterRoundTripMatrixAsync(args),
                1,
                "Param Alice",
                DBNull.Value,
                true,
                (short)31,
                12.34m,
                createdAt,
                DBNull.Value,
                DBNull.Value).GetAwaiter().GetResult();
        }

        public int RunTypedFieldAndFunctionBlend()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (service, _) => service.RunTypedFieldAndFunctionBlendAsync()).GetAwaiter().GetResult();

        public int RunTypedFieldCompoundPredicateMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (service, _) => service.RunTypedFieldCompoundPredicateMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunCastCalculationMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunCastCalculationMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunNullComparisonMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunNullComparisonMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTextLengthMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTextLengthMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTextCaseMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTextCaseMatrixAsync()).GetAwaiter().GetResult();

        public QueryResultSnapshot RunTypedFieldPredicateMatrix()
            => runner.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (service, _) => service.RunTypedFieldPredicateMatrixAsync()).GetAwaiter().GetResult();

        public void Dispose()
            => runner.Dispose();
    }

}
