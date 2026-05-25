using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedBatchUsersState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public string RunBatchMixedReadWrite(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchMixedReadWriteServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public string RunBatchScalar(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchScalarServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public int RunBatchNonQuery(int firstUserId, int secondUserId, int updateUserId, int deleteUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchNonQueryServiceTest>(firstUserId, secondUserId, updateUserId, deleteUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public object? RunBatchReaderMultiResult(int firstUserId, int secondUserId)
            => runner.RunTestAsync<InsertUsersScenario, BatchReaderMultiResultServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();

        public int RunBatchInsert(int rowCount)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchInsertServiceTest>(rowCount).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public int RunRowCountInBatch(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchRowCountInServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public string RunBatchTransactionControl(int firstUserId, int secondUserId)
        {
            var result = runner.RunTestAsync<InsertUsersScenario, BatchTransactionControlServiceTest>(firstUserId, secondUserId).GetAwaiter().GetResult();
            return Convert.ToString(result, CultureInfo.InvariantCulture)!;
        }

        public void Dispose()
            => runner.Dispose();
    }

}
