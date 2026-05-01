using System.Globalization;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedStoredProcedureState(
        NotFidelityTestService<DbConnection> runner) : IDisposable
    {
        public int RunStoredProcedureCall(int tenantId, string note)
        {
            var result = runner.RunTestAsync<NoopScenario, StoredProcedureBenchmarkServiceTest>(tenantId, note).GetAwaiter().GetResult();
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }

        public void Dispose()
            => runner.Dispose();
    }

}
