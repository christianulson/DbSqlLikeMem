using DbSqlLikeMem.TestTools.TemporaryTable;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedTemporaryTableSourceState(
        PreparedScenarioScope<TemporaryTableScenario, TemporaryTableServiceOpsTest> scope) : IDisposable
    {
        public TemporaryTableServiceOpsTest Service => scope.Service;

        public void Dispose()
            => scope.Dispose();
    }

}
