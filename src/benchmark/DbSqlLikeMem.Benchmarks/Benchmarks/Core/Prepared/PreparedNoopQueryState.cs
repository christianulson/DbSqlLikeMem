using DbSqlLikeMem.TestTools.Query;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedNoopQueryState(
        DbConnection connection,
        QueryServiceTest service) : IDisposable
    {
        public QueryServiceTest Service => service;

        public void Dispose()
            => connection.Dispose();
    }

}
