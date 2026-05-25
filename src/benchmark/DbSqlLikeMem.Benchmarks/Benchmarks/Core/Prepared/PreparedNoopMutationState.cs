using DbSqlLikeMem.TestTools.DML;
namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedNoopMutationState(
        DbConnection connection,
        DmlMutationServiceTest service) : IDisposable
    {
        public DmlMutationServiceTest Service => service;

        public void Dispose()
            => connection.Dispose();
    }

}
