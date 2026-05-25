using System.Collections.Concurrent;
using System.Threading;

namespace DbSqlLikeMem.TestTools;

internal static class CrossProcessProviderGate
{
    private static readonly ConcurrentDictionary<string, Semaphore> Semaphores = new(StringComparer.Ordinal);

    public static IDisposable? Acquire(ProviderSqlDialect dialect)
    {
        if (!ShouldSerialize(dialect.Provider))
            return null;

        var gateName = $"DbSqlLikeMem.TestTools.{dialect.Provider}.ContainerGate";
        var semaphore = Semaphores.GetOrAdd(gateName, static name => new Semaphore(1, 1, name));

        semaphore.WaitOne();

        return new Releaser(semaphore);
    }

    private static bool ShouldSerialize(ProviderId provider)
        => provider is ProviderId.Firebird or ProviderId.MariaDb or ProviderId.Oracle;

    private sealed class Releaser(Semaphore semaphore) : IDisposable
    {
        private bool disposed;

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            semaphore.Release();
        }
    }
}
