using System.Collections.Concurrent;

namespace DbSqlLikeMem.TestTools;

internal static class CrossProcessProviderGate
{
    private static readonly ConcurrentDictionary<string, Mutex> Mutexes = new(StringComparer.Ordinal);

    public static IDisposable? Acquire(ProviderSqlDialect dialect)
    {
        if (!ShouldSerialize(dialect.Provider))
            return null;

        var mutexName = $"DbSqlLikeMem.TestTools.{dialect.Provider}.ContainerGate";
        var mutex = Mutexes.GetOrAdd(mutexName, static name => new Mutex(false, name));

        try
        {
            mutex.WaitOne();
        }
        catch (AbandonedMutexException)
        {
            // The previous owner exited without releasing the gate. We still own it now.
        }

        return new Releaser(mutex);
    }

    private static bool ShouldSerialize(ProviderId provider)
        => provider is ProviderId.MariaDb or ProviderId.Oracle;

    private sealed class Releaser(Mutex mutex) : IDisposable
    {
        private bool disposed;

        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;
            mutex.ReleaseMutex();
        }
    }
}
