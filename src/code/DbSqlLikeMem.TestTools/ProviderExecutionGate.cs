using System.Collections.Concurrent;

namespace DbSqlLikeMem.TestTools;

internal static class ProviderExecutionGate
{
    private const string FirebirdGateName = @"DbSqlLikeMem.Firebird.RealConnection";
    private static readonly ConcurrentDictionary<string, Mutex> MutexCache = [];

    public static IDisposable? Acquire(
        DbConnection connection,
        ProviderId provider)
    {
        if (provider != ProviderId.Firebird)
            return null;

        if (!string.Equals(
                connection.GetType().FullName,
                "FirebirdSql.Data.FirebirdClient.FbConnection",
                StringComparison.Ordinal))
        {
            return null;
        }

        var mutex = MutexCache.GetOrAdd(FirebirdGateName, static name => new Mutex(false, name));
        mutex.WaitOne();
        return new MutexReleaser(mutex);
    }

    private sealed class MutexReleaser(Mutex mutex) : IDisposable
    {
        private bool released;

        public void Dispose()
        {
            if (released)
                return;

            released = true;
            mutex.ReleaseMutex();
        }
    }
}
