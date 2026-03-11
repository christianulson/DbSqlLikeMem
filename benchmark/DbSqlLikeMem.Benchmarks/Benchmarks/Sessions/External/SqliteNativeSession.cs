using Microsoft.Data.Sqlite;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs SQLite benchmarks against the native Microsoft.Data.Sqlite in-memory provider.
/// PT-br: Executa benchmarks de SQLite contra o provedor nativo em memória Microsoft.Data.Sqlite.
/// </summary>
public sealed class SqliteNativeSession()
    : BenchmarkSessionBase(new SqliteDialect(), BenchmarkEngine.NativeAdoNet)
{

    /// <summary>
    /// EN: Creates a new in-memory SQLite connection using Microsoft.Data.Sqlite.
    /// PT-br: Cria uma nova conexão SQLite em memória usando Microsoft.Data.Sqlite.
    /// </summary>
    /// <returns>EN: A new in-memory SQLite connection. PT-br: Uma nova conexão SQLite em memória.</returns>
    protected override DbConnection CreateConnection()
    {
        return new SqliteConnection("Data Source=:memory:");
    }
}
