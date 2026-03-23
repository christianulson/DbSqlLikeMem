using Microsoft.Data.Sqlite;
using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.Sqlite.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs SQLite benchmarks against the native Microsoft.Data.Sqlite in-memory provider.
/// PT-br: Executa benchmarks de SQLite contra o provedor nativo em memória Microsoft.Data.Sqlite.
/// </summary>
public sealed class SqliteNativeSession()
    : ExternalBenchmarkSessionBase(new SqliteProviderSqlDialect(), BenchmarkEngine.NativeAdoNet)
{
    private readonly string _connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = $"bench-{Guid.NewGuid():N}",
        Mode = SqliteOpenMode.Memory,
        Cache = SqliteCacheMode.Shared,
        Pooling = false,
        DefaultTimeout = 30
    }.ToString();

    private SqliteConnection? _anchorConnection;

    /// <summary>
    /// EN: Opens a shared in-memory SQLite database and keeps an anchor connection alive for the full session.
    /// PT-br: Abre um banco SQLite em memoria compartilhada e mantem uma conexao ancora ativa durante toda a sessao.
    /// </summary>
    protected override string StartExternalRuntime()
    {
        _anchorConnection = new SqliteConnection(_connectionString);
        _anchorConnection.Open();
        return _connectionString;
    }

    /// <summary>
    /// EN: Creates a new in-memory SQLite connection using Microsoft.Data.Sqlite.
    /// PT-br: Cria uma nova conexão SQLite em memória usando Microsoft.Data.Sqlite.
    /// </summary>
    /// <returns>EN: A new in-memory SQLite connection. PT-br: Uma nova conexão SQLite em memória.</returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new SqliteConnection(connectionString);
    }

    /// <summary>
    /// EN: Releases the anchor connection that keeps the shared in-memory database alive.
    /// PT-br: Libera a conexao ancora que mantem o banco em memoria compartilhada ativo.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        _anchorConnection?.Dispose();
        _anchorConnection = null;
    }
}
