using Microsoft.Data.Sqlite;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs SQLite benchmarks against the native Microsoft.Data.Sqlite in-memory provider.
/// PT-br: Executa benchmarks de SQLite contra o provedor nativo em memória Microsoft.Data.Sqlite.
/// </summary>
public sealed class SqliteNativeSession()
    : BenchmarkSessionBase(new SqliteDialect(), BenchmarkEngine.NativeAdoNet)
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
    public override void Initialize()
    {
        _anchorConnection = new SqliteConnection(_connectionString);
        _anchorConnection.Open();
    }

    /// <summary>
    /// EN: Creates a new in-memory SQLite connection using Microsoft.Data.Sqlite.
    /// PT-br: Cria uma nova conexão SQLite em memória usando Microsoft.Data.Sqlite.
    /// </summary>
    /// <returns>EN: A new in-memory SQLite connection. PT-br: Uma nova conexão SQLite em memória.</returns>
    protected override DbConnection CreateConnection()
    {
        return new SqliteConnection(_connectionString);
    }

    /// <summary>
    /// EN: Creates a SQLite temporary table, inserts one row, and validates the row count within the same connection.
    /// PT-br: Cria uma tabela temporaria SQLite, insere uma linha e valida a contagem na mesma conexao.
    /// </summary>
    protected override void RunTempTableCreateAndUse()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        ExecuteNonQuery(connection, CreateTemporaryUsersTable(users));
        ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"));

        var count = Convert.ToInt32(
            ExecuteScalar(connection, Dialect.CountRows(users)),
            CultureInfo.InvariantCulture);

        if (count != 1)
        {
            throw new InvalidOperationException($"Expected 1 temp-table row for {Dialect.DisplayName}, got {count}.");
        }

        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Verifies a SQLite rollback clears rows written to a temporary table during the transaction.
    /// PT-br: Verifica se um rollback no SQLite limpa as linhas gravadas em uma tabela temporaria durante a transacao.
    /// </summary>
    protected override void RunTempTableRollback()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support temp-table rollback benchmark.");
        }

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        ExecuteNonQuery(connection, CreateTemporaryUsersTable(users));

        using var tx = connection.BeginTransaction();
        ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), tx);
        ExecuteNonQuery(connection, Dialect.Savepoint(NewSavepointName()), tx);
        ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), tx);
        tx.Rollback();

        var count = Convert.ToInt32(
            ExecuteScalar(connection, Dialect.CountRows(users)),
            CultureInfo.InvariantCulture);

        if (count != 0)
        {
            throw new InvalidOperationException($"Expected rollback to clear temp-table rows for {Dialect.DisplayName}, got {count}.");
        }

        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Verifies a SQLite temporary table is isolated to the connection that created it.
    /// PT-br: Verifica se uma tabela temporaria SQLite fica isolada na conexao que a criou.
    /// </summary>
    protected override void RunTempTableCrossConnectionIsolation()
    {
        var users = NewUsersTableName();
        using var connection1 = CreateConnection();
        connection1.Open();
        using var connection2 = CreateConnection();
        connection2.Open();

        ExecuteNonQuery(connection1, CreateTemporaryUsersTable(users));
        ExecuteNonQuery(connection1, Dialect.InsertUser(users, 1, "Alice"));

        try
        {
            var count = Convert.ToInt32(
                ExecuteScalar(connection2, Dialect.CountRows(users)),
                CultureInfo.InvariantCulture);

            if (count != 0)
            {
                throw new InvalidOperationException($"Expected 0 temp-table rows from the peer connection for {Dialect.DisplayName}, got {count}.");
            }

            GC.KeepAlive(count);
        }
        catch (SqliteException ex) when (ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase))
        {
            GC.KeepAlive(ex.Message);
        }
    }

    /// <summary>
    /// EN: Releases the anchor connection that keeps the shared in-memory database alive.
    /// PT-br: Libera a conexao ancora que mantem o banco em memoria compartilhada ativo.
    /// </summary>
    public override void Dispose()
    {
        _anchorConnection?.Dispose();
        _anchorConnection = null;
    }

    private static string CreateTemporaryUsersTable(string tableName) =>
        $"CREATE TEMP TABLE {tableName} (Id INTEGER NOT NULL PRIMARY KEY, Name TEXT NOT NULL)";
}
