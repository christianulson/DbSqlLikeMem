using Microsoft.Data.Sqlite;
using System.Globalization;
using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.Sqlite.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs SQLite benchmarks against the native Microsoft.Data.Sqlite in-memory provider.
/// PT-br: Executa benchmarks de SQLite contra o provedor nativo em memória Microsoft.Data.Sqlite.
/// </summary>
public sealed class SqliteNativeSession()
    : BenchmarkSessionBase(new SqliteProviderSqlDialect(), BenchmarkEngine.NativeAdoNet)
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
    /// EN: Creates a SQLite temporary table from the shared source scenario and validates the projected rows within the same connection.
    /// PT-br: Cria uma tabela temporaria SQLite a partir do cenario compartilhado e valida as linhas projetadas na mesma conexao.
    /// </summary>
    protected override void RunTempTableCreateAndUse()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryTableScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var rows = service.RunCreateTemporaryTableAsSelectThenSelect(users, uId);
            GC.KeepAlive(rows);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
        }
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
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect));
        service.CreateScenario(users);

        try
        {
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
        finally
        {
            service.DropScenario(users);
        }
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
        var service = CreateTemporaryTableService(connection1, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect), CreateConnection);
        service.CreateScenario(users);

        try
        {
            var value = service.RunTemporaryTableCrossConnectionIsolation(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users);
            }
            catch
            {
                SafeDropTemporaryTable(connection1, users);
            }
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
}
