using System.Data;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
public abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <summary>
    /// EN: Creates a connection-scoped temporary users table, inserts one row, and validates the row count within the same session.
    /// PT-br: Cria uma tabela temporaria de usuarios no escopo da conexao, insere uma linha e valida a contagem na mesma sessao.
    /// </summary>
    protected override void RunTempTableCreateAndUse()
    {
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        var mockConnection = (DbConnectionMockBase)connection;
        CreateTemporaryUsersTable(mockConnection, users);

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
    /// EN: Verifies a transaction rollback restores the connection-scoped temporary table contents to the pre-transaction state.
    /// PT-br: Verifica se um rollback de transacao restaura o conteudo da tabela temporaria de escopo da conexao ao estado anterior.
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

        var mockConnection = (DbConnectionMockBase)connection;
        CreateTemporaryUsersTable(mockConnection, users);

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
    /// EN: Verifies connection-scoped temporary tables are not visible from a different logical connection.
    /// PT-br: Verifica se tabelas temporarias de escopo da conexao nao ficam visiveis em outra conexao logica.
    /// </summary>
    protected override void RunTempTableCrossConnectionIsolation()
    {
        var users = NewUsersTableName();
        using var connection1 = CreateConnection();
        connection1.Open();
        using var connection2 = CreateConnection();
        connection2.Open();

        var ownerConnection = (DbConnectionMockBase)connection1;
        var peerConnection = (DbConnectionMockBase)connection2;

        CreateTemporaryUsersTable(ownerConnection, users);
        ExecuteNonQuery(connection1, Dialect.InsertUser(users, 1, "Alice"));

        if (peerConnection.TryGetTemporaryTable(users, out _))
        {
            var count = Convert.ToInt32(
                ExecuteScalar(connection2, Dialect.CountRows(users)),
                CultureInfo.InvariantCulture);

            if (count != 0)
            {
                throw new InvalidOperationException($"Expected 0 temp-table rows from the peer connection for {Dialect.DisplayName}, got {count}.");
            }

            GC.KeepAlive(count);
            return;
        }

        GC.KeepAlive(users);
    }

    private static void CreateTemporaryUsersTable(DbConnectionMockBase connection, string tableName)
    {
        var table = connection.AddTemporaryTable(tableName);
        table.AddColumn("Id", DbType.Int32, nullable: false);
        table.AddColumn("Name", DbType.String, nullable: false);
    }
}
