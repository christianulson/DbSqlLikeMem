using System.Data;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the DbSqlLikeMem benchmark session implementation shared across providers.
/// PT: Fornece a implementacao de sessao de benchmark do DbSqlLikeMem compartilhada entre providers.
/// </summary>
public abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <inheritdoc />
    protected override void ExecuteSequentialUserInsertBatch(DbConnection connection, string tableName, int rowCount, DbTransaction? transaction = null)
    {
        if (rowCount <= 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"INSERT INTO {tableName} (Id, Name) VALUES (@id, @name)";
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        var idParameter = command.CreateParameter();
        idParameter.ParameterName = "@id";
        idParameter.Value = 0;
        command.Parameters.Add(idParameter);

        var nameParameter = command.CreateParameter();
        nameParameter.ParameterName = "@name";
        nameParameter.Value = string.Empty;
        command.Parameters.Add(nameParameter);

        if (rowCount > 1)
        {
            command.Prepare();
        }

        for (var i = 1; i <= rowCount; i++)
        {
            idParameter.Value = i;
            nameParameter.Value = $"User-{i}";
            command.ExecuteNonQuery();
        }
    }

    /// <inheritdoc />
    protected override void RunBatchMixedReadWrite()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users, uId));
            using var transaction = connection.BeginTransaction();

            using var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"INSERT INTO {users} (Id, Name) VALUES (@id, @name)";

            var insertId = insertCommand.CreateParameter();
            insertId.ParameterName = "@id";
            insertId.Value = 0;
            insertCommand.Parameters.Add(insertId);

            var insertName = insertCommand.CreateParameter();
            insertName.ParameterName = "@name";
            insertName.Value = string.Empty;
            insertCommand.Parameters.Add(insertName);

            insertCommand.Prepare();

            insertId.Value = 1;
            insertName.Value = "Alice";
            insertCommand.ExecuteNonQuery();

            insertId.Value = 2;
            insertName.Value = "Bob";
            insertCommand.ExecuteNonQuery();

            using var selectCommand = connection.CreateCommand();
            selectCommand.Transaction = transaction;
            selectCommand.CommandText = $"SELECT Name FROM {users} WHERE Id = @id";

            var selectId = selectCommand.CreateParameter();
            selectId.ParameterName = "@id";
            selectId.Value = 0;
            selectCommand.Parameters.Add(selectId);

            selectCommand.Prepare();
            selectId.Value = 1;
            var value = Convert.ToString(selectCommand.ExecuteScalar(), CultureInfo.InvariantCulture);

            using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = $"UPDATE {users} SET Name = @name WHERE Id = @id";

            var updateId = updateCommand.CreateParameter();
            updateId.ParameterName = "@id";
            updateId.Value = 0;
            updateCommand.Parameters.Add(updateId);

            var updateName = updateCommand.CreateParameter();
            updateName.ParameterName = "@name";
            updateName.Value = string.Empty;
            updateCommand.Parameters.Add(updateName);

            updateCommand.Prepare();
            updateId.Value = 2;
            updateName.Value = "Bob-v2";
            updateCommand.ExecuteNonQuery();

            transaction.Commit();
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected mixed-batch read result for {Dialect.DisplayName}: {value ?? "<null>"}.");
            }

            GC.KeepAlive(value);
        }
        finally
        {
            SafeDropTable(connection, users, uId);
        }
    }

    /// <inheritdoc />
    protected override void RunBatchScalar()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users, uId));
            using var transaction = connection.BeginTransaction();

            using var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"INSERT INTO {users} (Id, Name) VALUES (@id, @name)";

            var insertId = insertCommand.CreateParameter();
            insertId.ParameterName = "@id";
            insertId.Value = 0;
            insertCommand.Parameters.Add(insertId);

            var insertName = insertCommand.CreateParameter();
            insertName.ParameterName = "@name";
            insertName.Value = string.Empty;
            insertCommand.Parameters.Add(insertName);

            insertCommand.Prepare();

            insertId.Value = 1;
            insertName.Value = "Alice";
            insertCommand.ExecuteNonQuery();

            insertId.Value = 2;
            insertName.Value = "Bob";
            insertCommand.ExecuteNonQuery();

            transaction.Commit();

            using var countCommand = connection.CreateCommand();
            countCommand.CommandText = Dialect.CountRows(users);
            countCommand.Prepare();
            var count = Convert.ToInt32(countCommand.ExecuteScalar(), CultureInfo.InvariantCulture);

            using var selectCommand = connection.CreateCommand();
            selectCommand.CommandText = $"SELECT Name FROM {users} WHERE Id = @id";
            var selectId = selectCommand.CreateParameter();
            selectId.ParameterName = "@id";
            selectId.Value = 2;
            selectCommand.Parameters.Add(selectId);
            selectCommand.Prepare();
            var second = Convert.ToString(selectCommand.ExecuteScalar(), CultureInfo.InvariantCulture);

            if (count != 2 || !string.Equals(second, "Bob", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected scalar batch result for {Dialect.DisplayName}: count={count}, second={second ?? "<null>"}.");
            }

            GC.KeepAlive(second);
        }
        finally
        {
            SafeDropTable(connection, users, uId);
        }
    }

    /// <inheritdoc />
    protected override void RunBatchNonQuery()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();

        try
        {
            ExecuteNonQuery(connection, Dialect.CreateUsersTable(users, uId));
            using var transaction = connection.BeginTransaction();

            using var insertCommand = connection.CreateCommand();
            insertCommand.Transaction = transaction;
            insertCommand.CommandText = $"INSERT INTO {users} (Id, Name) VALUES (@id, @name)";

            var insertId = insertCommand.CreateParameter();
            insertId.ParameterName = "@id";
            insertId.Value = 0;
            insertCommand.Parameters.Add(insertId);

            var insertName = insertCommand.CreateParameter();
            insertName.ParameterName = "@name";
            insertName.Value = string.Empty;
            insertCommand.Parameters.Add(insertName);

            insertCommand.Prepare();

            insertId.Value = 1;
            insertName.Value = "Alice";
            insertCommand.ExecuteNonQuery();

            insertId.Value = 2;
            insertName.Value = "Bob";
            insertCommand.ExecuteNonQuery();

            using var updateCommand = connection.CreateCommand();
            updateCommand.Transaction = transaction;
            updateCommand.CommandText = $"UPDATE {users} SET Name = @name WHERE Id = @id";

            var updateId = updateCommand.CreateParameter();
            updateId.ParameterName = "@id";
            updateId.Value = 2;
            updateCommand.Parameters.Add(updateId);

            var updateName = updateCommand.CreateParameter();
            updateName.ParameterName = "@name";
            updateName.Value = "Bob-v2";
            updateCommand.Parameters.Add(updateName);

            updateCommand.Prepare();
            updateCommand.ExecuteNonQuery();

            using var deleteCommand = connection.CreateCommand();
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"DELETE FROM {users} WHERE Id = @id";

            var deleteId = deleteCommand.CreateParameter();
            deleteId.ParameterName = "@id";
            deleteId.Value = 1;
            deleteCommand.Parameters.Add(deleteId);

            deleteCommand.Prepare();
            deleteCommand.ExecuteNonQuery();

            transaction.Commit();

            using var countCommand = connection.CreateCommand();
            countCommand.CommandText = Dialect.CountRows(users);
            countCommand.Prepare();
            var count = Convert.ToInt32(countCommand.ExecuteScalar(), CultureInfo.InvariantCulture);
            if (count != 1)
            {
                throw new InvalidOperationException($"Unexpected non-query batch count for {Dialect.DisplayName}: {count}.");
            }

            GC.KeepAlive(count);
        }
        finally
        {
            SafeDropTable(connection, users, uId);
        }
    }

    /// <inheritdoc />
    protected override void RunStringAggregateLargeGroup()
        => base.RunStringAggregateLargeGroup();

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
