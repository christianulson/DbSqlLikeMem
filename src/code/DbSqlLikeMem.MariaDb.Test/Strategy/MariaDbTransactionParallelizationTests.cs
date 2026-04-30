namespace DbSqlLikeMem.MariaDb.Test.Strategy;

/// <summary>
/// EN: Covers multi-table rollback behavior when MariaDB runs in thread-safe mode.
/// PT: Cobre o comportamento de rollback com multiplas tabelas quando o MariaDB executa em modo thread-safe.
/// </summary>
public sealed class MariaDbTransactionParallelizationTests
{
    /// <summary>
    /// EN: Ensures rollback restores multiple tables correctly when the database runs in thread-safe mode.
    /// PT: Garante que rollback restaure varias tabelas corretamente quando o banco executa em modo thread-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionRollback_ShouldRestoreMultipleTables_WhenThreadSafe()
    {
        var db = new MariaDbDbMock
        {
            ThreadSafe = true
        };

        var users = AddUsersTable(db);
        users.AddPrimaryKeyIndexes("id");

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("user_id", DbType.Int32, false);
        orders.AddPrimaryKeyIndexes("id");

        using var connection = CreateOpenConnection(db);
        using var transaction = Assert.IsType<MySqlTransactionMock>(connection.BeginTransaction());
        using var command = new MySqlCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')"
        };
        using var orderCommand = new MySqlCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO orders (id, user_id) VALUES (10, 1)"
        };

        command.ExecuteNonQuery();
        orderCommand.ExecuteNonQuery();

        transaction.Rollback();

        Assert.Empty(users);
        Assert.Empty(orders);
        Assert.False(connection.HasActiveTransaction);
        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
    }

    private static MariaDbConnectionMock CreateOpenConnection(MariaDbDbMock? db = null)
    {
        var connection = new MariaDbConnectionMock(db ?? new MariaDbDbMock());
        connection.Open();
        return connection;
    }

    private static ITableMock AddUsersTable(MariaDbDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }
}
